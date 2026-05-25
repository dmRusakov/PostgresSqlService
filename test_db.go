package postgressql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// MemClient implements [Client] entirely in memory — no real PostgreSQL connection required.
//
// All DDL (CREATE, DROP, ALTER…) is silently accepted.
// INSERT / UPDATE / DELETE mutate an in-memory row store.
// SELECT queries return rows from that store with basic equality filtering.
//
// Typical test setup:
//
//	db := postgressql.NewMemClient()
//	db.Seed("myschema", "products",
//	    map[string]any{"id": uuid.New(), "sku": "X-1", "name": "Widget", "price": 9.99, "is_active": true},
//	)
//	svc := postgressql.NewService(postgressql.Parameters{
//	    DB: db, Schema: "myschema", Table: "products", Item: Product{},
//	})
//
// Note: InitData CSV loading still performs a real file open. Pass InitData:"" in tests to skip it.
type MemClient struct {
	mu     sync.RWMutex
	tables map[string]*memTableStore
	Calls  []MemCall // every SQL call recorded for assertions
}

// MemCall records one SQL invocation for use in test assertions.
type MemCall struct {
	Method string // "Exec", "Query", "QueryRow"
	SQL    string
	Args   []any
}

type memTableStore struct {
	rows []map[string]any
}

// NewMemClient returns an empty MemClient ready to use as a [Client].
func NewMemClient() *MemClient {
	return &MemClient{tables: make(map[string]*memTableStore)}
}

// Seed pre-loads rows into schema.table.
// Column names must match the db tag values used in the corresponding entity struct.
func (c *MemClient) Seed(schema, table string, rows ...map[string]any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := c.ensureTable(memKey(schema, table))
	t.rows = append(t.rows, rows...)
}

// TableRows returns a snapshot of all stored rows for schema.table.
// Use it in tests to verify what was inserted or still present after deletes.
func (c *MemClient) TableRows(schema, table string) []map[string]any {
	c.mu.RLock()
	defer c.mu.RUnlock()
	t, ok := c.tables[memKey(schema, table)]
	if !ok {
		return nil
	}
	out := make([]map[string]any, len(t.rows))
	copy(out, t.rows)
	return out
}

// Reset clears all stored rows and recorded calls (useful between sub-tests).
func (c *MemClient) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tables = make(map[string]*memTableStore)
	c.Calls = nil
}

func (c *MemClient) ensureTable(key string) *memTableStore {
	if t, ok := c.tables[key]; ok {
		return t
	}
	t := &memTableStore{}
	c.tables[key] = t
	return t
}

func (c *MemClient) record(method, rawSQL string, args []any) {
	c.mu.Lock()
	c.Calls = append(c.Calls, MemCall{Method: method, SQL: rawSQL, Args: args})
	c.mu.Unlock()
}

// ---- Client interface -------------------------------------------------------

func (c *MemClient) Close() {}

func (c *MemClient) Acquire(_ context.Context) (*pgxpool.Conn, error) {
	return nil, fmt.Errorf("MemClient: Acquire not supported in tests")
}

func (c *MemClient) AcquireFunc(_ context.Context, _ func(*pgxpool.Conn) error) error {
	return fmt.Errorf("MemClient: AcquireFunc not supported in tests")
}

func (c *MemClient) AcquireAllIdle(_ context.Context) []*pgxpool.Conn { return nil }
func (c *MemClient) Stat() *pgxpool.Stat                              { return nil }

func (c *MemClient) Exec(_ context.Context, rawSQL string, args ...any) (pgconn.CommandTag, error) {
	c.record("Exec", rawSQL, args)
	upper := strings.ToUpper(strings.TrimSpace(rawSQL))
	switch {
	case memIsDDL(upper):
		return pgconn.NewCommandTag("OK"), nil
	case strings.HasPrefix(upper, "INSERT"):
		n, err := c.doInsert(rawSQL, args)
		return pgconn.NewCommandTag(fmt.Sprintf("INSERT 0 %d", n)), err
	case strings.HasPrefix(upper, "UPDATE"):
		n, err := c.doUpdate(rawSQL, args)
		return pgconn.NewCommandTag(fmt.Sprintf("UPDATE %d", n)), err
	case strings.HasPrefix(upper, "DELETE"):
		n, err := c.doDelete(rawSQL, args)
		return pgconn.NewCommandTag(fmt.Sprintf("DELETE %d", n)), err
	}
	return pgconn.NewCommandTag("OK"), nil
}

func (c *MemClient) Query(_ context.Context, rawSQL string, args ...any) (pgx.Rows, error) {
	c.record("Query", rawSQL, args)
	cols, rows, err := c.doSelect(rawSQL, args)
	if err != nil {
		return &memRows{err: err}, nil
	}
	return &memRows{columns: cols, rows: rows}, nil
}

func (c *MemClient) QueryRow(_ context.Context, rawSQL string, args ...any) pgx.Row {
	c.record("QueryRow", rawSQL, args)
	upper := strings.ToUpper(strings.TrimSpace(rawSQL))
	if strings.HasPrefix(upper, "INSERT") {
		id, err := c.doInsertReturning(rawSQL, args)
		if err != nil {
			return &memSingleRow{err: err}
		}
		return &memSingleRow{values: []any{id}}
	}
	cols, rows, err := c.doSelect(rawSQL, args)
	if err != nil {
		return &memSingleRow{err: err}
	}
	if len(rows) == 0 {
		return &memSingleRow{err: pgx.ErrNoRows}
	}
	return &memSingleRow{values: memRowToValues(cols, rows[0])}
}

func (c *MemClient) Begin(_ context.Context) (pgx.Tx, error) {
	return &memTx{client: c}, nil
}

func (c *MemClient) BeginTx(_ context.Context, _ pgx.TxOptions) (pgx.Tx, error) {
	return &memTx{client: c}, nil
}

// ---- DML --------------------------------------------------------------------

func (c *MemClient) doInsert(rawSQL string, args []any) (int, error) {
	schema, table, err := memParseTable(rawSQL)
	if err != nil {
		return 0, err
	}
	cols, err := memParseInsertCols(rawSQL)
	if err != nil {
		return 0, err
	}
	row := memBuildRow(cols, args)
	c.mu.Lock()
	defer c.mu.Unlock()
	t := c.ensureTable(memKey(schema, table))
	t.rows = append(t.rows, row)
	return 1, nil
}

// doInsertReturning stores the row and returns the value of the id column.
func (c *MemClient) doInsertReturning(rawSQL string, args []any) (any, error) {
	schema, table, err := memParseTable(rawSQL)
	if err != nil {
		return nil, err
	}
	cols, err := memParseInsertCols(rawSQL)
	if err != nil {
		return nil, err
	}
	row := memBuildRow(cols, args)
	c.mu.Lock()
	defer c.mu.Unlock()
	key := memKey(schema, table)
	t := c.ensureTable(key)
	t.rows = append(t.rows, row)
	return row["id"], nil
}

func (c *MemClient) doUpdate(rawSQL string, args []any) (int, error) {
	schema, table, err := memParseTable(rawSQL)
	if err != nil {
		return 0, err
	}
	setCols, setIdxs, err := memParseSet(rawSQL)
	if err != nil {
		return 0, err
	}
	whereConds := memParseWhere(rawSQL)

	c.mu.Lock()
	defer c.mu.Unlock()
	t, ok := c.tables[memKey(schema, table)]
	if !ok {
		return 0, nil
	}
	count := 0
	for _, row := range t.rows {
		if memMatch(row, whereConds, args) {
			for i, col := range setCols {
				if setIdxs[i] <= len(args) {
					row[col] = args[setIdxs[i]-1]
				}
			}
			count++
		}
	}
	return count, nil
}

func (c *MemClient) doDelete(rawSQL string, args []any) (int, error) {
	schema, table, err := memParseTable(rawSQL)
	if err != nil {
		return 0, err
	}
	whereConds := memParseWhere(rawSQL)

	c.mu.Lock()
	defer c.mu.Unlock()
	key := memKey(schema, table)
	t, ok := c.tables[key]
	if !ok {
		return 0, nil
	}
	kept := t.rows[:0]
	count := 0
	for _, row := range t.rows {
		if memMatch(row, whereConds, args) {
			count++
		} else {
			kept = append(kept, row)
		}
	}
	t.rows = kept
	return count, nil
}

func (c *MemClient) doSelect(rawSQL string, args []any) ([]string, []map[string]any, error) {
	upper := strings.ToUpper(rawSQL)

	// TableIndex query — return a fixed hash string
	if strings.Contains(upper, "PG_STAT_USER_TABLES") {
		return []string{"concat"}, []map[string]any{{"concat": "00000000"}}, nil
	}

	// IsTableExists / IsSchemaExists — report "not found" so DDL path runs
	if strings.Contains(upper, "INFORMATION_SCHEMA.TABLES") ||
		strings.Contains(upper, "INFORMATION_SCHEMA.SCHEMATA") {
		return []string{"exists"}, nil, nil
	}

	// SELECT 1 (Exists check)
	if strings.HasPrefix(strings.TrimSpace(upper), "SELECT 1 ") {
		schema, table, err := memParseTable(rawSQL)
		if err != nil {
			return nil, nil, err
		}
		whereConds := memParseWhere(rawSQL)
		c.mu.RLock()
		defer c.mu.RUnlock()
		t := c.tables[memKey(schema, table)]
		if t != nil {
			for _, row := range t.rows {
				if memMatch(row, whereConds, args) {
					return []string{"exists"}, []map[string]any{{"exists": 1}}, nil
				}
			}
		}
		return []string{"exists"}, nil, nil
	}

	schema, table, err := memParseTable(rawSQL)
	if err != nil {
		return nil, nil, err
	}
	cols := memParseSelectCols(rawSQL)
	whereConds := memParseWhere(rawSQL)

	c.mu.RLock()
	defer c.mu.RUnlock()
	t := c.tables[memKey(schema, table)]
	if t == nil {
		return cols, nil, nil
	}
	var out []map[string]any
	for _, row := range t.rows {
		if memMatch(row, whereConds, args) {
			out = append(out, row)
		}
	}
	return cols, out, nil
}

// ---- SQL parsing helpers ----------------------------------------------------

var memReTable = regexp.MustCompile(`(?i)(?:INSERT\s+INTO|DELETE\s+FROM|UPDATE|FROM)\s+"?(\w+)"?\."?(\w+)"?`)

func memParseTable(rawSQL string) (schema, table string, err error) {
	m := memReTable.FindStringSubmatch(rawSQL)
	if len(m) < 3 {
		return "", "", fmt.Errorf("MemClient: cannot parse table from: %.80s", rawSQL)
	}
	return m[1], m[2], nil
}

var memReInsertCols = regexp.MustCompile(`(?i)INSERT\s+INTO\s+[^(]+\(([^)]+)\)\s+VALUES`)

func memParseInsertCols(rawSQL string) ([]string, error) {
	m := memReInsertCols.FindStringSubmatch(rawSQL)
	if len(m) < 2 {
		return nil, fmt.Errorf("MemClient: cannot parse INSERT columns from: %.80s", rawSQL)
	}
	parts := strings.Split(m[1], ",")
	cols := make([]string, len(parts))
	for i, p := range parts {
		cols[i] = strings.Trim(strings.TrimSpace(p), `"`)
	}
	return cols, nil
}

var memReSelectCols = regexp.MustCompile(`(?i)SELECT\s+(.+?)\s+FROM`)

func memParseSelectCols(rawSQL string) []string {
	m := memReSelectCols.FindStringSubmatch(rawSQL)
	if len(m) < 2 {
		return nil
	}
	parts := strings.Split(m[1], ",")
	cols := make([]string, len(parts))
	for i, p := range parts {
		cols[i] = strings.Trim(strings.TrimSpace(p), `"`)
	}
	return cols
}

var memReSet = regexp.MustCompile(`(?i)SET\s+(.+?)\s+WHERE`)

func memParseSet(rawSQL string) (cols []string, idxs []int, err error) {
	m := memReSet.FindStringSubmatch(rawSQL)
	if len(m) < 2 {
		return nil, nil, fmt.Errorf("MemClient: cannot parse SET from: %.80s", rawSQL)
	}
	for _, match := range memReAssign.FindAllStringSubmatch(m[1], -1) {
		cols = append(cols, strings.Trim(match[1], `"`))
		idx, _ := strconv.Atoi(match[2])
		idxs = append(idxs, idx)
	}
	return
}

// memCond represents one parsed WHERE condition.
type memCond struct {
	col  string
	op   string // "eq", "in", "like"
	idxs []int  // 1-based arg indexes
	or   bool   // part of an OR group (Search path)
}

var (
	memReWhere       = regexp.MustCompile(`(?i)WHERE\s+(.+?)(?:\s+ORDER\s+BY|\s+LIMIT|\s+GROUP\s+BY|$)`)
	memReAssign      = regexp.MustCompile(`"?(\w+)"?\s*=\s*\$(\d+)`)
	memReIn          = regexp.MustCompile(`(?i)"?(\w+)"?\s+IN\s+\(([^)]+)\)`)
	memReLike        = regexp.MustCompile(`(?i)"?(\w+)"?\s+LIKE\s+\$(\d+)`)
	memRePlaceholder = regexp.MustCompile(`\$(\d+)`)
)

// memParseWhere extracts WHERE conditions from a SQL string.
// Handles: col = $n  (equality), col IN ($n,...) (slice filter), col LIKE $n (search).
func memParseWhere(rawSQL string) []memCond {
	m := memReWhere.FindStringSubmatch(rawSQL)
	if len(m) < 2 {
		return nil
	}
	clause := m[1]

	// OR-LIKE group from Search: (col1 LIKE $1 OR col2 LIKE $1)
	if strings.HasPrefix(strings.TrimSpace(clause), "(") {
		inner := strings.TrimSpace(clause)
		inner = strings.TrimPrefix(inner, "(")
		inner = strings.TrimSuffix(inner, ")")
		var conds []memCond
		for _, match := range memReLike.FindAllStringSubmatch(inner, -1) {
			idx, _ := strconv.Atoi(match[2])
			conds = append(conds, memCond{
				col: strings.Trim(match[1], `"`), op: "like", idxs: []int{idx}, or: true,
			})
		}
		if len(conds) > 0 {
			return conds
		}
	}

	var conds []memCond

	// col IN ($n, $m, ...)
	for _, match := range memReIn.FindAllStringSubmatch(clause, -1) {
		col := strings.Trim(match[1], `"`)
		var idxs []int
		for _, ph := range memRePlaceholder.FindAllStringSubmatch(match[2], -1) {
			idx, _ := strconv.Atoi(ph[1])
			idxs = append(idxs, idx)
		}
		conds = append(conds, memCond{col: col, op: "in", idxs: idxs})
	}

	// col = $n  (only if no IN for this col already found)
	inCols := map[string]bool{}
	for _, c := range conds {
		inCols[c.col] = true
	}
	for _, match := range memReAssign.FindAllStringSubmatch(clause, -1) {
		col := strings.Trim(match[1], `"`)
		if inCols[col] {
			continue
		}
		idx, _ := strconv.Atoi(match[2])
		conds = append(conds, memCond{col: col, op: "eq", idxs: []int{idx}})
	}

	return conds
}

// memMatch evaluates WHERE conditions against a row.
// OR conditions (LIKE / Search) pass if any one matches.
// AND conditions (eq / in) all must match.
func memMatch(row map[string]any, conds []memCond, args []any) bool {
	if len(conds) == 0 {
		return true
	}

	// OR group (Search LIKE)
	if conds[0].or {
		for _, c := range conds {
			if c.idxs[0] > len(args) {
				continue
			}
			pattern := strings.Trim(fmt.Sprintf("%v", args[c.idxs[0]-1]), "%")
			val := fmt.Sprintf("%v", row[c.col])
			if strings.Contains(strings.ToLower(val), strings.ToLower(pattern)) {
				return true
			}
		}
		return false
	}

	// AND group
	for _, c := range conds {
		got := fmt.Sprintf("%v", row[c.col])
		switch c.op {
		case "eq":
			if c.idxs[0] > len(args) {
				continue
			}
			if got != fmt.Sprintf("%v", args[c.idxs[0]-1]) {
				return false
			}
		case "in":
			matched := false
			for _, idx := range c.idxs {
				if idx <= len(args) && got == fmt.Sprintf("%v", args[idx-1]) {
					matched = true
					break
				}
			}
			if !matched {
				return false
			}
		}
	}
	return true
}

func memBuildRow(cols []string, args []any) map[string]any {
	row := make(map[string]any, len(cols))
	for i, col := range cols {
		if i < len(args) {
			row[col] = args[i]
		}
	}
	if _, ok := row["id"]; !ok {
		row["id"] = uuid.New()
	}
	return row
}

func memRowToValues(cols []string, row map[string]any) []any {
	vals := make([]any, len(cols))
	for i, col := range cols {
		vals[i] = row[col]
	}
	return vals
}

func memIsDDL(upper string) bool {
	return strings.HasPrefix(upper, "CREATE") ||
		strings.HasPrefix(upper, "DROP") ||
		strings.HasPrefix(upper, "ALTER") ||
		strings.HasPrefix(upper, "TRUNCATE") ||
		strings.HasPrefix(upper, "COMMENT")
}

func memKey(schema, table string) string {
	return strings.Trim(schema, `"`) + "." + strings.Trim(table, `"`)
}

// ---- memRows (pgx.Rows) -----------------------------------------------------

type memRows struct {
	columns []string
	rows    []map[string]any
	idx     int
	err     error
}

func (r *memRows) Close()                                       {}
func (r *memRows) Err() error                                   { return r.err }
func (r *memRows) CommandTag() pgconn.CommandTag                { return pgconn.NewCommandTag("SELECT") }
func (r *memRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *memRows) RawValues() [][]byte                          { return nil }
func (r *memRows) Conn() *pgx.Conn                              { return nil }

func (r *memRows) Next() bool {
	if r.err != nil {
		return false
	}
	if r.idx >= len(r.rows) {
		return false
	}
	r.idx++
	return true
}

func (r *memRows) Scan(dest ...any) error {
	if r.idx == 0 || r.idx > len(r.rows) {
		return fmt.Errorf("memRows: Scan called without a preceding Next")
	}
	return memScanValues(dest, memRowToValues(r.columns, r.rows[r.idx-1]))
}

func (r *memRows) Values() ([]any, error) {
	if r.idx == 0 || r.idx > len(r.rows) {
		return nil, fmt.Errorf("memRows: Values called without a preceding Next")
	}
	return memRowToValues(r.columns, r.rows[r.idx-1]), nil
}

// ---- memSingleRow (pgx.Row) -------------------------------------------------

type memSingleRow struct {
	values []any
	err    error
}

func (r *memSingleRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	return memScanValues(dest, r.values)
}

// ---- value scanning ---------------------------------------------------------

// memScanValues copies values into the dest slice. dest elements are the same
// *sql.Null* types that model.go's scanRow passes to row.Scan.
func memScanValues(dest []any, values []any) error {
	for i, d := range dest {
		var v any
		if i < len(values) {
			v = values[i]
		}
		if err := memAssign(d, v); err != nil {
			return fmt.Errorf("memScanValues[%d]: %w", i, err)
		}
	}
	return nil
}

func memAssign(dest, src any) error {
	// treat typed nil pointers (e.g. (*uuid.UUID)(nil)) as NULL
	if src != nil {
		rv := reflect.ValueOf(src)
		if rv.Kind() == reflect.Ptr && rv.IsNil() {
			src = nil
		}
	}
	if src == nil {
		return nil // leave dest as zero / NULL
	}
	// dereference any non-nil pointer so all branches below work on concrete values
	if rv := reflect.ValueOf(src); rv.Kind() == reflect.Ptr {
		src = rv.Elem().Interface()
	}
	switch d := dest.(type) {
	case *sql.NullString:
		d.Valid = true
		switch v := src.(type) {
		case string:
			d.String = v
		case uuid.UUID:
			d.String = v.String()
		case *uuid.UUID:
			if v != nil {
				d.String = v.String()
			} else {
				d.Valid = false
			}
		default:
			d.String = fmt.Sprintf("%v", src)
		}
	case *sql.NullBool:
		d.Valid = true
		switch v := src.(type) {
		case bool:
			d.Bool = v
		default:
			d.Bool = fmt.Sprintf("%v", src) == "true"
		}
	case *sql.NullInt16:
		d.Valid = true
		d.Int16 = int16(memToInt64(src))
	case *sql.NullInt32:
		d.Valid = true
		d.Int32 = int32(memToInt64(src))
	case *sql.NullInt64:
		d.Valid = true
		d.Int64 = memToInt64(src)
	case *sql.NullFloat64:
		d.Valid = true
		d.Float64 = memToFloat64(src)
	case *sql.NullTime:
		d.Valid = true
		if v, ok := src.(time.Time); ok {
			d.Time = v
		}
	case *[]byte:
		switch v := src.(type) {
		case []byte:
			*d = v
		case json.RawMessage:
			*d = []byte(v)
		case string:
			*d = []byte(v)
		}
	// plain pointer types (used in executeTableIndex and a few direct scans)
	case *string:
		*d = fmt.Sprintf("%v", src)
	case **string:
		s := fmt.Sprintf("%v", src)
		*d = &s
	case *bool:
		if v, ok := src.(bool); ok {
			*d = v
		}
	case *int:
		*d = int(memToInt64(src))
	case *int64:
		*d = memToInt64(src)
	case *float64:
		*d = memToFloat64(src)
	case *time.Time:
		if v, ok := src.(time.Time); ok {
			*d = v
		}
	case *uuid.UUID:
		switch v := src.(type) {
		case uuid.UUID:
			*d = v
		case string:
			if u, err := uuid.Parse(v); err == nil {
				*d = u
			}
		}
		// default: silently ignore unknown dest types
	}
	return nil
}

func memToInt64(v any) int64 {
	switch n := v.(type) {
	case int:
		return int64(n)
	case int8:
		return int64(n)
	case int16:
		return int64(n)
	case int32:
		return int64(n)
	case int64:
		return n
	case uint:
		return int64(n)
	case uint8:
		return int64(n)
	case uint16:
		return int64(n)
	case uint32:
		return int64(n)
	case uint64:
		return int64(n)
	case float32:
		return int64(n)
	case float64:
		return int64(n)
	}
	i, _ := strconv.ParseInt(fmt.Sprintf("%v", v), 10, 64)
	return i
}

func memToFloat64(v any) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	case int:
		return float64(n)
	case int64:
		return float64(n)
	}
	f, _ := strconv.ParseFloat(fmt.Sprintf("%v", v), 64)
	return f
}

// ---- memTx (pgx.Tx) ---------------------------------------------------------

type memTx struct {
	client *MemClient
}

func (t *memTx) Begin(_ context.Context) (pgx.Tx, error) { return &memTx{client: t.client}, nil }
func (t *memTx) Commit(_ context.Context) error          { return nil }
func (t *memTx) Rollback(_ context.Context) error        { return nil }
func (t *memTx) Conn() *pgx.Conn                         { return nil }
func (t *memTx) LargeObjects() pgx.LargeObjects          { return pgx.LargeObjects{} }

func (t *memTx) Exec(ctx context.Context, rawSQL string, args ...any) (pgconn.CommandTag, error) {
	return t.client.Exec(ctx, rawSQL, args...)
}

func (t *memTx) Query(ctx context.Context, rawSQL string, args ...any) (pgx.Rows, error) {
	return t.client.Query(ctx, rawSQL, args...)
}

func (t *memTx) QueryRow(ctx context.Context, rawSQL string, args ...any) pgx.Row {
	return t.client.QueryRow(ctx, rawSQL, args...)
}

func (t *memTx) CopyFrom(_ context.Context, _ pgx.Identifier, _ []string, _ pgx.CopyFromSource) (int64, error) {
	return 0, fmt.Errorf("MemClient: CopyFrom not supported in tests")
}

func (t *memTx) SendBatch(_ context.Context, _ *pgx.Batch) pgx.BatchResults {
	return &memBatchResults{}
}

func (t *memTx) Prepare(_ context.Context, _, _ string) (*pgconn.StatementDescription, error) {
	return nil, nil
}

// ---- memBatchResults (pgx.BatchResults) -------------------------------------

type memBatchResults struct{}

func (b *memBatchResults) Exec() (pgconn.CommandTag, error) {
	return pgconn.NewCommandTag("OK"), nil
}
func (b *memBatchResults) Query() (pgx.Rows, error) { return &memRows{}, nil }
func (b *memBatchResults) QueryRow() pgx.Row        { return &memSingleRow{err: pgx.ErrNoRows} }
func (b *memBatchResults) Close() error             { return nil }

package postgressql

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"reflect"

	"github.com/dmRusakov/PostgresSqlService/tracing"
	"github.com/dmRusakov/PostgresSqlService/utils/crypt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	sq "github.com/Masterminds/squirrel"
)

type ModelInterface interface {
	Get(context.Context, string, any) (any, error)
	Exists(context.Context, string, any) bool
	List(context.Context, map[string][]any, Order) ([]any, error)
	FieldValues(context.Context, string, map[string][]any, Order) ([]any, error)
	Search(context.Context, string, Order) ([]any, error)
	Create(context.Context, any) (uuid.UUID, error)
	Update(context.Context, any, string, any) error
	Patch(context.Context, string, any, string, any) error
	Delete(context.Context, string, any) error
	TableIndex(context.Context) (*string, error)
}

// Model is a struct that contains the SQL statement builder and the PostgreSQL client.
type Model struct {
	Schema       string            // Schema is the name of the schema in the database.
	Table        string            // Table is the name of the table in the database.
	Client       Client            // Client is the PostgreSQL client used to execute queries.
	DbFieldCash  map[string]string // DbFieldCash is a cache for field names to database column names (e.g., "ID" to "id").
	Item         any               // Item is the type of the item that the model will work with, used for reflection.
	SearchFields []string          // SearchFields fields to search data in the table
}

func NewStorage(
	client Client,
	schema string,
	table string,
	item any,
	searchFields []string,
) (*Model, error) {

	return &Model{
		Schema:       schema,
		Table:        table,
		Client:       client,
		DbFieldCash:  map[string]string{},
		Item:         item,
		SearchFields: searchFields,
	}, nil
}

// List - List items with optional filters and ordering.
func (m *Model) List(
	ctx context.Context,
	filters map[string][]any,
	order Order,
) ([]any, error) {
	statement := m.selectStatement()

	// Apply filters to the statement
	for field, values := range filters {
		if len(values) == 0 {
			continue
		}

		if field[0] == '!' {
			column := m.getColumn(field[1:])
			statement = statement.Where(sq.NotEq{column: values})
		} else {
			column := m.getColumn(field)
			statement = statement.Where(sq.Eq{column: values})
		}
	}

	// order and paging
	statement = m.orderAndPaging(statement, order)

	// Execute the query
	return m.executeList(ctx, statement)
}

// FieldValues let list of values by field name
func (m *Model) FieldValues(
	ctx context.Context,
	field string,
	filters map[string][]any,
	order Order,
) ([]any, error) {
	// make select statement by field
	statement := m.selectStatementByFields([]string{field})

	// Apply filters to the statement
	for f, values := range filters {
		if len(values) == 0 {
			continue
		}
		column := m.getColumn(f)
		statement = statement.Where(sq.Eq{column: values})
	}

	// order and paging
	statement = m.orderAndPaging(statement, order)

	// get field type
	fieldType := reflect.TypeOf(m.Item)
	structField, ok := fieldType.FieldByName(field)
	if !ok {
		return nil, fmt.Errorf("field %s not found in struct", field)
	}
	fieldType = structField.Type
	if fieldType.Kind() == reflect.Ptr {
		fieldType = fieldType.Elem()
	}

	// Execute the query
	return m.executeFieldValues(ctx, statement, fieldType)
}

// Search - Search items by value
func (m *Model) Search(
	ctx context.Context,
	value string,
	order Order,
) ([]any, error) {
	if len(m.SearchFields) == 0 {
		return nil, fmt.Errorf("no search fields defined for model %s.%s", m.Schema, m.Table)
	}

	// make select statement
	statement := m.selectStatement()

	// Apply search conditions for each search field using sq.Or
	orConditions := sq.Or{}
	for _, field := range m.SearchFields {
		column := m.getColumn(field)
		orConditions = append(orConditions, sq.Like{column: "%" + value + "%"})
	}
	statement = statement.Where(orConditions)

	// order and paging
	statement = m.orderAndPaging(statement, order)

	// Execute the query
	return m.executeList(ctx, statement)
}

// Get - get data by field name and value of the field.
func (m *Model) Get(
	ctx context.Context,
	searchFieldName string,
	searchFieldValue any,
) (any, error) {
	field := m.getColumn(searchFieldName)
	if field == "" {
		return nil, fmt.Errorf("field %s not found in struct", searchFieldName)
	}

	return m.executeGet(ctx, m.selectStatement().Where(sq.Eq{m.getColumn(searchFieldName): searchFieldValue}))
}

// Exist - check if a record exists in the database by field name and value of the field.
func (m *Model) Exists(
	ctx context.Context,
	searchFieldName string,
	searchFieldValue any,
) bool {
	statement := sq.Select("1").From(fmt.Sprintf("%s.%s", m.Schema, m.Table)).
		Where(sq.Eq{m.getColumn(searchFieldName): searchFieldValue}).
		PlaceholderFormat(sq.Dollar).Limit(1)

	query, args, err := statement.ToSql()
	if err != nil {
		return false
	}

	// Logging for tracing
	tracing.SpanEvent(ctx, "Check Existence")
	tracing.TraceVal(ctx, "SQL", query)
	for i, arg := range args {
		tracing.TraceIVal(ctx, "arg-"+fmt.Sprint(i), arg)
	}

	// Execute the query
	row := m.Client.QueryRow(ctx, query, args...)
	var exists int
	err = row.Scan(&exists)
	if err != nil {
		return false
	}

	return true
}

// Create - create a new record in the database.
func (m *Model) Create(
	ctx context.Context,
	item any,
) (uuid.UUID, error) {
	v := reflect.ValueOf(item).Elem()
	idField := v.FieldByName("ID")
	if idField.IsValid() && idField.IsZero() && m.getColumn("ID") == "id" {
		newID := uuid.New()
		if idField.CanSet() {
			idField.Set(reflect.ValueOf(newID))
		}
	}

	var insertedID uuid.UUID
	err := m.doInTransaction(ctx, func(tx pgx.Tx) error {
		statement := m.insertStatement(item)
		query, args, err := statement.Suffix("RETURNING id").ToSql()
		if err != nil {
			err = fmt.Errorf("failed to build SQL query: %w", err)
			tracing.Error(ctx, err)
			return err
		}
		tracing.SpanEvent(ctx, "Insert Item")
		tracing.TraceVal(ctx, "SQL", query)
		for i, arg := range args {
			tracing.TraceIVal(ctx, "arg-"+fmt.Sprint(i), arg)
		}

		row := tx.QueryRow(ctx, query, args...)
		if err := row.Scan(&insertedID); err != nil {
			err = fmt.Errorf("failed to scan inserted ID: %w", err)
			tracing.Error(ctx, err)
			return err
		}
		return nil
	})
	if err != nil {
		return uuid.Nil, err
	}
	return insertedID, nil
}

// Update - update an existing record in the database.
func (m *Model) Update(
	ctx context.Context,
	item any, // item is the data to be updated in the database
	searchFieldName string, // Name of the field to search for the record to update
	searchFieldValue any, // Value of the field to search for the record to update
) error {
	return m.doInTransaction(ctx, func(tx pgx.Tx) error {
		statement := m.updateStatement(item).Where(sq.Eq{m.getColumn(searchFieldName): searchFieldValue})
		query, args, err := statement.ToSql()
		if err != nil {
			err = fmt.Errorf("failed to build SQL query: %w", err)
			tracing.Error(ctx, err)
			return err
		}
		tracing.SpanEvent(ctx, "Update Item")
		tracing.TraceVal(ctx, "SQL", query)
		for i, arg := range args {
			tracing.TraceIVal(ctx, "arg-"+fmt.Sprint(i), arg)
		}

		cmd, err := tx.Exec(ctx, query, args...)
		if err != nil {
			err = fmt.Errorf("failed to execute update: %w", err)
			tracing.Error(ctx, err)
			return err
		}

		if cmd.RowsAffected() == 0 {
			err = fmt.Errorf("no rows were updated")
			tracing.Error(ctx, err)
			return err
		}
		return nil
	})
}

// Patch - patch an existing record in the database.
func (m *Model) Patch(
	ctx context.Context,
	searchFieldName string, // Name of the field to search for the record to patch
	searchFieldValue any, // Value of the field to search for the record to patch
	setFieldName string, // Name of the field to set in the record
	setFieldValue any, // Value to set for the field in the record
) error {
	return m.doInTransaction(ctx, func(tx pgx.Tx) error {
		statement := sq.Update(fmt.Sprintf("%s.%s", m.Schema, m.Table)).
			Set(m.getColumn(setFieldName), setFieldValue).
			Where(sq.Eq{m.getColumn(searchFieldName): searchFieldValue}).
			PlaceholderFormat(sq.Dollar)

		query, args, err := statement.ToSql()
		if err != nil {
			err = fmt.Errorf("failed to build SQL query: %w", err)
			tracing.Error(ctx, err)
			return err
		}
		tracing.SpanEvent(ctx, "Patch Item")
		tracing.TraceVal(ctx, "SQL", query)
		for i, arg := range args {
			tracing.TraceIVal(ctx, "arg-"+fmt.Sprint(i), arg)
		}

		cmd, err := tx.Exec(ctx, query, args...)
		if err != nil {
			err = fmt.Errorf("failed to execute patch: %w", err)
			tracing.Error(ctx, err)
			return err
		}

		if cmd.RowsAffected() == 0 {
			err = fmt.Errorf("no rows were patched")
			tracing.Error(ctx, err)
			return err
		}
		return nil
	})
}

// Delete - delete a record from the database.
func (m *Model) Delete(
	ctx context.Context,
	searchFieldName string, // Name of the field to search for the record to delete
	searchFieldValue any, // Value of the field to search for the record to delete
) error {
	return m.doInTransaction(ctx, func(tx pgx.Tx) error {
		statement := sq.Delete(fmt.Sprintf("%s.%s", m.Schema, m.Table)).
			Where(sq.Eq{m.getColumn(searchFieldName): searchFieldValue}).
			PlaceholderFormat(sq.Dollar)

		query, args, err := statement.ToSql()
		if err != nil {
			err = fmt.Errorf("failed to build SQL query: %w", err)
			tracing.Error(ctx, err)
			return err
		}
		tracing.SpanEvent(ctx, "Delete Item")
		tracing.TraceVal(ctx, "SQL", query)
		for i, arg := range args {
			tracing.TraceIVal(ctx, "arg-"+fmt.Sprint(i), arg)
		}

		cmd, err := tx.Exec(ctx, query, args...)
		if err != nil {
			err = fmt.Errorf("failed to execute delete: %w", err)
			tracing.Error(ctx, err)
			return err
		}

		if cmd.RowsAffected() == 0 {
			err = fmt.Errorf("no rows were deleted")
			tracing.Error(ctx, err)
			return err
		}
		return nil
	})
}

// TableIndex - Return count of inserted, updated and deleted rows in table (using for calculate table changes).
func (m *Model) TableIndex(
	ctx context.Context,
) (*string, error) {
	return m.executeTableIndex(ctx)
}

// getColumn - get the database column name for a given field name.
func (m *Model) getColumn(field string) string {
	if dbColumn, ok := m.DbFieldCash[field]; ok {
		return dbColumn
	}

	// Inline logic from getDBColumnFromStruct
	t := reflect.TypeOf(m.Item)
	structField, ok := t.FieldByName(field)
	if ok {
		tag := structField.Tag.Get("db")
		if tag != "" {
			m.DbFieldCash[field] = tag
			return tag
		}
	}

	// if not found in cache or struct, return empty string
	err := fmt.Errorf("field %s not found in cache or struct", field)
	tracing.Error(context.Background(), err)

	return ""
}

// collectColumns returns db column names for all db-tagged fields in t,
// recursively expanding anonymous (embedded) struct fields.
func collectColumns(t reflect.Type) []string {
	var cols []string
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.Anonymous {
			ft := f.Type
			if ft.Kind() == reflect.Ptr {
				ft = ft.Elem()
			}
			cols = append(cols, collectColumns(ft)...)
		} else if tag := f.Tag.Get("db"); tag != "" {
			cols = append(cols, tag)
		}
	}
	return cols
}

// collectValues returns parallel slices of db column names and their values for t/v,
// recursively expanding anonymous (embedded) struct fields.
func collectValues(t reflect.Type, v reflect.Value) ([]string, []interface{}) {
	var cols []string
	var vals []interface{}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		fv := v.Field(i)
		if f.Anonymous {
			ft := f.Type
			if ft.Kind() == reflect.Ptr {
				if fv.IsNil() {
					continue
				}
				ft, fv = ft.Elem(), fv.Elem()
			}
			sc, sv := collectValues(ft, fv)
			cols = append(cols, sc...)
			vals = append(vals, sv...)
		} else if tag := f.Tag.Get("db"); tag != "" {
			cols = append(cols, tag)
			vals = append(vals, fv.Interface())
		}
	}
	return cols, vals
}

// collectFieldValues returns addressable reflect.Values for every db-tagged field in v,
// in the same column order as collectColumns. Used by scanRow.
func collectFieldValues(t reflect.Type, v reflect.Value) []reflect.Value {
	var fields []reflect.Value
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		fv := v.Field(i)
		if f.Anonymous {
			ft := f.Type
			if ft.Kind() == reflect.Ptr {
				if fv.IsNil() {
					fv.Set(reflect.New(ft.Elem()))
				}
				ft, fv = ft.Elem(), fv.Elem()
			}
			fields = append(fields, collectFieldValues(ft, fv)...)
		} else if f.Tag.Get("db") != "" {
			fields = append(fields, fv)
		}
	}
	return fields
}

// selectStatement - create a select statement for the model's table.
func (m *Model) selectStatement() sq.SelectBuilder {
	statement := sq.StatementBuilder.PlaceholderFormat(sq.Dollar).Select()
	for _, col := range collectColumns(reflect.TypeOf(m.Item)) {
		statement = statement.Column(col)
	}
	return statement.From(fmt.Sprintf("%s.%s", m.Schema, m.Table))
}

// selectStatement by fields - create a select statement for the model's table with specific fields.
func (m *Model) selectStatementByFields(fields []string) sq.SelectBuilder {
	statement := sq.StatementBuilder.PlaceholderFormat(sq.Dollar).Select()

	// Iterate over the provided fields
	for _, field := range fields {
		dbColumn := m.getColumn(field)
		if dbColumn != "" {
			statement = statement.Column(dbColumn)
		}
	}

	return statement.From(fmt.Sprintf("%s.%s", m.Schema, m.Table))
}

// insertStatement - create an insert statement for the given item.
func (m *Model) insertStatement(item any) *sq.InsertBuilder {
	statement := sq.StatementBuilder.PlaceholderFormat(sq.Dollar).Insert(fmt.Sprintf("%s.%s", m.Schema, m.Table))
	t := reflect.TypeOf(item)
	v := reflect.ValueOf(item)
	if t.Kind() == reflect.Ptr {
		t, v = t.Elem(), v.Elem()
	}
	cols, vals := collectValues(t, v)
	builder := statement.Columns(cols...).Values(vals...)
	return &builder
}

// updateStatement - create an update statement for the given item.
func (m *Model) updateStatement(item any) *sq.UpdateBuilder {
	statement := sq.StatementBuilder.PlaceholderFormat(sq.Dollar).Update(fmt.Sprintf("%s.%s", m.Schema, m.Table))
	t := reflect.TypeOf(item)
	v := reflect.ValueOf(item)
	if t.Kind() == reflect.Ptr {
		t, v = t.Elem(), v.Elem()
	}
	cols, vals := collectValues(t, v)
	setClauses := make(map[string]interface{}, len(cols))
	for i, col := range cols {
		setClauses[col] = vals[i]
	}
	builder := statement.SetMap(setClauses)
	return &builder
}

// orderAndPaging - apply ordering and pagination to the SQL statement.
func (m *Model) orderAndPaging(
	statement sq.SelectBuilder,
	order Order,
) sq.SelectBuilder {
	// order
	if order.By != "" {
		column := m.getColumn(order.By)
		if order.Dir == "desc" {
			statement = statement.OrderBy(column + " DESC")
		} else {
			statement = statement.OrderBy(column + " ASC")
		}
	}

	// page and limit PTO
	if order.Limit == 0 {
		order.Limit = 25 // Default to 100 items per page if not specified
	}
	statement = statement.Offset((order.Page) * order.Limit).Limit(order.Limit)

	return statement
}

// executeList - execute a SQL SELECT statement and return a list of items.
func (m *Model) executeList(
	ctx context.Context,
	statement sq.SelectBuilder,
) ([]any, error) {
	query, args, err := statement.ToSql()
	if err != nil {
		err = fmt.Errorf("failed to build SQL query: %w", err)
		tracing.Error(ctx, err)
		return nil, err
	}

	tracing.SpanEvent(ctx, "Select Items")
	tracing.TraceVal(ctx, "SQL", query)
	for i, arg := range args {
		tracing.TraceIVal(ctx, "arg-"+fmt.Sprint(i), arg)
	}

	rows, err := m.Client.Query(ctx, query, args...)
	if err != nil {
		err = fmt.Errorf("failed to execute query: %w", err)
		tracing.Error(ctx, err)
		return nil, err
	}
	defer rows.Close()

	items := make([]any, 0)
	for rows.Next() {
		item := reflect.New(reflect.TypeOf(m.Item)).Interface()
		err = m.scanRow(rows, item)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}

	return items, nil
}

// executeFieldValues - execute a SQL SELECT statement and return a list of values of a single field.
func (m *Model) executeFieldValues(
	ctx context.Context,
	statement sq.SelectBuilder,
	fieldType reflect.Type,
) ([]any, error) {
	query, args, err := statement.ToSql()
	if err != nil {
		err = fmt.Errorf("failed to build SQL query: %w", err)
		tracing.Error(ctx, err)
		return nil, err
	}
	tracing.SpanEvent(ctx, "Select Items")
	tracing.TraceVal(ctx, "SQL", query)
	for i, arg := range args {
		tracing.TraceIVal(ctx, "arg-"+fmt.Sprint(i), arg)
	}

	rows, err := m.Client.Query(ctx, query, args...)
	if err != nil {
		err = fmt.Errorf("failed to execute query: %w", err)
		tracing.Error(ctx, err)
		return nil, err
	}

	defer rows.Close()

	values := make([]any, 0)
	for rows.Next() {
		value := reflect.New(fieldType).Interface()
		if err := rows.Scan(value); err != nil {
			return nil, err
		}
		// Dereference pointer if needed
		values = append(values, reflect.ValueOf(value).Elem().Interface())
	}

	return values, nil
}

// executeGet - execute a SQL SELECT statement and return a single item.
func (m *Model) executeGet(
	ctx context.Context,
	statement sq.SelectBuilder,
) (any, error) {
	query, args, err := statement.ToSql()
	if err != nil {
		err = fmt.Errorf("failed to build SQL query: %w", err)
		tracing.Error(ctx, err)
		return nil, err
	}
	tracing.SpanEvent(ctx, "Get Item")
	tracing.TraceVal(ctx, "SQL", query)
	for i, arg := range args {
		tracing.TraceIVal(ctx, "arg-"+fmt.Sprint(i), arg)
	}

	row := m.Client.QueryRow(ctx, query, args...)

	item := reflect.New(reflect.TypeOf(m.Item)).Interface()
	err = m.scanRow(row, item)
	if err != nil {
		return nil, fmt.Errorf("failed to scan row: %w", err)
	}

	return item, nil
}

// executeInsert - execute an insert statement and return the number of affected rows.
func (m *Model) executeInsert(
	ctx context.Context,
	statement *sq.InsertBuilder,
) error {
	query, args, err := statement.ToSql()
	if err != nil {
		err = fmt.Errorf("failed to build SQL query: %w", err)
		tracing.Error(ctx, err)
		return err
	}
	tracing.SpanEvent(ctx, "Insert Item")
	tracing.TraceVal(ctx, "SQL", query)
	for i, arg := range args {
		tracing.TraceIVal(ctx, "arg-"+fmt.Sprint(i), arg)
	}

	cmd, err := m.Client.Exec(ctx, query, args...)
	if err != nil {
		err = fmt.Errorf("failed to execute insert: %w", err)
		tracing.Error(ctx, err)
		return err
	}

	if cmd.RowsAffected() == 0 {
		err = fmt.Errorf("no rows were inserted")
		tracing.Error(ctx, err)
		return err
	}

	return nil
}

// executeUpdate - execute an update statement and return the number of affected rows.
func (m *Model) executeUpdate(
	ctx context.Context,
	statement *sq.UpdateBuilder,
) error {
	query, args, err := statement.ToSql()
	if err != nil {
		err = fmt.Errorf("failed to build SQL query: %w", err)
		tracing.Error(ctx, err)
		return err
	}
	tracing.SpanEvent(ctx, "Update Item")
	tracing.TraceVal(ctx, "SQL", query)
	for i, arg := range args {
		tracing.TraceIVal(ctx, "arg-"+fmt.Sprint(i), arg)
	}

	cmd, err := m.Client.Exec(ctx, query, args...)
	if err != nil {
		err = fmt.Errorf("failed to execute update: %w", err)
		tracing.Error(ctx, err)
		return err
	}

	if cmd.RowsAffected() == 0 {
		err = fmt.Errorf("no rows were updated")
		tracing.Error(ctx, err)
		return err
	}

	return nil
}

// executeDelete - execute a delete statement and return the number of affected rows.
func (m *Model) executeDelete(
	ctx context.Context,
	statement *sq.DeleteBuilder,
) error {
	query, args, err := statement.ToSql()
	if err != nil {
		err = fmt.Errorf("failed to build SQL query: %w", err)
		tracing.Error(ctx, err)
		return err
	}
	tracing.SpanEvent(ctx, "Delete Item")
	tracing.TraceVal(ctx, "SQL", query)
	for i, arg := range args {
		tracing.TraceIVal(ctx, "arg-"+fmt.Sprint(i), arg)
	}

	cmd, err := m.Client.Exec(ctx, query, args...)
	if err != nil {
		err = fmt.Errorf("failed to execute delete: %w", err)
		tracing.Error(ctx, err)
		return err
	}

	if cmd.RowsAffected() == 0 {
		err = fmt.Errorf("no rows were deleted")
		tracing.Error(ctx, err)
		return err
	}

	return nil
}

// executeTableIndex - execute a query to get the table index.
func (m *Model) executeTableIndex(
	ctx context.Context,
) (*string, error) {
	query, args, err := sq.
		Select("concat(n_tup_ins, n_tup_upd, n_tup_del, n_tup_hot_upd, n_live_tup, n_dead_tup, n_mod_since_analyze, n_ins_since_vacuum)").
		From("pg_stat_user_tables").
		Where(sq.Eq{"relname": m.Table, "schemaname": m.Schema}).
		PlaceholderFormat(sq.Dollar).
		ToSql()

	if err != nil {
		tracing.Error(ctx, err)
		return nil, err
	}

	row := m.Client.QueryRow(ctx, query, args...)
	var data *string
	if err := row.Scan(&data); err != nil {
		tracing.Error(ctx, err)
		return nil, err
	}

	// get short geo_hash of data
	hash := crypt.ShortHash(*data)
	return &hash, nil
}

// scanRow - scan a single row into the provided destination struct.
func (m *Model) scanRow(row pgx.Row, dest any) error {
	v := reflect.ValueOf(dest)
	if v.Kind() != reflect.Ptr || v.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("dest must be pointer to struct")
	}
	v = v.Elem()

	fieldVals := collectFieldValues(v.Type(), v)
	scanArgs := make([]interface{}, len(fieldVals))
	postScan := make([]func(), 0, len(fieldVals))

	for i, fv := range fieldVals {
		fv := fv // per-iteration capture
		switch fv.Type() {
		case reflect.TypeOf(""): // string
			ptr := new(sql.NullString)
			scanArgs[i] = ptr
			postScan = append(postScan, func() {
				if ptr.Valid {
					fv.SetString(ptr.String)
				}
			})
		case reflect.TypeOf(sql.NullString{}): // sql.NullString
			scanArgs[i] = fv.Addr().Interface()
		case reflect.TypeOf(uuid.UUID{}): // uuid.UUID
			ptr := new(sql.NullString)
			scanArgs[i] = ptr
			postScan = append(postScan, func() {
				if ptr.Valid {
					if u, err := uuid.Parse(ptr.String); err == nil {
						fv.Set(reflect.ValueOf(u))
					}
				}
			})
		case reflect.TypeOf(false): // bool
			ptr := new(sql.NullBool)
			scanArgs[i] = ptr
			postScan = append(postScan, func() {
				if ptr.Valid {
					fv.SetBool(ptr.Bool)
				}
			})
		case reflect.TypeOf(time.Time{}): // time.Time
			ptr := new(sql.NullTime)
			scanArgs[i] = ptr
			postScan = append(postScan, func() {
				if ptr.Valid {
					fv.Set(reflect.ValueOf(ptr.Time))
				}
			})
		case reflect.TypeOf(sql.NullTime{}): // sql.NullTime
			scanArgs[i] = fv.Addr().Interface()
		case reflect.TypeOf(int(0)): // int
			ptr := new(sql.NullInt64)
			scanArgs[i] = ptr
			postScan = append(postScan, func() {
				if ptr.Valid {
					fv.SetInt(ptr.Int64)
				}
			})
		case reflect.TypeOf(int64(0)): // int64
			ptr := new(sql.NullInt64)
			scanArgs[i] = ptr
			postScan = append(postScan, func() {
				if ptr.Valid {
					fv.SetInt(ptr.Int64)
				}
			})
		case reflect.TypeOf(int32(0)): // int32
			ptr := new(sql.NullInt32)
			scanArgs[i] = ptr
			postScan = append(postScan, func() {
				if ptr.Valid {
					fv.SetInt(int64(ptr.Int32))
				}
			})
		case reflect.TypeOf(int16(0)): // int16
			ptr := new(sql.NullInt16)
			scanArgs[i] = ptr
			postScan = append(postScan, func() {
				if ptr.Valid {
					fv.SetInt(int64(ptr.Int16))
				}
			})
		case reflect.TypeOf(int8(0)): // int8
			ptr := new(sql.NullInt16)
			scanArgs[i] = ptr
			postScan = append(postScan, func() {
				if ptr.Valid {
					fv.SetInt(int64(ptr.Int16))
				}
			})
		case reflect.TypeOf(uint64(0)): // uint64
			ptr := new(sql.NullInt64)
			scanArgs[i] = ptr
			postScan = append(postScan, func() {
				if ptr.Valid {
					fv.SetUint(uint64(ptr.Int64))
				}
			})
		case reflect.TypeOf(uint32(0)): // uint32
			ptr := new(sql.NullInt64)
			scanArgs[i] = ptr
			postScan = append(postScan, func() {
				if ptr.Valid {
					fv.SetUint(uint64(ptr.Int64))
				}
			})
		case reflect.TypeOf(uint16(0)): // uint16
			ptr := new(sql.NullInt64)
			scanArgs[i] = ptr
			postScan = append(postScan, func() {
				if ptr.Valid {
					fv.SetUint(uint64(ptr.Int64))
				}
			})
		case reflect.TypeOf(uint8(0)): // uint8
			ptr := new(sql.NullInt64)
			scanArgs[i] = ptr
			postScan = append(postScan, func() {
				if ptr.Valid {
					fv.SetUint(uint64(ptr.Int64))
				}
			})
		case reflect.TypeOf(float64(0)): // float64
			ptr := new(sql.NullFloat64)
			scanArgs[i] = ptr
			postScan = append(postScan, func() {
				if ptr.Valid {
					fv.SetFloat(ptr.Float64)
				}
			})
		case reflect.TypeOf(float32(0)): // float32
			ptr := new(sql.NullFloat64)
			scanArgs[i] = ptr
			postScan = append(postScan, func() {
				if ptr.Valid {
					fv.SetFloat(ptr.Float64)
				}
			})
		case reflect.TypeOf([]byte{}): // []byte
			ptr := new([]byte)
			scanArgs[i] = ptr
			postScan = append(postScan, func() {
				fv.Set(reflect.ValueOf(*ptr))
			})
		case reflect.TypeOf((*string)(nil)): // *string
			ptr := new(sql.NullString)
			scanArgs[i] = ptr
			postScan = append(postScan, func() {
				if ptr.Valid {
					str := ptr.String
					fv.Set(reflect.ValueOf(&str))
				} else {
					fv.Set(reflect.ValueOf((*string)(nil)))
				}
			})
		case reflect.TypeOf((*int64)(nil)): // *int64
			ptr := new(sql.NullInt64)
			scanArgs[i] = ptr
			postScan = append(postScan, func() {
				if ptr.Valid {
					val := ptr.Int64
					fv.Set(reflect.ValueOf(&val))
				} else {
					fv.Set(reflect.ValueOf((*int64)(nil)))
				}
			})
		case reflect.TypeOf((*int)(nil)): // *int
			ptr := new(sql.NullInt64)
			scanArgs[i] = ptr
			postScan = append(postScan, func() {
				if ptr.Valid {
					val := int(ptr.Int64)
					fv.Set(reflect.ValueOf(&val))
				} else {
					fv.Set(reflect.ValueOf((*int)(nil)))
				}
			})
		case reflect.TypeOf((*int32)(nil)): // *int32
			ptr := new(sql.NullInt32)
			scanArgs[i] = ptr
			postScan = append(postScan, func() {
				if ptr.Valid {
					val := ptr.Int32
					fv.Set(reflect.ValueOf(&val))
				} else {
					fv.Set(reflect.ValueOf((*int32)(nil)))
				}
			})
		case reflect.TypeOf((*int16)(nil)): // *int16
			ptr := new(sql.NullInt16)
			scanArgs[i] = ptr
			postScan = append(postScan, func() {
				if ptr.Valid {
					val := ptr.Int16
					fv.Set(reflect.ValueOf(&val))
				} else {
					fv.Set(reflect.ValueOf((*int16)(nil)))
				}
			})
		case reflect.TypeOf((*float64)(nil)): // *float64
			ptr := new(sql.NullFloat64)
			scanArgs[i] = ptr
			postScan = append(postScan, func() {
				if ptr.Valid {
					val := ptr.Float64
					fv.Set(reflect.ValueOf(&val))
				} else {
					fv.Set(reflect.ValueOf((*float64)(nil)))
				}
			})
		case reflect.TypeOf((*float32)(nil)): // *float32
			ptr := new(sql.NullFloat64)
			scanArgs[i] = ptr
			postScan = append(postScan, func() {
				if ptr.Valid {
					val := float32(ptr.Float64)
					fv.Set(reflect.ValueOf(&val))
				} else {
					fv.Set(reflect.ValueOf((*float32)(nil)))
				}
			})
		case reflect.TypeOf((*bool)(nil)): // *bool
			ptr := new(sql.NullBool)
			scanArgs[i] = ptr
			postScan = append(postScan, func() {
				if ptr.Valid {
					val := ptr.Bool
					fv.Set(reflect.ValueOf(&val))
				} else {
					fv.Set(reflect.ValueOf((*bool)(nil)))
				}
			})
		case reflect.TypeOf((*time.Time)(nil)): // *time.Time
			ptr := new(sql.NullTime)
			scanArgs[i] = ptr
			postScan = append(postScan, func() {
				if ptr.Valid {
					val := ptr.Time
					fv.Set(reflect.ValueOf(&val))
				} else {
					fv.Set(reflect.ValueOf((*time.Time)(nil)))
				}
			})
		case reflect.TypeOf((*uuid.UUID)(nil)): // *uuid.UUID
			ptr := new(sql.NullString)
			scanArgs[i] = ptr
			postScan = append(postScan, func() {
				if ptr.Valid {
					if u, err := uuid.Parse(ptr.String); err == nil {
						fv.Set(reflect.ValueOf(&u))
					}
				} else {
					fv.Set(reflect.ValueOf((*uuid.UUID)(nil)))
				}
			})
		default:
			ft := fv.Type()
			if ft.Kind() == reflect.Struct || (ft.Kind() == reflect.Ptr && ft.Elem().Kind() == reflect.Struct) {
				ptr := new([]byte)
				scanArgs[i] = ptr
				postScan = append(postScan, func() {
					if ptr != nil && len(*ptr) > 0 {
						if jsonErr := UnmarshalJSON(*ptr, fv.Addr().Interface()); jsonErr != nil {
							tracing.Error(context.Background(), fmt.Errorf("failed to unmarshal JSON: %w", jsonErr))
						}
					}
				})
			} else {
				return fmt.Errorf("unsupported field type: %s", fv.Type())
			}
		}
	}

	if err := row.Scan(scanArgs...); err != nil {
		return err
	}
	for _, fn := range postScan {
		fn()
	}
	return nil
}

func (m *Model) doInTransaction(ctx context.Context, fn func(tx pgx.Tx) error) error {
	tx, err := m.Client.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback(ctx)
			panic(p)
		}
	}()
	if err := fn(tx); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	return tx.Commit(ctx)
}

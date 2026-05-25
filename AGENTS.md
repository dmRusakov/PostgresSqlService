# PostgresSqlService

Generic PostgreSQL service layer for Go. Provides reflection-based ORM, automatic schema/table creation from struct tags, connection pooling, DTO transformations, and validation.

Module: `github.com/dmRusakov/PostgresSqlService`

---

## Files

| File | Purpose |
|------|---------|
| `db.go` | `Client` interface, `NewClient`, `IsSchemaExists`, `IsTableExists`, helpers |
| `entity.go` | `Config`, `Parameters`, `Order` structs |
| `model.go` | Low-level ORM (`Model`) + auto DDL generation |
| `service.go` | High-level `Service` wrapping `Model` with DTO / validation |

---

## Quick Start

```go
service := postgressql.NewService(postgressql.Parameters{
    DB:           db,
    Schema:       "myschema",
    Table:        "users",
    Item:         User{},
    SearchFields: []string{"Name", "Email"},
    DtoFunc: &map[string]func(any) any{
        "Create": func(data any) any {
            u := data.(*User)
            u.CreatedAt = time.Now()
            u.UpdatedAt = time.Now()
            return u
        },
    },
    ValidateFunc: &map[string]func(any) error{
        "Create": func(data any) error {
            u := data.(*User)
            if u.Name == "" {
                return fmt.Errorf("name is required")
            }
            return nil
        },
    },
})
```

`NewService` automatically ensures the schema exists, creates the table if absent (DDL derived from struct tags), and optionally seeds rows from a CSV file.

---

## entity.go

### Config
```go
type Config struct {
    Host     string
    Port     string
    User     string
    Password string
    DB       string
}
```

### Parameters
```go
type Parameters struct {
    DB           Client
    Schema       string
    Table        string
    Item         any                          // zero value of the target struct
    SearchFields []string                     // fields available for Search()
    DtoFunc      *map[string]func(any) any    // keyed by "Create","Update","Patch","Delete"
    ValidateFunc *map[string]func(any) error  // keyed by "Create","Update","Patch","Delete"
    InitData     string                       // optional path to a CSV file for initial seeding
}
```

`InitData` is loaded only when the table is first created. It must point to a CSV file (first row = column headers matching `db` tags). The path is relative to the process working directory.

### Order
```go
type Order struct {
    By    string `json:"by"`    // struct field name to sort by
    Dir   string `json:"dir"`   // "asc" or "desc"
    Page  uint64 `json:"page"`  // 0-based page index
    Limit uint64 `json:"limit"` // rows per page (default 25)
}
```

---

## db.go

### NewClient
```go
func NewClient(
    ctx         context.Context,
    maxAttempts int,
    maxDelay    time.Duration,
    config      Config,
    binary      bool,           // true → QueryExecModeCacheDescribe
) (*pgxpool.Pool, error)
```

### Helpers
```go
func IsSchemaExists(ctx context.Context, client Client, schemaName string) (bool, error)
func IsTableExists(ctx context.Context, client Client, schemaName, tableName string) (bool, error)
func DoWithAttempts(fn func() error, maxAttempts int, delay time.Duration) error
func UnmarshalJSON(data []byte, dest any) error
```

---

## Struct Tags

Tags control both runtime field mapping and auto-generated DDL.

| Tag | Values | Effect |
|-----|--------|--------|
| `db:"col"` | column name | maps Go field ↔ DB column; required |
| `pg:"TYPE"` | SQL type string | overrides the inferred SQL type |
| `pk:"true"` | — | marks column as PRIMARY KEY; `uuid.UUID` also gets `DEFAULT uuid_generate_v4()` |
| `default:"expr"` | SQL expression | adds `DEFAULT expr` to the column |
| `index:"true"` | — | creates a B-tree index |
| `index:"unique"` | — | creates a UNIQUE index |
| `index:"gist"` | — | creates a GIST index |
| `fk:"tbl.col"` | qualified ref | adds `REFERENCES tbl.col` |
| `fk:"tbl.col:cascade"` | — | adds `REFERENCES tbl.col ON DELETE CASCADE` |

**Auto rules applied during DDL generation:**

- Field named `ID`, type `uuid.UUID`, `db:"id"` at top-level depth → auto PRIMARY KEY with `uuid_generate_v4()`
- Fields named `created_at`, `updated_at`, `audited_at`, `started_at` → auto `DEFAULT NOW()`
- Pointer fields (`*T`) → nullable (no `NOT NULL`)
- Non-pointer fields without a default → `NOT NULL`
- Embedded (anonymous) struct fields are flattened into columns; `pk`/`fk`/`index` tags are only honoured on top-level fields

### Type Inference

| Go type | SQL type |
|---------|----------|
| `uuid.UUID` | `UUID` |
| `time.Time` | `TIMESTAMPTZ` |
| `string` | `TEXT` |
| `bool` | `BOOLEAN` |
| `int`, `int32` | `INT` |
| `int64` | `BIGINT` |
| `int16` | `SMALLINT` |
| `float32` | `REAL` |
| `float64` | `DOUBLE PRECISION` |
| `[]byte` | `BYTEA` |
| pointer to any above | same type, nullable |

Override with `pg:"TYPE"` when you need `VARCHAR(n)`, `CHAR(1)`, `GEOMETRY`, etc.

### Example Entity
```go
type Product struct {
    ID        uuid.UUID  `db:"id"          json:"id"`
    SKU       string     `db:"sku"         json:"sku"          pg:"VARCHAR(32)"  index:"unique"`
    Name      string     `db:"name"        json:"name"`
    Price     float64    `db:"price"       json:"price"`
    CategoryID uuid.UUID `db:"category_id" json:"category_id"  fk:"catalog.categories.id:cascade"`
    IsActive  bool       `db:"is_active"   json:"is_active"    default:"true"`
    CreatedAt time.Time  `db:"created_at"  json:"created_at"`
    UpdatedAt time.Time  `db:"updated_at"  json:"updated_at"`
    DeletedAt *time.Time `db:"deleted_at"  json:"deleted_at,omitempty"`
}
```

Generated DDL (simplified):
```sql
CREATE TABLE IF NOT EXISTS "myschema"."products" (
    "id"          UUID          PRIMARY KEY DEFAULT public.uuid_generate_v4(),
    "sku"         VARCHAR(32)   NOT NULL,
    "name"        TEXT          NOT NULL,
    "price"       DOUBLE PRECISION NOT NULL,
    "category_id" UUID          NOT NULL REFERENCES catalog.categories.id ON DELETE CASCADE,
    "is_active"   BOOLEAN       DEFAULT true NOT NULL,
    "created_at"  TIMESTAMPTZ   DEFAULT NOW() NOT NULL,
    "updated_at"  TIMESTAMPTZ   DEFAULT NOW() NOT NULL,
    "deleted_at"  TIMESTAMPTZ
);
CREATE UNIQUE INDEX IF NOT EXISTS "idx_myschema_products_sku" ON "myschema"."products" ("sku");
```

---

## service.go

### NewService
```go
func NewService(p Parameters) *Service
```

Single constructor. Calls `NewStorage` internally which:
1. Ensures `uuid-ossp` extension exists
2. Creates schema if absent
3. Creates table if absent (DDL from struct tags)
4. Seeds table from `p.InitData` CSV if set and table was just created

### ServiceInterface
```go
type ServiceInterface interface {
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
```

### Operation Flow

Mutating operations (`Create`, `Update`, `Patch`, `Delete`):
1. Run `DtoFunc[operation]` if defined — transforms / enriches the data
2. Run `ValidateFunc[operation]` if defined — returns error on failure
3. Execute the DB operation in a transaction

Read operations (`Get`, `List`, `Search`, `FieldValues`, `Exists`, `TableIndex`) bypass DTO/validation.

### Methods

```go
// Returns single record by field match
Get(ctx, fieldName string, fieldValue any) (any, error)

// Returns true if any record matches
Exists(ctx, fieldName string, fieldValue any) bool

// Returns filtered, paginated records
// Prefix field name with "!" for NOT IN: map[string][]any{"!Status": {"deleted"}}
List(ctx, filters map[string][]any, order Order) ([]any, error)

// Returns distinct values for a single column
FieldValues(ctx, fieldName string, filters map[string][]any, order Order) ([]any, error)

// LIKE search across all SearchFields
Search(ctx, query string, order Order) ([]any, error)

// Insert; auto-generates UUID ID if the field is zero
Create(ctx, entity any) (uuid.UUID, error)

// Replace all fields of matching record
Update(ctx, entity any, searchField string, searchValue any) error

// Update a single column
Patch(ctx, searchField string, searchValue any, setField string, setValue any) error

// Delete matching record
Delete(ctx, searchField string, searchValue any) error

// Returns hash of pg_stat_user_tables counters (insert/update/delete totals)
TableIndex(ctx) (*string, error)
```

### DtoFunc Patterns
```go
DtoFunc: &map[string]func(any) any{
    "Create": func(data any) any {
        d := data.(*MyEntity)
        now := time.Now()
        if d.CreatedAt.IsZero() {
            d.CreatedAt = now
        }
        d.UpdatedAt = now
        if d.Status == "" {
            d.Status = "active"
        }
        return d
    },
    "Update": func(data any) any {
        d := data.(*MyEntity)
        d.UpdatedAt = time.Now()
        return d
    },
},
```

### ValidateFunc Patterns
```go
ValidateFunc: &map[string]func(any) error{
    "Create": func(data any) error {
        d := data.(*MyEntity)
        if d.Name == "" {
            return fmt.Errorf("name is required")
        }
        return nil
    },
},
```

---

## model.go internals

### NewStorage (called by NewService)
```go
func NewStorage(
    client       Client,
    schema       string,
    table        string,
    item         any,
    searchFields []string,
    initData     string,   // path to CSV seed file, or ""
) *Model
```

Panics on schema/table creation failure so misconfiguration surfaces at startup.

### buildTableDDL
Reflects over `item`, respects all struct tags listed above, and returns a `CREATE TABLE IF NOT EXISTS` statement plus a slice of `CREATE INDEX` statements. Called automatically by `makeTableIfNotExist`.

### loadCSV
```go
func (m *Model) loadCSV(ctx context.Context, path string) error
```
Opens a CSV file, treats the first row as column headers (must match `db` tag values), and bulk-inserts all remaining rows. Called once, only when the table is first created.

---

## Negative Filters

Prefix the field name with `!` in `List` filters to generate a `NOT IN` clause:

```go
// WHERE status NOT IN ('deleted', 'archived')
List(ctx, map[string][]any{"!Status": {"deleted", "archived"}}, order)
```

---

## Working Directory Note

`InitData` paths are resolved relative to the **process working directory**, not the source file. Use paths relative to where the binary is run from (e.g. `"internal/policy/s_57_data/init_data/AttDecode.csv"`).

# PostgresSqlService

A Go library that turns a struct into a fully operational PostgreSQL service — schema creation, table DDL, CRUD, search, pagination, DTO transformations, and validation — all driven by struct field tags.

```
github.com/dmRusakov/PostgresSqlService
```

---

## Table of Contents

- [Installation](#installation)
- [How It Works](#how-it-works)
- [Struct Tags](#struct-tags)
- [App Setup](#app-setup)
- [API Reference](#api-reference)
- [Unit Testing](#unit-testing)

---

## Installation

```bash
go get github.com/dmRusakov/PostgresSqlService
```

For a local module (monorepo / replace directive):

```go
// go.mod
require github.com/dmRusakov/PostgresSqlService v0.0.0

replace github.com/dmRusakov/PostgresSqlService => ../PostgresSqlService
```

---

## How It Works

`NewService` is the only entry point you need:

```
NewService(Parameters)
    └─ NewStorage(...)
          ├─ CREATE EXTENSION IF NOT EXISTS "uuid-ossp"
          ├─ CREATE SCHEMA IF NOT EXISTS "<schema>"
          ├─ CREATE TABLE IF NOT EXISTS "<schema>"."<table>" (DDL from struct tags)
          ├─ CREATE INDEX ... (from index tags)
          └─ seed from CSV (if InitData is set and table was just created)
```

Everything is idempotent — safe to call on every startup.

---

## Struct Tags

| Tag | Example | Effect |
|-----|---------|--------|
| `db:"col"` | `db:"user_name"` | maps field to DB column (required) |
| `pg:"TYPE"` | `pg:"VARCHAR(64)"` | override inferred SQL type |
| `pk:"true"` | `pk:"true"` | PRIMARY KEY; `uuid.UUID` also gets `DEFAULT uuid_generate_v4()` |
| `default:"expr"` | `default:"true"` | SQL DEFAULT expression |
| `index:"true"` | `index:"true"` | B-tree index |
| `index:"unique"` | `index:"unique"` | UNIQUE index |
| `index:"gist"` | `index:"gist"` | GIST index (geometry, etc.) |
| `fk:"tbl.col"` | `fk:"public.roles.id"` | REFERENCES constraint |
| `fk:"tbl.col:cascade"` | `fk:"public.roles.id:cascade"` | REFERENCES … ON DELETE CASCADE |

**Auto rules:**

- Field `ID uuid.UUID` with `db:"id"` at the top struct level → auto PRIMARY KEY + `uuid_generate_v4()`
- Fields named `created_at`, `updated_at`, `audited_at`, `started_at` → auto `DEFAULT NOW()`
- Non-pointer fields without a DEFAULT → `NOT NULL`
- Pointer fields (`*T`) → nullable
- Embedded (anonymous) structs are flattened; `pk`/`fk`/`index` tags only apply to top-level fields

**Go → SQL type inference:**

| Go | SQL |
|----|-----|
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

Override with `pg:"TYPE"` for `VARCHAR(n)`, `CHAR(1)`, `GEOMETRY`, etc.

---

## App Setup

### 1. Define the entity

```go
// internal/policy/product/entity.go
package product

import (
    "time"
    "github.com/google/uuid"
)

type Product struct {
    ID         uuid.UUID  `db:"id"          json:"id"`
    SKU        string     `db:"sku"         json:"sku"          pg:"VARCHAR(32)"  index:"unique"`
    Name       string     `db:"name"        json:"name"`
    Price      float64    `db:"price"       json:"price"`
    IsActive   bool       `db:"is_active"   json:"is_active"    default:"true"`
    CreatedAt  time.Time  `db:"created_at"  json:"created_at"`
    UpdatedAt  time.Time  `db:"updated_at"  json:"updated_at"`
    ArchivedAt *time.Time `db:"archived_at" json:"archived_at,omitempty"`
}
```

### 2. Create the service

```go
// internal/policy/product/policy.go
package product

import (
    "time"
    postgressql "github.com/dmRusakov/PostgresSqlService"
    "github.com/jackc/pgx/v5/pgxpool"
)

type Policy struct {
    Service *postgressql.Service
}

func NewPolicy(db *pgxpool.Pool) *Policy {
    return &Policy{
        Service: postgressql.NewService(postgressql.Parameters{
            DB:           db,
            Schema:       "store",
            Table:        "products",
            Item:         Product{},
            SearchFields: []string{"SKU", "Name"},
            DtoFunc: &map[string]func(any) any{
                "Create": func(data any) any {
                    d := data.(*Product)
                    now := time.Now()
                    if d.CreatedAt.IsZero() {
                        d.CreatedAt = now
                    }
                    d.UpdatedAt = now
                    return d
                },
                "Update": func(data any) any {
                    d := data.(*Product)
                    d.UpdatedAt = time.Now()
                    return d
                },
            },
            ValidateFunc: &map[string]func(any) error{
                "Create": func(data any) error {
                    d := data.(*Product)
                    if d.SKU == "" {
                        return fmt.Errorf("sku is required")
                    }
                    return nil
                },
            },
        }),
    }
}
```

### 3. Connect and initialize

```go
// main.go or app_init
import postgressql "github.com/dmRusakov/PostgresSqlService"

pool, err := postgressql.NewClient(
    context.Background(),
    5,                  // retry attempts
    5*time.Second,      // delay between retries
    postgressql.Config{
        Host:     "localhost",
        Port:     "5432",
        User:     "myuser",
        Password: "secret",
        DB:       "mydb",
    },
    true, // binary protocol
)
if err != nil {
    log.Fatal(err)
}

productPolicy := product.NewPolicy(pool)
```

### 4. Use it

```go
ctx := context.Background()

// Create
id, err := productPolicy.Service.Create(ctx, &product.Product{
    SKU:  "ABC-001",
    Name: "Widget",
    Price: 9.99,
})

// Get
raw, err := productPolicy.Service.Get(ctx, "SKU", "ABC-001")
p := raw.(*product.Product)

// List with filter and pagination
items, err := productPolicy.Service.List(ctx,
    map[string][]any{
        "IsActive": {true},
        "!SKU":     {"DISCONTINUED"}, // NOT IN
    },
    postgressql.Order{By: "CreatedAt", Dir: "desc", Page: 0, Limit: 20},
)

// Search across SearchFields
results, err := productPolicy.Service.Search(ctx, "widget", postgressql.Order{Limit: 10})

// Update
err = productPolicy.Service.Update(ctx, p, "ID", p.ID)

// Patch single field
err = productPolicy.Service.Patch(ctx, "ID", p.ID, "IsActive", false)

// Delete
err = productPolicy.Service.Delete(ctx, "ID", p.ID)
```

### Seeding a lookup table from CSV

```go
postgressql.NewService(postgressql.Parameters{
    DB:       db,
    Schema:   "ref",
    Table:    "countries",
    Item:     Country{},
    InitData: "internal/policy/ref/init_data/countries.csv", // relative to binary CWD
})
```

The CSV first row must be column names matching `db` tag values. Seeding runs once, only when the table is first created.

---

## API Reference

### Service methods

```go
// Single record by field match
Get(ctx, fieldName string, fieldValue any) (any, error)

// True if any record matches
Exists(ctx, fieldName string, fieldValue any) bool

// Filtered + paginated list; prefix field with "!" for NOT IN
List(ctx, filters map[string][]any, order Order) ([]any, error)

// Distinct values for one column
FieldValues(ctx, fieldName string, filters map[string][]any, order Order) ([]any, error)

// LIKE search across SearchFields
Search(ctx, query string, order Order) ([]any, error)

// Insert; auto-assigns UUID ID if zero
Create(ctx, entity any) (uuid.UUID, error)

// Replace all fields of matching record
Update(ctx, entity any, searchField string, searchValue any) error

// Update single column
Patch(ctx, searchField string, searchValue any, setField string, setValue any) error

// Delete matching record
Delete(ctx, searchField string, searchValue any) error

// Hash of pg_stat_user_tables change counters (useful for cache invalidation)
TableIndex(ctx) (*string, error)
```

---

## Unit Testing

### Strategy A — interface mock (pure unit tests, no DB)

`ServiceInterface` is fully defined. Create a mock for code that depends on the service.

```go
// product/mock_service_test.go
package product_test

import (
    "context"
    postgressql "github.com/dmRusakov/PostgresSqlService"
    "github.com/google/uuid"
)

type mockProductService struct {
    items map[uuid.UUID]*Product
}

func newMockService() *mockProductService {
    return &mockProductService{items: map[uuid.UUID]*Product{}}
}

func (m *mockProductService) Create(_ context.Context, entity any) (uuid.UUID, error) {
    p := entity.(*Product)
    if p.ID == uuid.Nil {
        p.ID = uuid.New()
    }
    m.items[p.ID] = p
    return p.ID, nil
}

func (m *mockProductService) Get(_ context.Context, field string, value any) (any, error) {
    for _, p := range m.items {
        if field == "ID" && p.ID == value.(uuid.UUID) {
            return p, nil
        }
    }
    return nil, fmt.Errorf("not found")
}

// implement remaining ServiceInterface methods as needed ...
func (m *mockProductService) Exists(_ context.Context, _ string, _ any) bool      { return false }
func (m *mockProductService) List(_ context.Context, _ map[string][]any, _ postgressql.Order) ([]any, error) {
    return nil, nil
}
// etc.
```

Use the mock in tests:

```go
func TestCreateProduct(t *testing.T) {
    svc := newMockService()
    id, err := svc.Create(context.Background(), &Product{SKU: "X1", Name: "Test"})
    require.NoError(t, err)
    require.NotEqual(t, uuid.Nil, id)
}
```

### Strategy B — nil client (DDL introspection, no DB)

Pass `nil` as `DB` to skip all DB operations. The `Service` struct is created and the model is initialized without touching Postgres. Useful for testing DDL generation or DTO logic in isolation.

```go
func TestProductDtoSetsUpdatedAt(t *testing.T) {
    svc := postgressql.NewService(postgressql.Parameters{
        DB:    nil, // no DB connection — skips schema/table creation
        Schema: "store",
        Table:  "products",
        Item:   Product{},
        DtoFunc: &map[string]func(any) any{
            "Update": func(data any) any {
                d := data.(*Product)
                d.UpdatedAt = time.Now()
                return d
            },
        },
    })
    _ = svc // inspect dtoFunc behaviour without needing a live database
}
```

### Strategy C — real Postgres via testcontainers (integration tests)

```go
// product/integration_test.go
//go:build integration

package product_test

import (
    "context"
    "testing"
    "time"

    "github.com/stretchr/testify/require"
    "github.com/testcontainers/testcontainers-go"
    "github.com/testcontainers/testcontainers-go/modules/postgres"
    postgressql "github.com/dmRusakov/PostgresSqlService"
)

func TestProductCRUD(t *testing.T) {
    ctx := context.Background()

    pgc, err := postgres.RunContainer(ctx,
        testcontainers.WithImage("postgres:16"),
        postgres.WithDatabase("testdb"),
        postgres.WithUsername("test"),
        postgres.WithPassword("test"),
        testcontainers.WithWaitStrategy(/* ready check */),
    )
    require.NoError(t, err)
    t.Cleanup(func() { _ = pgc.Terminate(ctx) })

    connStr, err := pgc.ConnectionString(ctx, "sslmode=disable")
    require.NoError(t, err)

    pool, err := postgressql.NewClient(ctx, 3, time.Second,
        postgressql.Config{/* parse connStr or fill fields directly */}, false)
    require.NoError(t, err)

    svc := postgressql.NewService(postgressql.Parameters{
        DB:     pool,
        Schema: "store",
        Table:  "products",
        Item:   Product{},
    })

    // Table is created automatically — run your tests
    id, err := svc.Create(ctx, &Product{SKU: "T-001", Name: "Test Widget", Price: 1.99})
    require.NoError(t, err)
    require.NotEqual(t, uuid.Nil, id)

    raw, err := svc.Get(ctx, "ID", id)
    require.NoError(t, err)
    require.Equal(t, "T-001", raw.(*Product).SKU)
}
```

Run integration tests separately:

```bash
go test -tags integration ./...
```

---

## Dependencies

| Package | Purpose |
|---------|---------|
| `github.com/jackc/pgx/v5` | PostgreSQL driver and connection pool |
| `github.com/Masterminds/squirrel` | SQL query builder |
| `github.com/google/uuid` | UUID generation |
| `go.opentelemetry.io/otel` | Distributed tracing |
| `golang.org/x/crypto` | Hashing utilities |

---

## License

See [LICENSE](LICENSE).

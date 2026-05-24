# PostgreSQL Package (pkg/postgressql)

## Package Overview
Generic database abstraction layer for PostgreSQL with reflection-based ORM capabilities, connection pooling, service layer pattern, and built-in validation/DTO transformations.

## Files Structure
- `db.go` - Database client initialization and connection management
- `entity.go` - Core data structures (Config, Order)
- `model.go` - Low-level ORM with CRUD operations
- `service.go` - High-level service layer with validation and DTO transformations

---

## db.go

### Client Interface
Abstract interface wrapping `pgxpool.Pool` for database operations.

```go
type Client interface {
	Close()
	Acquire(ctx context.Context) (*pgxpool.Conn, error)
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Begin(ctx context.Context) (pgx.Tx, error)
	BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error)
}
```

### NewClient
```go
func NewClient(
	ctx context.Context,
	maxAttempts int,
	maxDelay time.Duration,
	config Config,
	binary bool,
) (*pgxpool.Pool, error)
```

**Parameters:**
- `ctx`: Context for connection initialization
- `maxAttempts`: Number of connection retry attempts
- `maxDelay`: Delay between retry attempts
- `config`: Database connection configuration
- `binary`: Use binary protocol (QueryExecModeCacheDescribe)

**Returns:** Connected `pgxpool.Pool` or error

**Example:**
```go
config := postgressql.Config{
	Host:     "192.168.12.214",
	Port:     "9932",
	User:     "fooDbAdmin",
	Password: "secret",
	DB:       "FreeOpenOcean",
}

pool, err := postgressql.NewClient(
	context.Background(),
	5,
	5*time.Second,
	config,
	true,
)
```

### DoWithAttempts
```go
func DoWithAttempts(fn func() error, maxAttempts int, delay time.Duration) error
```
Retry helper that executes a function with exponential backoff.

### ConnStringFromCfg
```go
func (c *Config) ConnStringFromCfg() string
```
Converts `Config` struct to PostgreSQL connection string (DSN).
Returns format: `postgres://user:password@host:port/database`

### UnmarshalJSON
```go
func UnmarshalJSON(data []byte, dest any) error
```
JSON unmarshaler for JSONB/JSON database columns.

---

## entity.go

### Config
Database connection configuration.

```go
type Config struct {
	Host     string
	Port     string
	User     string
	Password string
	DB       string
}
```

### Order
Query ordering and pagination configuration.

```go
type Order struct {
	By    string `json:"by"`    // Field name to order by (struct field name)
	Dir   string `json:"dir"`   // "asc" or "desc"
	Page  uint64 `json:"page"`  // Page number (0-based)
	Limit uint64 `json:"limit"` // Items per page (default: 25)
}
```

---

## model.go

### Model Overview
The `Model` struct provides a generic database abstraction layer for PostgreSQL operations using reflection-based field mapping and prepared statements through the Squirrel query builder.

## Initialization

### NewStorage
```go
func NewStorage(
	client Client,
	schema string,
	table string,
	item any,
	searchFields []string,
	validateFunc *map[string]func(any) error,
) *Model
```

**Parameters:**
- `client`: PostgreSQL client (pgxpool.Pool connection)
- `schema`: Database schema name (e.g., "public", "logs")
- `table`: Table name (e.g., "users", "logs")
- `item`: Zero value of the struct type to work with (used for reflection)
- `searchFields`: Fields allowed in Search() operations (for full-text search)
- `validateFunc`: Optional map of validation functions keyed by operation name ("Create", "Update", "Delete", "Patch")

**Example:**
```go
validateFuncs := map[string]func(any) error{
	"Create": validateCreateLog,
	"Update": validateUpdateLog,
	"Delete": validateDeleteLog,
}

storage := NewStorage(
	db,
	"logs",
	"logs",
	Log{},
	[]string{"AppName", "Message"},
	&validateFuncs,
)
```

## Field Mapping

### Database Tags
Use `db` tags on struct fields to map to database columns:

```go
type Log struct {
	ID        string     `db:"id"`
	AppName   string     `db:"app_name"`
	Level     *int       `db:"level"`
	Message   string     `db:"message"`
	CreatedAt time.Time  `db:"created_at"`
}
```

## CRUD Operations

### Create
```go
func (m *Model) Create(ctx context.Context, item any) (uuid.UUID, error)
```
- Automatically generates UUID for `ID` field if empty
- Runs `ValidateFunc["Create"]` if defined
- Executes within a transaction
- Returns the generated ID or error

**Validation:** Called before insert with the full item struct

### Read Operations

#### Get
```go
func (m *Model) Get(ctx context.Context, searchFieldName string, searchFieldValue any) (any, error)
```
- Retrieves single record by field match
- Returns struct pointer or error

#### List
```go
func (m *Model) List(ctx context.Context, filters map[string][]any, order Order) ([]any, error)
```
- Returns multiple records with optional filters and pagination
- Filters: `map[string][]any{"FieldName": {value1, value2}}`
- Order: `Order{By: "FieldName", Dir: "asc", Page: 0, Limit: 25}`

#### Search
```go
func (m *Model) Search(ctx context.Context, value string, order Order) ([]any, error)
```
- Full-text LIKE search across SearchFields
- Requires SearchFields to be defined in NewStorage

#### Exists
```go
func (m *Model) Exists(ctx context.Context, searchFieldName string, searchFieldValue any) bool
```
- Checks if record exists, returns boolean

### Update
```go
func (m *Model) Update(ctx context.Context, item any, searchFieldName string, searchFieldValue any) error
```
- Updates entire record matching search condition
- Runs `ValidateFunc["Update"]` if defined
- Validation receives the full item struct
- Executes within a transaction
- Returns error if no rows affected

### Patch
```go
func (m *Model) Patch(ctx context.Context, searchFieldName string, searchFieldValue any, setFieldName string, setFieldValue any) error
```
- Updates single field in record matching search condition
- Runs `ValidateFunc["Patch"]` if defined
- Validation receives map: `{searchFieldName: searchFieldValue, setFieldName: setFieldValue}`
- Executes within a transaction
- Returns error if no rows affected

### Delete
```go
func (m *Model) Delete(ctx context.Context, searchFieldName string, searchFieldValue any) error
```
- Deletes record matching search condition
- Runs `ValidateFunc["Delete"]` if defined
- Validation receives map: `{searchFieldName: searchFieldValue}`
- Executes within a transaction
- Returns error if no rows affected

## Validation Functions

### Structure
Validation functions have signature: `func(any) error`

### Naming Convention
Map keys must match operation names exactly:
- `"Create"` - for Create operations
- `"Update"` - for Update operations
- `"Patch"` - for Patch operations
- `"Delete"` - for Delete operations

### Execution Order
1. Validation function runs (if defined and match exists)
2. If validation returns error → operation stops, error returned
3. If validation passes → operation proceeds

### Example Implementation
```go
func ValidateCreateLog(data any) error {
	log, ok := data.(*logger.Log)
	if !ok {
		return fmt.Errorf("invalid data type")
	}
	
	if log.AppName == "" {
		return fmt.Errorf("AppName is required")
	}
	if log.Message == "" {
		return fmt.Errorf("Message is required")
	}
	
	return nil
}

func ValidateDeleteLog(data any) error {
	validateData, ok := data.(map[string]any)
	if !ok {
		return fmt.Errorf("invalid validation data")
	}
	
	id, ok := validateData["ID"]
	if !ok || id == "" {
		return fmt.Errorf("ID is required for deletion")
	}
	
	return nil
}
```

## Error Handling

### Standard Pattern
All operations return errors with context:
```go
- "failed to build SQL query" - query construction failed
- "failed to execute [operation]" - database execution failed
- "no rows were [operation]ed" - operation affected zero rows
- "field [name] not found in struct" - mapping error
```

### Transactions
- All mutating operations (Create, Update, Patch, Delete) use transactions
- Automatic rollback on error
- Automatic commit on success
- Panic recovery with rollback included

## Tracing Integration

All operations include tracing via `pkg/tracing`:
- `tracing.SpanEvent()` - marks operation checkpoint
- `tracing.TraceVal()` - logs SQL query
- `tracing.TraceIVal()` - logs query arguments
- `tracing.Error()` - logs errors with context

## Type Support

### Supported Field Types
- `string`, `*string`
- `int`, `uint32`, `uint64`, `*int`
- `bool`, `*bool`
- `time.Time`, `*time.Time`
- `uuid.UUID`, `*uuid.UUID`
- `sql.NullString`, `sql.NullBool`, `sql.NullTime`
- Struct/pointer-to-struct (JSON unmarshaling)

## Best Practices

1. **Always use pointers for nullable fields:** `*string`, `*int`
2. **Define SearchFields for full-text search:** Only fields in SearchFields are searchable
3. **Use ValidateFunc for business logic:** Separate validation from storage concerns
4. **Check row counts on update/delete:** Use error return to verify operations succeeded
5. **Cache DbFieldCash:** Field-to-column mappings are cached automatically
6. **Handle context cancellation:** Pass context with timeout to prevent hanging queries

## Common Patterns

### CRUD Service Wrapper
```go
type LogService struct {
	storage *postgressql.Model
	logger  *logger.Logger
}

func (s *LogService) CreateLog(ctx context.Context, appName, message string) error {
	log := &logger.Log{
		AppName: appName,
		Message: message,
	}
	_, err := s.storage.Create(ctx, log)
	if err != nil {
		s.logger.Log(s.logger.MakeError(3, "kR9jL2", err.Error()))
	}
	return err
}
```

### Filter Example
```go
results, err := storage.List(ctx,
	map[string][]any{
		"Level": {1, 2},  // WHERE level IN (1, 2)
		"AppName": {"MyApp"},
	},
	postgressql.Order{By: "CreatedAt", Dir: "desc", Page: 0, Limit: 10},
)
```

---

## service.go

### Service Overview
High-level service layer wrapping `Model` with additional capabilities:
- **DTO transformations** - Transform data before operations
- **Validation** - Validate data before operations
- **Simplified interface** - Clean API for business logic

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

### Service Struct
```go
type Service struct {
	model        *Model
	dtoFunc      *map[string]func(any) any      // DTO transformations
	validateFunc *map[string]func(any) error    // Validation functions
}
```

### NewService
```go
func NewService(
	storage *Model,
	dtoFunc *map[string]func(any) any,
	validateFunc *map[string]func(any) error,
) *Service
```

**Parameters:**
- `storage`: Initialized `Model` instance
- `dtoFunc`: Optional DTO transformation functions keyed by operation ("Create", "Update", "Patch", "Delete")
- `validateFunc`: Optional validation functions keyed by operation

**Example:**
```go
dtoFuncs := map[string]func(any) any{
	"Create": func(data any) any {
		log := data.(*Log)
		log.CreatedAt = time.Now()
		return log
	},
}

validateFuncs := map[string]func(any) error{
	"Create": func(data any) error {
		log := data.(*Log)
		if log.Message == "" {
			return fmt.Errorf("message is required")
		}
		return nil
	},
}

service := postgressql.NewService(model, &dtoFuncs, &validateFuncs)
```

### Execution Flow
All mutation operations follow this pattern:

1. **DTO Transformation** (if defined)
   - Runs `dtoFunc[operationName]`
   - Transforms/enriches data

2. **Validation** (if defined)
   - Runs `validateFunc[operationName]`
   - Returns error on validation failure

3. **Model Operation**
   - Executes database operation
   - Returns result or error

### Operation Methods

#### Create
```go
func (s *Service) Create(ctx context.Context, entity any) (uuid.UUID, error)
```
- DTO receives: full entity
- Validation receives: transformed entity
- Returns generated UUID or error

#### Update
```go
func (s *Service) Update(ctx context.Context, entity any, field string, value any) error
```
- DTO receives: full entity
- Validation receives: transformed entity
- Updates all fields of matching record

#### Patch
```go
func (s *Service) Patch(ctx context.Context, fieldToFind string, valueToFind any, fieldToUpdate string, valueToUpdate any) error
```
- DTO receives: `map[string]any{fieldToFind: valueToFind, fieldToUpdate: valueToUpdate}`
- Validation receives: transformed map
- Updates single field of matching record

#### Delete
```go
func (s *Service) Delete(ctx context.Context, field string, value any) error
```
- DTO receives: `map[string]any{field: value}`
- Validation receives: transformed map
- Deletes matching record

### Read Operations
Read operations (`Get`, `List`, `Search`, `FieldValues`, `Exists`, `TableIndex`) bypass DTO/validation and call model directly.

### DTO Function Structure
DTO functions have signature: `func(any) any`

**Example:**
```go
dtoFuncs := map[string]func(any) any{
	"Create": func(data any) any {
		log := data.(*logger.Log)
		log.CreatedAt = time.Now()
		log.ID = uuid.New()
		return log
	},
	"Patch": func(data any) any {
		patchData := data.(map[string]any)
		patchData["UpdatedAt"] = time.Now()
		return patchData
	},
}
```

### Validation Function Structure
Validation functions have signature: `func(any) error`

**Example:**
```go
validateFuncs := map[string]func(any) error{
	"Create": func(data any) error {
		log := data.(*logger.Log)
		if log.AppName == "" {
			return fmt.Errorf("AppName is required")
		}
		if log.Message == "" {
			return fmt.Errorf("Message is required")
		}
		return nil
	},
	"Delete": func(data any) error {
		deleteData := data.(map[string]any)
		id, ok := deleteData["ID"]
		if !ok || id == uuid.Nil {
			return fmt.Errorf("valid ID is required for deletion")
		}
		return nil
	},
}
```

---

## Creating Go Structs from Database Tables

### Overview
Convert SQL table definitions to Go structs with proper field mapping, types, and tags.

### Step-by-Step Process

#### 1. Analyze Table Schema
```sql
CREATE TABLE map_objects (
    id         uuid         default uuid_generate_v4() not null primary key,
    name       varchar(255) default NULL::character varying,
    s_57_code  varchar(10)                             not null unique,
    is_active  boolean      default true,
    created_at timestamp    default CURRENT_TIMESTAMP  not null,
    updated_at timestamp    default CURRENT_TIMESTAMP  not null
);
```

#### 2. Create Struct with Field Mapping

**File Location:** `internal/entity/db/{schema}/{TableName}.go`

```go
package {schema}

import (
	"time"
	"github.com/google/uuid"
)

const {TableName}SchemaName = "{schema}"
const {TableName}TableName = "{table_name}"

type {TableName} struct {
	ID        uuid.UUID `db:"id" json:"id"`
	Name      *string   `db:"name" json:"name,omitempty"`
	S57Code   string    `db:"s_57_code" json:"s_57_code"`
	IsActive  bool      `db:"is_active" json:"is_active"`
	CreatedAt time.Time `db:"created_at" json:"created_at"`
	UpdatedAt time.Time `db:"updated_at" json:"updated_at"`
}

// Create{TableName}Params keeps required fields for inserts.
type Create{TableName}Params struct {
	Name     *string `db:"name" json:"name,omitempty"`
	S57Code  string  `db:"s_57_code" json:"s_57_code" validate:"required"`
	IsActive *bool   `db:"is_active" json:"is_active,omitempty"`
}
```

#### 3. Type Mapping Reference

| SQL Type | Go Type | Notes |
|----------|---------|-------|
| `uuid` | `uuid.UUID` | Primary keys, foreign keys |
| `varchar(n)`, `text` | `string` | Required fields |
| `varchar(n)`, `text` | `*string` | Nullable fields |
| `integer`, `bigint` | `int`, `int64` | Numbers |
| `integer`, `bigint` | `*int64` | Nullable numbers |
| `boolean` | `bool` | True/false |
| `timestamp`, `timestamptz` | `time.Time` | Dates |
| `jsonb`, `json` | `map[string]interface{}` | JSON objects |
| `bytea` | `[]byte` | Binary data |

#### 4. Tag Conventions
- `db:"column_name"` - Maps to database column
- `json:"field_name,omitempty"` - JSON serialization, omit empty
- `validate:"required"` - Validation tags (optional)

#### 5. Directory Structure
```
internal/entity/db/
├── {schema1}/
│   ├── Table1.go
│   └── Table2.go
└── {schema2}/
    └── Table3.go
```

### Example: MapObjects Struct

```go
package map_metadata

import (
	"time"
	"github.com/google/uuid"
)

const MapObjectsSchemaName = "map_metadata"
const MapObjectsTableName = "map_objects"

type MapObjects struct {
	ID        uuid.UUID `db:"id" json:"id"`
	Name      *string   `db:"name" json:"name,omitempty"`
	S57Code   string    `db:"s_57_code" json:"s_57_code"`
	IsActive  bool      `db:"is_active" json:"is_active"`
	CreatedAt time.Time `db:"created_at" json:"created_at"`
	UpdatedAt time.Time `db:"updated_at" json:"updated_at"`
}

type CreateMapObjectsParams struct {
	Name     *string `db:"name" json:"name,omitempty"`
	S57Code  string  `db:"s_57_code" json:"s_57_code" validate:"required"`
	IsActive *bool   `db:"is_active" json:"is_active,omitempty"`
}
```

---

## Service Initialization Patterns

### Overview
Initialize `postgressql.Service` instances with proper DTO transformations and validation.

### Basic Service Initialization

#### 1. Simple Service (No DTO/Validation)
```go
service := postgressql.NewService(
	postgressql.NewStorage(
		db,
		"schema",
		"table",
		Entity{},
		[]string{"Field1", "Field2"}, // Search fields
		nil, // No validation
	),
	nil, // No DTO
	nil, // No validation
)
```

#### 2. Service with DTO and Validation
```go
// DTO Functions
dtoFuncs := map[string]func(any) any{
	"Create": func(data any) any {
		entity := data.(*Entity)
		entity.ID = uuid.New()
		entity.CreatedAt = time.Now()
		entity.UpdatedAt = time.Now()
		return entity
	},
	"Update": func(data any) any {
		entity := data.(*Entity)
		entity.UpdatedAt = time.Now()
		return entity
	},
}

// Validation Functions
validateFuncs := map[string]func(any) error{
	"Create": func(data any) error {
		entity := data.(*Entity)
		if entity.RequiredField == "" {
			return fmt.Errorf("required_field is required")
		}
		return nil
	},
}

service := postgressql.NewService(
	postgressql.NewStorage(db, "schema", "table", Entity{}, searchFields, nil),
	&dtoFuncs,
	&validateFuncs,
)
```

### Application-Level Initialization

#### 1. App Structure
```go
type MyApp struct {
	Services struct {
		Entity1 *postgressql.Service
		Entity2 *postgressql.Service
	}
}
```

#### 2. Goroutine-Based Initialization
```go
func init() {
	var wg sync.WaitGroup
	
	// Initialize multiple services concurrently
	wg.Add(2)
	
	go func() {
		defer wg.Done()
		app.Services.Entity1 = postgressql.NewService(
			postgressql.NewStorage(db, "schema1", "table1", Entity1{}, searchFields1, nil),
			&dtoFuncs1,
			&validateFuncs1,
		)
	}()
	
	go func() {
		defer wg.Done()
		app.Services.Entity2 = postgressql.NewService(
			postgressql.NewStorage(db, "schema2", "table2", Entity2{}, searchFields2, nil),
			&dtoFuncs2,
			&validateFuncs2,
		)
	}()
	
	wg.Wait() // Wait for all services to initialize
}
```

### DTO Function Patterns

#### Create DTO - Auto-generate fields
```go
"Create": func(data any) any {
	entity := data.(*Entity)
	
	// Auto-generate ID if not set
	if entity.ID == uuid.Nil {
		entity.ID = uuid.New()
	}
	
	// Set timestamps
	now := time.Now()
	entity.CreatedAt = now
	entity.UpdatedAt = now
	
	// Set defaults
	if entity.Status == "" {
		entity.Status = "active"
	}
	
	return entity
}
```

#### Update DTO - Update timestamps
```go
"Update": func(data any) any {
	entity := data.(*Entity)
	entity.UpdatedAt = time.Now()
	return entity
}
```

#### Patch DTO - Handle partial updates
```go
"Patch": func(data any) any {
	patchData := data.(map[string]any)
	
	// Add audit fields
	patchData["UpdatedAt"] = time.Now()
	patchData["UpdatedBy"] = getCurrentUser()
	
	return patchData
}
```

### Validation Function Patterns

#### Create Validation - Required fields
```go
"Create": func(data any) error {
	entity := data.(*Entity)
	
	if entity.Name == "" {
		return fmt.Errorf("name is required")
	}
	
	if entity.Email != "" && !isValidEmail(entity.Email) {
		return fmt.Errorf("invalid email format")
	}
	
	return nil
}
```

#### Delete Validation - Permission checks
```go
"Delete": func(data any) error {
	deleteData := data.(map[string]any)
	
	id, ok := deleteData["ID"]
	if !ok {
		return fmt.Errorf("ID is required for deletion")
	}
	
	// Check permissions
	if !hasPermissionToDelete(id) {
		return fmt.Errorf("insufficient permissions to delete")
	}
	
	return nil
}
```

### Complete Example: MapObjects Service

```go
// DTO Functions
dtoFuncs := map[string]func(any) any{
	"Create": func(data any) any {
		obj := data.(*map_metadata.MapObjects)
		
		obj.ID = uuid.New()
		obj.CreatedAt = time.Now()
		obj.UpdatedAt = time.Now()
		if !obj.IsActive {
			obj.IsActive = true // Default to active
		}
		
		return obj
	},
	"Update": func(data any) any {
		obj := data.(*map_metadata.MapObjects)
		obj.UpdatedAt = time.Now()
		return obj
	},
}

// Validation Functions
validateFuncs := map[string]func(any) error{
	"Create": func(data any) error {
		obj := data.(*map_metadata.MapObjects)
		
		if obj.S57Code == "" {
			return fmt.Errorf("s57_code is required")
		}
		
		return nil
	},
}

// Service Initialization
app.Services.MapObjects = postgressql.NewService(
	postgressql.NewStorage(
		app.DB,
		map_metadata.MapObjectsSchemaName,
		map_metadata.MapObjectsTableName,
		map_metadata.MapObjects{},
		[]string{"Name", "S57Code", "IsActive"},
		nil,
	),
	&dtoFuncs,
	&validateFuncs,
)
```

---

## Best Practices

1. **Always use pointers for nullable fields:** `*string`, `*int`
2. **Define SearchFields for full-text search:** Only fields in SearchFields are searchable
3. **Use ValidateFunc for business logic:** Separate validation from storage concerns
4. **Check row counts on update/delete:** Use error return to verify operations succeeded
5. **Cache DbFieldCash:** Field-to-column mappings are cached automatically
6. **Handle context cancellation:** Pass context with timeout to prevent hanging queries

## Common Patterns

### CRUD Service Wrapper
```go
type LogService struct {
	storage *postgressql.Model
	logger  *logger.Logger
}

func (s *LogService) CreateLog(ctx context.Context, appName, message string) error {
	log := &logger.Log{
		AppName: appName,
		Message: message,
	}
	_, err := s.storage.Create(ctx, log)
	if err != nil {
		s.logger.Log(s.logger.MakeError(3, "kR9jL2", err.Error()))
	}
	return err
}
```

### Filter Example
```go
results, err := storage.List(ctx,
	map[string][]any{
		"Level": {1, 2},  // WHERE level IN (1, 2)
		"AppName": {"MyApp"},
	},
	postgressql.Order{By: "CreatedAt", Dir: "desc", Page: 0, Limit: 10},
)
```

---

## service.go

### Service Overview
High-level service layer wrapping `Model` with additional capabilities:
- **DTO transformations** - Transform data before operations
- **Validation** - Validate data before operations
- **Simplified interface** - Clean API for business logic

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

### Service Struct
```go
type Service struct {
	model        *Model
	dtoFunc      *map[string]func(any) any      // DTO transformations
	validateFunc *map[string]func(any) error    // Validation functions
}
```

### NewService
```go
func NewService(
	storage *Model,
	dtoFunc *map[string]func(any) any,
	validateFunc *map[string]func(any) error,
) *Service
```

**Parameters:**
- `storage`: Initialized `Model` instance
- `dtoFunc`: Optional DTO transformation functions keyed by operation ("Create", "Update", "Patch", "Delete")
- `validateFunc`: Optional validation functions keyed by operation

**Example:**
```go
dtoFuncs := map[string]func(any) any{
	"Create": func(data any) any {
		log := data.(*Log)
		log.CreatedAt = time.Now()
		return log
	},
}

validateFuncs := map[string]func(any) error{
	"Create": func(data any) error {
		log := data.(*Log)
		if log.Message == "" {
			return fmt.Errorf("message is required")
		}
		return nil
	},
}

service := postgressql.NewService(model, &dtoFuncs, &validateFuncs)
```

### Execution Flow
All mutation operations follow this pattern:

1. **DTO Transformation** (if defined)
   - Runs `dtoFunc[operationName]`
   - Transforms/enriches data

2. **Validation** (if defined)
   - Runs `validateFunc[operationName]`
   - Returns error on validation failure

3. **Model Operation**
   - Executes database operation
   - Returns result or error

### Operation Methods

#### Create
```go
func (s *Service) Create(ctx context.Context, entity any) (uuid.UUID, error)
```
- DTO receives: full entity
- Validation receives: transformed entity
- Returns generated UUID or error

#### Update
```go
func (s *Service) Update(ctx context.Context, entity any, field string, value any) error
```
- DTO receives: full entity
- Validation receives: transformed entity
- Updates all fields of matching record

#### Patch
```go
func (s *Service) Patch(ctx context.Context, fieldToFind string, valueToFind any, fieldToUpdate string, valueToUpdate any) error
```
- DTO receives: `map[string]any{fieldToFind: valueToFind, fieldToUpdate: valueToUpdate}`
- Validation receives: transformed map
- Updates single field of matching record

#### Delete
```go
func (s *Service) Delete(ctx context.Context, field string, value any) error
```
- DTO receives: `map[string]any{field: value}`
- Validation receives: transformed map
- Deletes matching record

### Read Operations
Read operations (`Get`, `List`, `Search`, `FieldValues`, `Exists`, `TableIndex`) bypass DTO/validation and call model directly.

### DTO Function Structure
DTO functions have signature: `func(any) any`

**Example:**
```go
dtoFuncs := map[string]func(any) any{
	"Create": func(data any) any {
		log := data.(*logger.Log)
		log.CreatedAt = time.Now()
		log.ID = uuid.New()
		return log
	},
	"Patch": func(data any) any {
		patchData := data.(map[string]any)
		patchData["UpdatedAt"] = time.Now()
		return patchData
	},
}
```

### Validation Function Structure
Validation functions have signature: `func(any) error`

**Example:**
```go
validateFuncs := map[string]func(any) error{
	"Create": func(data any) error {
		log := data.(*logger.Log)
		if log.AppName == "" {
			return fmt.Errorf("AppName is required")
		}
		if log.Message == "" {
			return fmt.Errorf("Message is required")
		}
		return nil
	},
	"Delete": func(data any) error {
		deleteData := data.(map[string]any)
		id, ok := deleteData["ID"]
		if !ok || id == uuid.Nil {
			return fmt.Errorf("valid ID is required for deletion")
		}
		return nil
	},
}
```


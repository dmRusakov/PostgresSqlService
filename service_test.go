package postgressql_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"reflect"
	"testing"
	"time"

	postgressql "github.com/dmRusakov/PostgresSqlService"
	"github.com/google/uuid"
)

// ---- test entity ------------------------------------------------------------
// testItem exercises every PostgreSQL column type supported by the library.

type testItem struct {
	// Primary key — UUID, auto-generated
	ID uuid.UUID `db:"id" json:"id"`

	// String / text variants
	Name   string  `db:"name"   json:"name"`
	Code   string  `db:"code"   json:"code"   pg:"VARCHAR(16)" index:"unique"`
	Status string  `db:"status" json:"status" pg:"CHAR(1)"     default:"'A'"`
	Note   *string `db:"note"   json:"note,omitempty"`

	// Signed integer variants
	Score    int   `db:"score"    json:"score"`
	SmallVal int16 `db:"small_val" json:"small_val"` // SMALLINT
	MedVal   int32 `db:"med_val"  json:"med_val"`    // INT
	BigVal   int64 `db:"big_val"  json:"big_val"`    // BIGINT
	TinyVal  int8  `db:"tiny_val" json:"tiny_val"`   // SMALLINT (via int8)

	// Unsigned integer variants
	UScore uint   `db:"u_score"  json:"u_score"`
	USmall uint16 `db:"u_small"  json:"u_small"`
	UMed   uint32 `db:"u_med"    json:"u_med"`
	UBig   uint64 `db:"u_big"    json:"u_big"`
	UTiny  uint8  `db:"u_tiny"   json:"u_tiny"`

	// Nullable signed integers
	OptInt   *int   `db:"opt_int"   json:"opt_int,omitempty"`
	OptBig   *int64 `db:"opt_big"   json:"opt_big,omitempty"`
	OptInt8  *int8  `db:"opt_int8"  json:"opt_int8,omitempty"`
	OptInt16 *int16 `db:"opt_int16" json:"opt_int16,omitempty"`
	OptInt32 *int32 `db:"opt_int32" json:"opt_int32,omitempty"`

	// Nullable unsigned integers
	OptUint   *uint   `db:"opt_uint"   json:"opt_uint,omitempty"`
	OptUint8  *uint8  `db:"opt_uint8"  json:"opt_uint8,omitempty"`
	OptUint16 *uint16 `db:"opt_uint16" json:"opt_uint16,omitempty"`
	OptUint32 *uint32 `db:"opt_uint32" json:"opt_uint32,omitempty"`
	OptUint64 *uint64 `db:"opt_uint64" json:"opt_uint64,omitempty"`

	// Float variants
	Price      float32  `db:"price"      json:"price"      pg:"REAL"`
	Rating     float64  `db:"rating"     json:"rating"` // DOUBLE PRECISION
	OptFloat   *float64 `db:"opt_float"  json:"opt_float,omitempty"`
	OptFloat32 *float32 `db:"opt_float32" json:"opt_float32,omitempty"`

	// Boolean
	Active  bool  `db:"active"   json:"active"  default:"true"`
	OptBool *bool `db:"opt_bool" json:"opt_bool,omitempty"`

	// Nullable UUID (foreign-key style)
	OwnerID *uuid.UUID `db:"owner_id" json:"owner_id,omitempty"`

	// Binary / BYTEA
	Payload []byte `db:"payload" json:"payload,omitempty" pg:"BYTEA"`

	// Structured / JSONB
	Meta json.RawMessage `db:"meta" json:"meta,omitempty" pg:"JSONB"`

	// Timestamps
	StartAt   *time.Time `db:"start_at"   json:"start_at,omitempty"`
	CreatedAt time.Time  `db:"created_at" json:"created_at"`
	UpdatedAt time.Time  `db:"updated_at" json:"updated_at"`
}

// ---- helpers ----------------------------------------------------------------

const testSchema = "testschema"
const testTable = "items"

func newService(t *testing.T, db *postgressql.MemClient) *postgressql.Service {
	t.Helper()
	return postgressql.NewService(postgressql.Parameters{
		DB:           db,
		Schema:       testSchema,
		Table:        testTable,
		Item:         testItem{},
		SearchFields: []string{"Name", "Code"},
		DtoFunc: &map[string]func(any) any{
			"Create": func(data any) any {
				d := data.(*testItem)
				now := time.Now()
				if d.CreatedAt.IsZero() {
					d.CreatedAt = now
				}
				d.UpdatedAt = now
				return d
			},
			"Update": func(data any) any {
				d := data.(*testItem)
				d.UpdatedAt = time.Now()
				return d
			},
		},
		ValidateFunc: &map[string]func(any) error{
			"Create": func(data any) error {
				d := data.(*testItem)
				if d.Name == "" {
					return fmt.Errorf("name is required")
				}
				return nil
			},
		},
	})
}

// seedRow pre-loads a minimal row; the new fields default to zero/nil.
func seedRow(db *postgressql.MemClient, id uuid.UUID, name, code string, score int, active bool) {
	now := time.Now()
	db.Seed(testSchema, testTable, map[string]any{
		"id":         id,
		"name":       name,
		"code":       code,
		"status":     "A",
		"score":      score,
		"small_val":  int16(0),
		"med_val":    int32(0),
		"big_val":    int64(0),
		"tiny_val":   int8(0),
		"u_score":    uint(0),
		"u_small":    uint16(0),
		"u_med":      uint32(0),
		"u_big":      uint64(0),
		"u_tiny":     uint8(0),
		"price":      float32(0),
		"rating":     float64(0),
		"active":     active,
		"payload":    []byte{},
		"created_at": now,
		"updated_at": now,
	})
}

// ---- Create -----------------------------------------------------------------

func TestService_Create_succeeds(t *testing.T) {
	db := postgressql.NewMemClient()
	svc := newService(t, db)

	item := &testItem{Name: "Widget", Code: "W-001", Score: 10}
	id, err := svc.Create(context.Background(), item)
	if err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}
	if id == uuid.Nil {
		t.Fatal("Create: expected non-nil UUID")
	}

	rows := db.TableRows(testSchema, testTable)
	if len(rows) != 1 {
		t.Fatalf("Create: expected 1 stored row, got %d", len(rows))
	}
	if rows[0]["name"] != "Widget" {
		t.Errorf("Create: stored name = %v, want Widget", rows[0]["name"])
	}
}

func TestService_Create_dto_sets_timestamps(t *testing.T) {
	db := postgressql.NewMemClient()
	svc := newService(t, db)

	before := time.Now()
	item := &testItem{Name: "Thing", Code: "T-1", Score: 1}
	_, err := svc.Create(context.Background(), item)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if item.CreatedAt.Before(before) {
		t.Error("Create DTO: CreatedAt not set")
	}
	if item.UpdatedAt.Before(before) {
		t.Error("Create DTO: UpdatedAt not set")
	}
}

func TestService_Create_auto_generates_id(t *testing.T) {
	db := postgressql.NewMemClient()
	svc := newService(t, db)

	item := &testItem{Name: "Auto-ID", Code: "A-1"}
	id, err := svc.Create(context.Background(), item)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if id == uuid.Nil {
		t.Error("Create: ID should be auto-generated")
	}
}

func TestService_Create_validation_blocks_empty_name(t *testing.T) {
	db := postgressql.NewMemClient()
	svc := newService(t, db)

	item := &testItem{Code: "X-1"} // no Name
	_, err := svc.Create(context.Background(), item)
	if err == nil {
		t.Fatal("Create: expected validation error for empty name")
	}
	if len(db.TableRows(testSchema, testTable)) != 0 {
		t.Error("Create: row should not be stored when validation fails")
	}
}

func TestService_Create_records_call(t *testing.T) {
	db := postgressql.NewMemClient()
	svc := newService(t, db)

	_, _ = svc.Create(context.Background(), &testItem{Name: "Log-test", Code: "L-1"})

	found := false
	for _, c := range db.Calls {
		if c.Method == "QueryRow" {
			found = true
			break
		}
	}
	if !found {
		t.Error("Create: expected a QueryRow call recorded")
	}
}

// ---- All-types round-trip ---------------------------------------------------

func TestService_Create_Get_all_field_types(t *testing.T) {
	db := postgressql.NewMemClient()
	svc := newService(t, db)
	ctx := context.Background()

	ownerID := uuid.New()
	note := "hello world"
	optInt := 77
	optBig := int64(8_000_000_000)
	optInt8 := int8(42)
	optInt16 := int16(1000)
	optInt32 := int32(100_000)
	optUint := uint(99)
	optUint8 := uint8(200)
	optUint16 := uint16(60000)
	optUint32 := uint32(3_000_000_000)
	optUint64 := uint64(10_000_000_000)
	optFloat := 2.718281828
	optFloat32 := float32(3.14)
	optBool := true
	startAt := time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC)
	meta := json.RawMessage(`{"key":"value","n":42}`)

	in := &testItem{
		Name:       "Full",
		Code:       "F-1",
		Status:     "B",
		Note:       &note,
		Score:      100,
		SmallVal:   32767,
		MedVal:     2_147_483_647,
		BigVal:     9_000_000_000,
		TinyVal:    127,
		UScore:     42,
		USmall:     65535,
		UMed:       4_294_967_295,
		UBig:       100,
		UTiny:      255,
		OptInt:     &optInt,
		OptBig:     &optBig,
		OptInt8:    &optInt8,
		OptInt16:   &optInt16,
		OptInt32:   &optInt32,
		OptUint:    &optUint,
		OptUint8:   &optUint8,
		OptUint16:  &optUint16,
		OptUint32:  &optUint32,
		OptUint64:  &optUint64,
		Price:      19.99,
		Rating:     4.875,
		OptFloat:   &optFloat,
		OptFloat32: &optFloat32,
		Active:     true,
		OptBool:    &optBool,
		OwnerID:    &ownerID,
		Payload:    []byte{0x01, 0x02, 0xAB, 0xFF},
		Meta:       meta,
		StartAt:    &startAt,
	}

	id, err := svc.Create(ctx, in)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	raw, err := svc.Get(ctx, "ID", id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	got := raw.(*testItem)

	// string types
	check(t, "Name", "Full", got.Name)
	check(t, "Code", "F-1", got.Code)
	check(t, "Status", "B", got.Status)
	if got.Note == nil || *got.Note != note {
		t.Errorf("Note: got %v, want %q", got.Note, note)
	}

	// integer types
	check(t, "Score", 100, got.Score)
	check(t, "SmallVal", int16(32767), got.SmallVal)
	check(t, "MedVal", int32(2_147_483_647), got.MedVal)
	check(t, "BigVal", int64(9_000_000_000), got.BigVal)
	check(t, "TinyVal", int8(127), got.TinyVal)
	check(t, "UScore", uint(42), got.UScore)
	check(t, "USmall", uint16(65535), got.USmall)
	check(t, "UMed", uint32(4_294_967_295), got.UMed)
	check(t, "UBig", uint64(100), got.UBig)
	check(t, "UTiny", uint8(255), got.UTiny)
	if got.OptInt == nil || *got.OptInt != optInt {
		t.Errorf("OptInt: got %v, want %d", got.OptInt, optInt)
	}
	if got.OptBig == nil || *got.OptBig != optBig {
		t.Errorf("OptBig: got %v, want %d", got.OptBig, optBig)
	}
	if got.OptInt8 == nil || *got.OptInt8 != optInt8 {
		t.Errorf("OptInt8: got %v, want %d", got.OptInt8, optInt8)
	}
	if got.OptInt16 == nil || *got.OptInt16 != optInt16 {
		t.Errorf("OptInt16: got %v, want %d", got.OptInt16, optInt16)
	}
	if got.OptInt32 == nil || *got.OptInt32 != optInt32 {
		t.Errorf("OptInt32: got %v, want %d", got.OptInt32, optInt32)
	}

	// nullable unsigned integers
	if got.OptUint == nil || *got.OptUint != optUint {
		t.Errorf("OptUint: got %v, want %d", got.OptUint, optUint)
	}
	if got.OptUint8 == nil || *got.OptUint8 != optUint8 {
		t.Errorf("OptUint8: got %v, want %d", got.OptUint8, optUint8)
	}
	if got.OptUint16 == nil || *got.OptUint16 != optUint16 {
		t.Errorf("OptUint16: got %v, want %d", got.OptUint16, optUint16)
	}
	if got.OptUint32 == nil || *got.OptUint32 != optUint32 {
		t.Errorf("OptUint32: got %v, want %d", got.OptUint32, optUint32)
	}
	if got.OptUint64 == nil || *got.OptUint64 != optUint64 {
		t.Errorf("OptUint64: got %v, want %d", got.OptUint64, optUint64)
	}

	// float types
	if math.Abs(float64(got.Price)-19.99) > 0.001 {
		t.Errorf("Price: got %v, want ~19.99", got.Price)
	}
	if math.Abs(got.Rating-4.875) > 1e-9 {
		t.Errorf("Rating: got %v, want 4.875", got.Rating)
	}
	if got.OptFloat == nil || math.Abs(*got.OptFloat-optFloat) > 1e-9 {
		t.Errorf("OptFloat: got %v, want %v", got.OptFloat, optFloat)
	}
	if got.OptFloat32 == nil || math.Abs(float64(*got.OptFloat32)-float64(optFloat32)) > 0.001 {
		t.Errorf("OptFloat32: got %v, want %v", got.OptFloat32, optFloat32)
	}

	// bool
	check(t, "Active", true, got.Active)
	if got.OptBool == nil || *got.OptBool != optBool {
		t.Errorf("OptBool: got %v, want %v", got.OptBool, optBool)
	}

	// UUID
	if got.OwnerID == nil || *got.OwnerID != ownerID {
		t.Errorf("OwnerID: got %v, want %v", got.OwnerID, ownerID)
	}

	// binary
	if len(got.Payload) != 4 || got.Payload[2] != 0xAB {
		t.Errorf("Payload: got %v, want [1 2 171 255]", got.Payload)
	}

	// JSONB
	if string(got.Meta) != string(meta) {
		t.Errorf("Meta: got %s, want %s", got.Meta, meta)
	}

	// timestamps
	if got.StartAt == nil || !got.StartAt.Equal(startAt) {
		t.Errorf("StartAt: got %v, want %v", got.StartAt, startAt)
	}
	if got.CreatedAt.IsZero() {
		t.Error("CreatedAt should be set by DTO")
	}
}

func TestService_Nullable_fields_stay_nil_when_absent(t *testing.T) {
	db := postgressql.NewMemClient()
	svc := newService(t, db)
	ctx := context.Background()

	_, err := svc.Create(ctx, &testItem{Name: "Sparse", Code: "S-1"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	raw, err := svc.Get(ctx, "Code", "S-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	got := raw.(*testItem)

	nilChecks := []struct {
		name string
		got  any
	}{
		{"Note", got.Note},
		{"OptInt", got.OptInt},
		{"OptBig", got.OptBig},
		{"OptInt8", got.OptInt8},
		{"OptInt16", got.OptInt16},
		{"OptInt32", got.OptInt32},
		{"OptUint", got.OptUint},
		{"OptUint8", got.OptUint8},
		{"OptUint16", got.OptUint16},
		{"OptUint32", got.OptUint32},
		{"OptUint64", got.OptUint64},
		{"OptFloat", got.OptFloat},
		{"OptFloat32", got.OptFloat32},
		{"OptBool", got.OptBool},
		{"OwnerID", got.OwnerID},
		{"StartAt", got.StartAt},
	}
	for _, c := range nilChecks {
		if c.got != nil {
			rv := reflect.ValueOf(c.got)
			if rv.Kind() != reflect.Ptr || !rv.IsNil() {
				t.Errorf("%s should be nil, got %v", c.name, c.got)
			}
		}
	}
}

// ---- Get --------------------------------------------------------------------

func TestService_Get_existing(t *testing.T) {
	db := postgressql.NewMemClient()
	id := uuid.New()
	seedRow(db, id, "Gadget", "G-1", 5, true)
	svc := newService(t, db)

	raw, err := svc.Get(context.Background(), "ID", id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	got := raw.(*testItem)
	if got.Name != "Gadget" {
		t.Errorf("Get: name = %q, want Gadget", got.Name)
	}
	if got.ID != id {
		t.Errorf("Get: ID = %v, want %v", got.ID, id)
	}
}

func TestService_Get_missing_returns_error(t *testing.T) {
	db := postgressql.NewMemClient()
	svc := newService(t, db)

	_, err := svc.Get(context.Background(), "ID", uuid.New())
	if err == nil {
		t.Fatal("Get: expected error for missing row")
	}
}

// ---- Exists -----------------------------------------------------------------

func TestService_Exists_true(t *testing.T) {
	db := postgressql.NewMemClient()
	id := uuid.New()
	seedRow(db, id, "Present", "P-1", 1, true)
	svc := newService(t, db)

	if !svc.Exists(context.Background(), "ID", id) {
		t.Error("Exists: expected true for seeded row")
	}
}

func TestService_Exists_false(t *testing.T) {
	db := postgressql.NewMemClient()
	svc := newService(t, db)

	if svc.Exists(context.Background(), "ID", uuid.New()) {
		t.Error("Exists: expected false for unknown ID")
	}
}

// ---- List -------------------------------------------------------------------

func TestService_List_no_filter(t *testing.T) {
	db := postgressql.NewMemClient()
	seedRow(db, uuid.New(), "Alpha", "A-1", 1, true)
	seedRow(db, uuid.New(), "Beta", "B-1", 2, true)
	seedRow(db, uuid.New(), "Gamma", "C-1", 3, false)
	svc := newService(t, db)

	items, err := svc.List(context.Background(), nil, postgressql.Order{Limit: 25})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(items) != 3 {
		t.Errorf("List: got %d items, want 3", len(items))
	}
}

func TestService_List_with_filter(t *testing.T) {
	db := postgressql.NewMemClient()
	id1 := uuid.New()
	id2 := uuid.New()
	seedRow(db, id1, "Alpha", "A-1", 1, true)
	seedRow(db, id2, "Beta", "B-1", 2, false)
	svc := newService(t, db)

	items, err := svc.List(context.Background(),
		map[string][]any{"Active": {true}},
		postgressql.Order{Limit: 25},
	)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(items) != 1 {
		t.Errorf("List: got %d items with active=true, want 1", len(items))
	}
	if items[0].(*testItem).Name != "Alpha" {
		t.Errorf("List: got name %q, want Alpha", items[0].(*testItem).Name)
	}
}

func TestService_List_empty_table(t *testing.T) {
	db := postgressql.NewMemClient()
	svc := newService(t, db)

	items, err := svc.List(context.Background(), nil, postgressql.Order{Limit: 25})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(items) != 0 {
		t.Errorf("List: expected 0 items, got %d", len(items))
	}
}

// ---- Search -----------------------------------------------------------------

func TestService_Search_finds_match(t *testing.T) {
	db := postgressql.NewMemClient()
	seedRow(db, uuid.New(), "SuperWidget", "SW-1", 1, true)
	seedRow(db, uuid.New(), "BasicThing", "BT-1", 2, true)
	svc := newService(t, db)

	results, err := svc.Search(context.Background(), "Widget", postgressql.Order{Limit: 25})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("Search: got %d results, want 1", len(results))
	}
	if results[0].(*testItem).Name != "SuperWidget" {
		t.Errorf("Search: got %q, want SuperWidget", results[0].(*testItem).Name)
	}
}

func TestService_Search_no_match(t *testing.T) {
	db := postgressql.NewMemClient()
	seedRow(db, uuid.New(), "Alpha", "A-1", 1, true)
	svc := newService(t, db)

	results, err := svc.Search(context.Background(), "zzznomatch", postgressql.Order{Limit: 25})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("Search: expected 0 results, got %d", len(results))
	}
}

// ---- FieldValues ------------------------------------------------------------

func TestService_FieldValues_returns_column(t *testing.T) {
	db := postgressql.NewMemClient()
	seedRow(db, uuid.New(), "Alpha", "A-1", 10, true)
	seedRow(db, uuid.New(), "Beta", "B-1", 20, true)
	svc := newService(t, db)

	vals, err := svc.FieldValues(context.Background(), "Score", nil, postgressql.Order{Limit: 25})
	if err != nil {
		t.Fatalf("FieldValues: %v", err)
	}
	if len(vals) != 2 {
		t.Errorf("FieldValues: got %d values, want 2", len(vals))
	}
}

// ---- Update -----------------------------------------------------------------

func TestService_Update_changes_row(t *testing.T) {
	db := postgressql.NewMemClient()
	id := uuid.New()
	seedRow(db, id, "Old Name", "O-1", 1, true)
	svc := newService(t, db)

	updated := &testItem{ID: id, Name: "New Name", Code: "O-1", Score: 99, Active: true}
	err := svc.Update(context.Background(), updated, "ID", id)
	if err != nil {
		t.Fatalf("Update: %v", err)
	}

	rows := db.TableRows(testSchema, testTable)
	if rows[0]["name"] != "New Name" {
		t.Errorf("Update: name = %v, want New Name", rows[0]["name"])
	}
	if rows[0]["score"] != 99 {
		t.Errorf("Update: score = %v, want 99", rows[0]["score"])
	}
}

func TestService_Update_dto_sets_updated_at(t *testing.T) {
	db := postgressql.NewMemClient()
	id := uuid.New()
	seedRow(db, id, "Thing", "T-1", 1, true)
	svc := newService(t, db)

	before := time.Now()
	item := &testItem{ID: id, Name: "Thing", Code: "T-1", Score: 1, Active: true}
	_ = svc.Update(context.Background(), item, "ID", id)

	if item.UpdatedAt.Before(before) {
		t.Error("Update DTO: UpdatedAt not refreshed")
	}
}

func TestService_Update_no_match_returns_error(t *testing.T) {
	db := postgressql.NewMemClient()
	svc := newService(t, db)

	err := svc.Update(context.Background(),
		&testItem{ID: uuid.New(), Name: "Ghost", Code: "G-1"},
		"ID", uuid.New(),
	)
	if err == nil {
		t.Fatal("Update: expected error when no rows matched")
	}
}

// ---- Patch ------------------------------------------------------------------

func TestService_Patch_updates_single_field(t *testing.T) {
	db := postgressql.NewMemClient()
	id := uuid.New()
	seedRow(db, id, "Item", "I-1", 5, true)
	svc := newService(t, db)

	err := svc.Patch(context.Background(), "ID", id, "Score", 42)
	if err != nil {
		t.Fatalf("Patch: %v", err)
	}

	rows := db.TableRows(testSchema, testTable)
	if rows[0]["score"] != 42 {
		t.Errorf("Patch: score = %v, want 42", rows[0]["score"])
	}
	if rows[0]["name"] != "Item" {
		t.Errorf("Patch: name changed unexpectedly to %v", rows[0]["name"])
	}
}

func TestService_Patch_no_match_returns_error(t *testing.T) {
	db := postgressql.NewMemClient()
	svc := newService(t, db)

	err := svc.Patch(context.Background(), "ID", uuid.New(), "Score", 99)
	if err == nil {
		t.Fatal("Patch: expected error when no rows matched")
	}
}

// ---- Delete -----------------------------------------------------------------

func TestService_Delete_removes_row(t *testing.T) {
	db := postgressql.NewMemClient()
	id := uuid.New()
	seedRow(db, id, "Doomed", "D-1", 1, true)
	seedRow(db, uuid.New(), "Survivor", "S-1", 2, true)
	svc := newService(t, db)

	err := svc.Delete(context.Background(), "ID", id)
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}

	rows := db.TableRows(testSchema, testTable)
	if len(rows) != 1 {
		t.Fatalf("Delete: expected 1 remaining row, got %d", len(rows))
	}
	if rows[0]["name"] != "Survivor" {
		t.Errorf("Delete: wrong row removed; remaining = %v", rows[0]["name"])
	}
}

func TestService_Delete_no_match_returns_error(t *testing.T) {
	db := postgressql.NewMemClient()
	svc := newService(t, db)

	err := svc.Delete(context.Background(), "ID", uuid.New())
	if err == nil {
		t.Fatal("Delete: expected error when no rows matched")
	}
}

// ---- TableIndex -------------------------------------------------------------

func TestService_TableIndex_returns_hash(t *testing.T) {
	db := postgressql.NewMemClient()
	svc := newService(t, db)

	hash, err := svc.TableIndex(context.Background())
	if err != nil {
		t.Fatalf("TableIndex: %v", err)
	}
	if hash == nil || *hash == "" {
		t.Error("TableIndex: expected non-empty hash")
	}
}

// ---- DTO/Validate isolation -------------------------------------------------

func TestService_no_dto_passthrough(t *testing.T) {
	db := postgressql.NewMemClient()
	svc := postgressql.NewService(postgressql.Parameters{
		DB: db, Schema: testSchema, Table: testTable, Item: testItem{},
	})

	item := &testItem{Name: "Raw", Code: "R-1"}
	_, err := svc.Create(context.Background(), item)
	if err != nil {
		t.Fatalf("Create (no DTO): %v", err)
	}
	if !item.CreatedAt.IsZero() {
		t.Error("no-DTO service should not set CreatedAt")
	}
}

func TestService_validate_blocks_create(t *testing.T) {
	db := postgressql.NewMemClient()
	svc := postgressql.NewService(postgressql.Parameters{
		DB: db, Schema: testSchema, Table: testTable, Item: testItem{},
		ValidateFunc: &map[string]func(any) error{
			"Create": func(data any) error {
				return fmt.Errorf("always reject")
			},
		},
	})

	_, err := svc.Create(context.Background(), &testItem{Name: "X", Code: "X-1"})
	if err == nil {
		t.Fatal("expected validation error")
	}
	if len(db.TableRows(testSchema, testTable)) != 0 {
		t.Error("row should not be stored after validation failure")
	}
}

func TestService_validate_blocks_delete(t *testing.T) {
	db := postgressql.NewMemClient()
	id := uuid.New()
	seedRow(db, id, "Keep Me", "K-1", 1, true)
	svc := postgressql.NewService(postgressql.Parameters{
		DB: db, Schema: testSchema, Table: testTable, Item: testItem{},
		ValidateFunc: &map[string]func(any) error{
			"Delete": func(data any) error {
				return fmt.Errorf("delete forbidden")
			},
		},
	})

	err := svc.Delete(context.Background(), "ID", id)
	if err == nil {
		t.Fatal("expected validation error on Delete")
	}
	if len(db.TableRows(testSchema, testTable)) != 1 {
		t.Error("row should not be deleted after validation failure")
	}
}

func TestService_patch_dto_transforms_map(t *testing.T) {
	db := postgressql.NewMemClient()
	id := uuid.New()
	seedRow(db, id, "Item", "I-1", 0, true)

	dtoCalled := false
	svc := postgressql.NewService(postgressql.Parameters{
		DB: db, Schema: testSchema, Table: testTable, Item: testItem{},
		DtoFunc: &map[string]func(any) any{
			"Patch": func(data any) any {
				dtoCalled = true
				return data
			},
		},
	})

	_ = svc.Patch(context.Background(), "ID", id, "Score", 7)
	if !dtoCalled {
		t.Error("Patch DTO was not called")
	}
}

// ---- MemClient helpers ------------------------------------------------------

func TestMemClient_Reset_clears_state(t *testing.T) {
	db := postgressql.NewMemClient()
	seedRow(db, uuid.New(), "A", "A-1", 1, true)
	_ = postgressql.NewService(postgressql.Parameters{
		DB: db, Schema: testSchema, Table: testTable, Item: testItem{},
	})

	db.Reset()

	if len(db.TableRows(testSchema, testTable)) != 0 {
		t.Error("Reset: rows should be cleared")
	}
	if len(db.Calls) != 0 {
		t.Error("Reset: calls should be cleared")
	}
}

func TestMemClient_records_all_methods(t *testing.T) {
	db := postgressql.NewMemClient()
	id := uuid.New()
	seedRow(db, id, "Rec", "R-1", 1, true)
	svc := newService(t, db)
	ctx := context.Background()

	db.Reset()
	seedRow(db, id, "Rec", "R-1", 1, true)

	_, _ = svc.Create(ctx, &testItem{Name: "N", Code: "N-1"})
	_, _ = svc.Get(ctx, "ID", id)
	_ = svc.Exists(ctx, "ID", id)
	_, _ = svc.List(ctx, nil, postgressql.Order{Limit: 10})
	_, _ = svc.Search(ctx, "Rec", postgressql.Order{Limit: 10})

	methods := map[string]bool{}
	for _, c := range db.Calls {
		methods[c.Method] = true
	}
	for _, want := range []string{"QueryRow", "Query"} {
		if !methods[want] {
			t.Errorf("expected method %q to be recorded", want)
		}
	}
}

// ---- helper -----------------------------------------------------------------

func check[T comparable](t *testing.T, field string, want, got T) {
	t.Helper()
	if got != want {
		t.Errorf("%s: got %v, want %v", field, got, want)
	}
}

// TestMain runs the entire test suite 100 times to stress the service for
// performance validation, correctness under load, and to aid optimization cycles.
// Use `go test -run=^TestService_Create_succeeds$` (or similar) + timeout for quick partial runs during dev.
func TestMain(m *testing.M) {
	const repeats = 100
	exitCode := 0
	for i := 0; i < repeats; i++ {
		if code := m.Run(); code != 0 {
			exitCode = code
		}
	}
	os.Exit(exitCode)
}

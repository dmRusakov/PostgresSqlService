package postgressql

import (
	"context"

	"github.com/google/uuid"
)

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

type Service struct {
	model        *Model
	dtoFunc      *map[string]func(any) any
	validateFunc *map[string]func(any) error
}

func NewService(storage *Model, dtoFunc *map[string]func(any) any, validateFunc *map[string]func(any) error) *Service {
	return &Service{
		model:        storage,
		dtoFunc:      dtoFunc,
		validateFunc: validateFunc,
	}
}

func (s *Service) runValidate(funcName string, data any) error {
	if s.validateFunc == nil {
		return nil
	}
	validateFn, ok := (*s.validateFunc)[funcName]
	if !ok {
		return nil
	}
	return validateFn(data)
}

func (s *Service) runDto(funcName string, data any) any {
	if s.dtoFunc == nil {
		return data
	}
	dtoFn, ok := (*s.dtoFunc)[funcName]
	if !ok {
		return data
	}
	return dtoFn(data)
}

func (s *Service) Get(ctx context.Context, field string, value any) (any, error) {
	return s.model.Get(ctx, field, value)
}

func (s *Service) Exists(ctx context.Context, field string, value any) bool {
	return s.model.Exists(ctx, field, value)
}

func (s *Service) List(ctx context.Context, filters map[string][]any, order Order) ([]any, error) {
	return s.model.List(ctx, filters, order)
}

func (s *Service) FieldValues(ctx context.Context, fields string, filters map[string][]any, order Order) ([]any, error) {
	return s.model.FieldValues(ctx, fields, filters, order)
}

func (s *Service) Search(ctx context.Context, query string, order Order) ([]any, error) {
	return s.model.Search(ctx, query, order)
}

func (s *Service) Create(ctx context.Context, entity any) (uuid.UUID, error) {
	entity = s.runDto("Create", entity)
	if err := s.runValidate("Create", entity); err != nil {
		return uuid.Nil, err
	}
	return s.model.Create(ctx, entity)
}

func (s *Service) Update(ctx context.Context, entity any, field string, value any) error {
	entity = s.runDto("Update", entity)
	if err := s.runValidate("Update", entity); err != nil {
		return err
	}
	return s.model.Update(ctx, entity, field, value)
}

func (s *Service) Patch(ctx context.Context, fieldToFind string, valueToFind any, fieldToUpdate string, valueToUpdate any) error {
	validateData := map[string]any{fieldToFind: valueToFind, fieldToUpdate: valueToUpdate}
	validateData = s.runDto("Patch", validateData).(map[string]any)
	if err := s.runValidate("Patch", validateData); err != nil {
		return err
	}
	return s.model.Patch(ctx, fieldToFind, valueToFind, fieldToUpdate, valueToUpdate)
}

func (s *Service) Delete(ctx context.Context, field string, value any) error {
	validateData := map[string]any{field: value}
	validateData = s.runDto("Delete", validateData).(map[string]any)
	if err := s.runValidate("Delete", validateData); err != nil {
		return err
	}
	return s.model.Delete(ctx, field, value)
}

func (s *Service) TableIndex(ctx context.Context) (*string, error) {
	return s.model.TableIndex(ctx)
}

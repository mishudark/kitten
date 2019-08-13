package upperdb

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/fatih/structs"
	validation "github.com/go-ozzo/ozzo-validation"
	"github.com/mishudark/errors"
	db "upper.io/db.v3"
	"upper.io/db.v3/lib/sqlbuilder"
)

// PartialMutation defines a custom
type PartialMutation struct {
	structValue         interface{}
	includeFields       []string
	excludeFields       []string
	includeUpdateFields []string
	excludeUpdateFields []string
	fieldsMap           map[string]string
	table               string
	col                 DbCollection
	sess                sqlbuilder.Database
}

func validatePartialMutation(op *PartialMutation) error {
	return validation.ValidateStruct(op,
		validation.Field(&op.structValue, validation.Required),
		validation.Field(&op.table, validation.Required),
		validation.Field(&op.sess, validation.Required),
		validation.Field(&op.includeFields, validation.By(func(value interface{}) error {
			if len(op.includeFields) == 0 && len(op.excludeFields) == 0 {
				return errors.E(errors.New("PartialMutation, included or excluded fields are required"), errors.Invalid)
			}

			return nil
		})),
	)
}

// Option defines an option that changes the values of PartialMutation struct
type Option func(op *PartialMutation)

// Values set the provided struct as value
func Values(structValue interface{}) Option {
	return func(op *PartialMutation) {
		op.structValue = structValue
	}
}

// Include set the fields to be included in the mutation, this rules has priority over the exluded
// rule
func Include(fields []string) Option {
	return func(op *PartialMutation) {
		op.includeFields = fields
	}
}

// Exclude set the fields to be excluded in the mutation, this is a map of struct field name and
// db field name
func Exclude(fields []string) Option {
	return func(op *PartialMutation) {
		op.excludeFields = fields
	}
}

// IncludeUpdate set the fields to be included in the mutation, this rules has priority over the exluded
// rule
func IncludeUpdate(fields []string) Option {
	return func(op *PartialMutation) {
		op.includeUpdateFields = fields
	}
}

// ExcludeUpdate set the fields to be excluded in the mutation, this is a map of struct field name and
// db field name
func ExcludeUpdate(fields []string) Option {
	return func(op *PartialMutation) {
		op.excludeUpdateFields = fields
	}
}

// Session adds a database session
func Session(sess sqlbuilder.Database) Option {
	return func(op *PartialMutation) {
		op.sess = sess
	}
}

// Table over witch is run the operation
func Table(table string) Option {
	return func(op *PartialMutation) {
		op.table = table
	}
}

// NewPartialMutation returns a PartialMutation and uses a set of options to create it
func NewPartialMutation(opt Option, opts ...Option) (*PartialMutation, error) {
	operation := &PartialMutation{}

	opt(operation)
	for _, o := range opts {
		o(operation)
	}

	if operation.structValue != nil {
		operation.fieldsMap = make(map[string]string)
		t := reflect.TypeOf(operation.structValue)
		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)
			tag := field.Tag.Get("db")
			if tag == "" || tag == "-" {
				continue
			}

			parts := strings.Split(tag, ",")
			operation.fieldsMap[field.Name] = parts[0]
		}
	}

	err := validatePartialMutation(operation)
	if err != nil {
		return nil, err
	}

	operation.col = Ensure(operation.sess, operation.table)
	return operation, nil
}

// InsertModifier can be used to modify columns and values
type InsertModifier func(columns []string, values []interface{})

// Insert the provided values with the included or exluded fields, include rules has preference over
// the excluded rules
func (p *PartialMutation) Insert(structPtr interface{}, whereColumn, whereValue string, mod InsertModifier) error {
	if structPtr == nil || reflect.TypeOf(structPtr).Kind() != reflect.Ptr {
		return fmt.Errorf("expecting a pointer but got %T", structPtr)
	}

	var (
		columns []string
		values  []interface{}
		err     error
	)

	if len(p.includeFields) > 0 {
		columns, values, err = p.getColumnsValuesIncluding(structPtr, p.includeFields)
	} else {
		columns, values, err = p.getColumnsValuesExcluding(structPtr, p.excludeFields)
	}

	if err != nil {
		return err
	}

	if mod != nil {
		mod(columns, values)
	}

	query := p.sess.InsertInto(p.table).Columns(columns...).Values(values...)
	res, err := query.Exec()
	if err != nil {
		return err
	}

	if n, _ := res.RowsAffected(); n == 0 {
		return errors.E(errors.Errorf("operation insert can not be performed, zero rows affected, resource %s", whereValue), errors.NotExist)
	}

	return p.col().Find(whereColumn, whereValue).Limit(1).One(structPtr)
}

// List the elements starting from the given page token, in cae it is empty
// the list will start from zero, using the column name over with it will be ordered
func (p *PartialMutation) List(container interface{}, column, pageToken string, limit int) (nextPageToken string, err error) {
	if limit == 0 || limit < 0 {
		limit = 30
	}

	var query db.Result
	if pageToken == "" {
		query = p.col().Find().OrderBy(column)
	} else {
		query = p.col().Find(fmt.Sprintf("%s >= ?", column), pageToken).OrderBy(column)
	}

	err = query.Limit(limit).All(container)
	if err != nil {
		return "", err
	}

	var row struct {
		ID string `db:"id"`
	}

	query.Offset(limit).Limit(1).One(&row) // nolint: errcheck
	return row.ID, nil
}

// Update the provided values with the included or exluded fields, include rules has preference over
// the excluded rules
func (p *PartialMutation) Update(structPtr interface{}, whereColumn, whereValue string) error {
	if structPtr == nil || reflect.TypeOf(structPtr).Kind() != reflect.Ptr {
		return fmt.Errorf("expecting a pointer but got %T", structPtr)
	}

	var (
		columns []string
		values  []interface{}
		err     error
	)

	includeFields := p.includeFields
	if p.includeUpdateFields != nil {
		includeFields = p.includeUpdateFields
	}

	excludeFields := p.excludeFields
	if p.excludeUpdateFields != nil {
		excludeFields = p.excludeUpdateFields
	}

	if len(includeFields) > 0 {
		columns, values, err = p.getColumnsValuesIncluding(structPtr, includeFields)
	} else {
		columns, values, err = p.getColumnsValuesExcluding(structPtr, excludeFields)
	}

	if err != nil {
		return err
	}

	mapValues := make(map[string]interface{})
	for i := range columns {
		mapValues[columns[i]] = values[i]
	}

	query := p.sess.Update(p.table).Set(mapValues).Where(whereColumn, whereValue)
	res, err := query.Exec()
	if err != nil {
		return err
	}

	if n, _ := res.RowsAffected(); n == 0 {
		return errors.E(errors.Errorf("operation update can not be performed, not exist, resource %s", whereValue), errors.NotExist)
	}

	return p.col().Find(whereColumn, whereValue).Limit(1).One(structPtr)
}

func (p *PartialMutation) getColumnsValuesIncluding(structValue interface{}, fields []string) (columns []string, values []interface{}, err error) {
	mapValues := structs.Map(structValue)

	for _, field := range fields {
		val, ok := mapValues[field]
		if !ok {
			return nil, nil, errors.E(errors.Errorf("getColumnsValuesIncluding operation, invalid field: %s", field), errors.Internal)
		}

		columns = append(columns, p.fieldsMap[field])
		values = append(values, val)
	}

	return columns, values, nil
}

func (p *PartialMutation) getColumnsValuesExcluding(structValue interface{}, fields []string) (columns []string, values []interface{}, err error) {
	mapValues := structs.Map(structValue)

	for _, column := range fields {
		delete(mapValues, column)
	}

	for field, val := range mapValues {
		col, ok := p.fieldsMap[field]
		if !ok {
			continue
		}

		columns = append(columns, col)
		values = append(values, val)
	}

	return columns, values, nil
}

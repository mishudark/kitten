package upperdb

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	db "upper.io/db.v3"
	"upper.io/db.v3/lib/sqlbuilder"
)

type Resource struct {
	Name        string    `db:"name"`
	DisplayName string    `db:"display_name"`
	Quantity    int       `db:"quantity"`
	CreateTime  time.Time `db:"-"`
	UpdateTime  time.Time `db:"-"`
}

func TestColumnValuesIncluded(t *testing.T) {
	mut, err := NewPartialMutation(
		Values(Resource{}),
		Include([]string{
			"Name",
		}),
		IncludeUpdate([]string{}),
		Table("resources"),
		Session(&databaseMock{}),
	)

	if err != nil {
		t.Fatal(err)
	}

	r := Resource{
		Name:        "CAN",
		DisplayName: "Canada",
		Quantity:    3,
	}

	var tests = []struct {
		name            string
		expectedColumns []string
		expectedValues  []interface{}
		given           []string
	}{
		{
			name:            "Name allowed",
			given:           []string{"Name"},
			expectedColumns: []string{"name"},
			expectedValues:  []interface{}{"CAN"},
		},
		{
			name:            "Name and DisplayName allowed",
			given:           []string{"Name", "DisplayName"},
			expectedColumns: []string{"name", "display_name"},
			expectedValues:  []interface{}{"CAN", "Canada"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			columns, values, _ := mut.getColumnsValuesIncluding(r, tt.given)

			if equal := cmp.Equal(tt.expectedColumns, columns); !equal {
				diff := cmp.Diff(tt.expectedColumns, columns)
				t.Errorf("%s: +got, -want, %s", tt.name, diff)
			}

			if equal := cmp.Equal(tt.expectedValues, values); !equal {
				diff := cmp.Diff(tt.expectedValues, values)
				t.Errorf("%s: +got, -want, %s", tt.name, diff)
			}
		})
	}
}

func TestColumnValuesExcluded(t *testing.T) {
	mut, err := NewPartialMutation(
		Values(Resource{}),
		Exclude([]string{
			"Name",
		}),
		Table("resources"),
		Session(&databaseMock{}),
	)

	if err != nil {
		t.Fatal(err)
	}

	r := Resource{
		Name:        "CAN",
		DisplayName: "Canada",
		Quantity:    3,
	}

	var tests = []struct {
		name            string
		expectedColumns []string
		expectedValues  []interface{}
		given           []string
	}{
		{
			name:            "Name and DisplayName excluded",
			given:           []string{"Name", "DisplayName"},
			expectedColumns: []string{"quantity"},
			expectedValues:  []interface{}{3},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			columns, values, _ := mut.getColumnsValuesExcluding(r, tt.given)

			if equal := cmp.Equal(tt.expectedColumns, columns); !equal {
				diff := cmp.Diff(tt.expectedColumns, columns)
				t.Errorf("%s: +got, -want, %s", tt.name, diff)
			}

			if equal := cmp.Equal(tt.expectedValues, values); !equal {
				diff := cmp.Diff(tt.expectedValues, values)
				t.Errorf("%s: +got, -want, %s", tt.name, diff)
			}
		})
	}
}

type databaseMock struct{}

func (d *databaseMock) Driver() interface{} {
	panic("not implemented")
}

func (d *databaseMock) Open(db.ConnectionURL) error {
	panic("not implemented")
}

func (d *databaseMock) Ping() error {
	panic("not implemented")
}

func (d *databaseMock) Close() error {
	panic("not implemented")
}

func (d *databaseMock) Collection(string) db.Collection {
	panic("not implemented")
}

func (d *databaseMock) Collections() ([]string, error) {
	panic("not implemented")
}

func (d *databaseMock) Name() string {
	panic("not implemented")
}

func (d *databaseMock) ConnectionURL() db.ConnectionURL {
	panic("not implemented")
}

func (d *databaseMock) ClearCache() {
	panic("not implemented")
}

func (d *databaseMock) SetLogging(bool) {
	panic("not implemented")
}

func (d *databaseMock) LoggingEnabled() bool {
	panic("not implemented")
}

func (d *databaseMock) SetLogger(db.Logger) {
	panic("not implemented")
}

func (d *databaseMock) Logger() db.Logger {
	panic("not implemented")
}

func (d *databaseMock) SetPreparedStatementCache(bool) {
	panic("not implemented")
}

func (d *databaseMock) PreparedStatementCacheEnabled() bool {
	panic("not implemented")
}

func (d *databaseMock) SetConnMaxLifetime(time.Duration) {
	panic("not implemented")
}

func (d *databaseMock) ConnMaxLifetime() time.Duration {
	panic("not implemented")
}

func (d *databaseMock) SetMaxIdleConns(int) {
	panic("not implemented")
}

func (d *databaseMock) MaxIdleConns() int {
	panic("not implemented")
}

func (d *databaseMock) SetMaxOpenConns(int) {
	panic("not implemented")
}

func (d *databaseMock) MaxOpenConns() int {
	panic("not implemented")
}

func (d *databaseMock) Select(columns ...interface{}) sqlbuilder.Selector {
	panic("not implemented")
}

func (d *databaseMock) SelectFrom(table ...interface{}) sqlbuilder.Selector {
	panic("not implemented")
}

func (d *databaseMock) InsertInto(table string) sqlbuilder.Inserter {
	panic("not implemented")
}

func (d *databaseMock) DeleteFrom(table string) sqlbuilder.Deleter {
	panic("not implemented")
}

func (d *databaseMock) Update(table string) sqlbuilder.Updater {
	panic("not implemented")
}

func (d *databaseMock) Exec(query interface{}, args ...interface{}) (sql.Result, error) {
	panic("not implemented")
}

func (d *databaseMock) ExecContext(ctx context.Context, query interface{}, args ...interface{}) (sql.Result, error) {
	panic("not implemented")
}

func (d *databaseMock) Prepare(query interface{}) (*sql.Stmt, error) {
	panic("not implemented")
}

func (d *databaseMock) PrepareContext(ctx context.Context, query interface{}) (*sql.Stmt, error) {
	panic("not implemented")
}

func (d *databaseMock) Query(query interface{}, args ...interface{}) (*sql.Rows, error) {
	panic("not implemented")
}

func (d *databaseMock) QueryContext(ctx context.Context, query interface{}, args ...interface{}) (*sql.Rows, error) {
	panic("not implemented")
}

func (d *databaseMock) QueryRow(query interface{}, args ...interface{}) (*sql.Row, error) {
	panic("not implemented")
}

func (d *databaseMock) QueryRowContext(ctx context.Context, query interface{}, args ...interface{}) (*sql.Row, error) {
	panic("not implemented")
}

func (d *databaseMock) Iterator(query interface{}, args ...interface{}) sqlbuilder.Iterator {
	panic("not implemented")
}

func (d *databaseMock) IteratorContext(ctx context.Context, query interface{}, args ...interface{}) sqlbuilder.Iterator {
	panic("not implemented")
}

func (d *databaseMock) NewTx(ctx context.Context) (sqlbuilder.Tx, error) {
	panic("not implemented")
}

func (d *databaseMock) Tx(ctx context.Context, fn func(sess sqlbuilder.Tx) error) error {
	panic("not implemented")
}

func (d *databaseMock) Context() context.Context {
	panic("not implemented")
}

func (d *databaseMock) WithContext(context.Context) sqlbuilder.Database {
	panic("not implemented")
}

func (d *databaseMock) SetTxOptions(sql.TxOptions) {
	panic("not implemented")
}

func (d *databaseMock) TxOptions() *sql.TxOptions {
	panic("not implemented")
}

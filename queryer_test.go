package crud

import (
	"context"
	"database/sql"
	"time"

	"github.com/wfunc/crud/testsql"

	_ "github.com/lib/pq"
)

var sharedPG *TestDBQueryer

func getPG() *TestDBQueryer {
	if sharedPG != nil {
		return sharedPG
	}
	db, err := sql.Open("postgres", "postgresql://dev:123@psql.loc:5432/crud?sslmode=disable")
	if err != nil {
		panic(err)
	}
	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)
	sharedPG = NewTestDBQueryer(db)
	_, _, err = sharedPG.Exec(context.Background(), testsql.PG_DROP)
	if err != nil {
		panic(err)
	}
	_, _, err = sharedPG.Exec(context.Background(), testsql.PG_LATEST)
	if err != nil {
		panic(err)
	}
	return sharedPG
}

func clearPG() {
	queryer := getPG()
	queryer.Exec(context.Background(), testsql.PG_CLEAR)
}

// TestDBQueryer is a test wrapper around sql.DB
type TestDBQueryer struct {
	*sql.DB
	ErrNoRows error
}

// NewTestDBQueryer creates a new TestDBQueryer
func NewTestDBQueryer(db *sql.DB) (queryer *TestDBQueryer) {
	queryer = &TestDBQueryer{DB: db, ErrNoRows: ErrNoRows}
	return
}

func (d *TestDBQueryer) getErrNoRows() (err error) {
	if d.ErrNoRows == nil {
		err = ErrNoRows
	} else {
		err = d.ErrNoRows
	}
	return
}

func (d *TestDBQueryer) Exec(ctx context.Context, query string, args ...any) (insertID, affected int64, err error) {
	res, err := d.DB.ExecContext(ctx, query, args...)
	if err == nil {
		insertID, _ = res.LastInsertId() //ignore error for some driver is not supported
	}
	if err == nil {
		affected, err = res.RowsAffected()
	}
	return
}

func (d *TestDBQueryer) ExecRow(ctx context.Context, query string, args ...any) (insertID int64, err error) {
	insertID, affected, err := d.Exec(ctx, query, args...)
	if err == nil && affected < 1 {
		err = d.getErrNoRows()
	}
	return
}

func (d *TestDBQueryer) Query(ctx context.Context, query string, args ...any) (rows Rows, err error) {
	rows, err = d.DB.QueryContext(ctx, query, args...)
	return
}

func (d *TestDBQueryer) QueryRow(ctx context.Context, query string, args ...any) (row Row) {
	row = d.DB.QueryRowContext(ctx, query, args...)
	return
}

type TestCrudQueryer struct {
	Queryer Queryer
}

func (t *TestCrudQueryer) CrudExec(ctx context.Context, query string, args ...any) (insertId, affected int64, err error) {
	insertId, affected, err = t.Queryer.Exec(ctx, query, args...)
	return
}
func (t *TestCrudQueryer) CrudExecRow(ctx context.Context, query string, args ...any) (insertId int64, err error) {
	insertId, err = t.Queryer.ExecRow(ctx, query, args...)
	return
}
func (t *TestCrudQueryer) CrudQuery(ctx context.Context, query string, args ...any) (rows Rows, err error) {
	rows, err = t.Queryer.Query(ctx, query, args...)
	return
}
func (t *TestCrudQueryer) CrudQueryRow(ctx context.Context, query string, args ...any) (row Row) {
	row = t.Queryer.QueryRow(ctx, query, args...)
	return
}

package crud

import (
	"context"
	"database/sql"
)

// ErrNoRows is the error returned when no rows are found in a query.
var ErrNoRows = sql.ErrNoRows

// Scanner is an interface that wraps the Scan method, allowing it to be used with various types.
type Scanner interface {
	Scan(v any)
}

// Rows is an interface that represents a set of rows returned by a query.
type Rows interface {
	Scan(dest ...any) (err error)
	Next() bool
	Close() error
}

// Row is an interface that represents a single row returned by a query.
type Row interface {
	Scan(dest ...any) (err error)
}

// Queryer is an interface that defines methods for executing queries and returning results.
type Queryer interface {
	Exec(ctx context.Context, query string, args ...any) (insertID, affected int64, err error)
	ExecRow(ctx context.Context, query string, args ...any) (insertID int64, err error)
	Query(ctx context.Context, query string, args ...any) (rows Rows, err error)
	QueryRow(ctx context.Context, query string, args ...any) (row Row)
}

// CrudQueryer is an interface that extends Queryer with CRUD operations.
type CrudQueryer interface {
	CrudExec(ctx context.Context, query string, args ...any) (insertID, affected int64, err error)
	CrudExecRow(ctx context.Context, query string, args ...any) (insertID int64, err error)
	CrudQuery(ctx context.Context, query string, args ...any) (rows Rows, err error)
	CrudQueryRow(ctx context.Context, query string, args ...any) (row Row)
}

package crud

import (
	"context"
	"database/sql"
)

var ErrNoRows = sql.ErrNoRows

type Scanner interface {
	Scan(v any)
}

type Rows interface {
	Scan(dest ...any) (err error)
	Next() bool
	Close() error
}

type Row interface {
	Scan(dest ...any) (err error)
}

type Queryer interface {
	Exec(ctx context.Context, query string, args ...any) (insertId, affected int64, err error)
	ExecRow(ctx context.Context, query string, args ...any) (insertId int64, err error)
	Query(ctx context.Context, query string, args ...any) (rows Rows, err error)
	QueryRow(ctx context.Context, query string, args ...any) (row Row)
}

type CrudQueryer interface {
	CrudExec(ctx context.Context, query string, args ...any) (insertId, affected int64, err error)
	CrudExecRow(ctx context.Context, query string, args ...any) (insertId int64, err error)
	CrudQuery(ctx context.Context, query string, args ...any) (rows Rows, err error)
	CrudQueryRow(ctx context.Context, query string, args ...any) (row Row)
}

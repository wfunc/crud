package pgx

import (
	"context"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/wfunc/crud"
)

// Shared is a shared instance of PgQueryer that can be used across the application.
var Shared *PgQueryer

// Pool is a function that returns the shared PgQueryer instance.
var Pool = func() *PgQueryer {
	return Shared
}

// Bootstrap initializes the PgQueryer with a connection string and returns the connection pool.
func Bootstrap(connString string) (pool *pgxpool.Pool, err error) {
	pool, err = pgxpool.Connect(context.Background(), connString)
	if err == nil {
		Shared = NewPgQueryer(pool)
	}
	return
}

// ErrNoRows is returned when a query yields no rows.
var ErrNoRows = pgx.ErrNoRows

// ErrTxClosed is returned when a transaction is closed.
var ErrTxClosed = pgx.ErrTxClosed

// ErrTxCommitRollback is returned when a transaction commit is rolled back.
var ErrTxCommitRollback = pgx.ErrTxCommitRollback

// Row represents a single row result from a query.
type Row struct {
	SQL string
	pgx.Row
}

// Scan scans the row into the provided destination variables.
func (r Row) Scan(dest ...any) (err error) {
	defer func() {
		xerr := r.Row.Scan(dest...)
		if err == nil {
			err = xerr
		}
	}()
	err = mockerCheck("Rows.Scan", r.SQL)
	return
}

// Rows represents a set of rows returned from a query.
type Rows struct {
	SQL string
	pgx.Rows
}

// Scan scans the current row into the provided destination variables.
func (r *Rows) Scan(dest ...any) error {
	if err := mockerCheck("Rows.Scan", r.SQL); err != nil {
		return err
	}
	return r.Rows.Scan(dest...)
}

// Values retrieves the values of the current row as a slice.
func (r *Rows) Values() ([]any, error) {
	if err := mockerCheck("Rows.Values", r.SQL); err != nil {
		return nil, err
	}
	return r.Rows.Values()
}

// Close closes the rows and releases any associated resources.
func (r *Rows) Close() (err error) {
	r.Rows.Close()
	return
}

// BatchResults represents the results of a batch operation.
type BatchResults struct {
	pgx.BatchResults
}

// Exec executes the batch operation and returns the command tag.
func (b *BatchResults) Exec() (pgconn.CommandTag, error) {
	if err := mockerCheck("BatchResult.Exec", ""); err != nil {
		return nil, err
	}
	return b.BatchResults.Exec()
}

// Query executes the batch operation and returns the rows.
func (b *BatchResults) Query() (rows *Rows, err error) {
	if err = mockerCheck("BatchResult.Query", ""); err != nil {
		return nil, err
	}
	raw, err := b.BatchResults.Query()
	if err == nil {
		rows = &Rows{Rows: raw}
	}
	return
}

// QueryRow executes the batch operation and returns a single row.
func (b *BatchResults) QueryRow() *Row {
	return &Row{Row: b.BatchResults.QueryRow()}
}

// Close closes the batch results and releases any associated resources.
func (b *BatchResults) Close() error {
	if err := mockerCheck("BatchResult.Close", ""); err != nil {
		return err
	}
	return b.BatchResults.Close()
}

// Tx represents a transaction in PostgreSQL.
type Tx struct {
	pgx.Tx
}

// Begin starts a pseudo nested transaction.
func (t *Tx) Begin(ctx context.Context) (tx *Tx, err error) {
	if err = mockerCheck("Tx.Begin", ""); err != nil {
		return nil, err
	}
	raw, err := t.Tx.Begin(ctx)
	if err == nil {
		tx = &Tx{Tx: raw}
	}
	return
}

// Commit commits the transaction.
func (t *Tx) Commit(ctx context.Context) error {
	if err := mockerCheck("Tx.Commit", ""); err != nil {
		t.Tx.Rollback(ctx)
		return err
	}
	return t.Tx.Commit(ctx)
}

// Rollback rolls back the transaction.
func (t *Tx) Rollback(ctx context.Context) error {
	if err := mockerCheck("Tx.Rollback", ""); err != nil {
		t.Tx.Rollback(ctx)
		return err
	}
	return t.Tx.Rollback(ctx)
}

// CopyFrom copies data from a source to a table in the database.
func (t *Tx) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	if err := mockerCheck("Tx.CopyFrom", ""); err != nil {
		return 0, err
	}
	return t.Tx.CopyFrom(ctx, tableName, columnNames, rowSrc)
}

// SendBatch sends a batch of commands to the database and returns the results.
func (t *Tx) SendBatch(ctx context.Context, b *pgx.Batch) *BatchResults {
	return &BatchResults{
		BatchResults: t.Tx.SendBatch(ctx, b),
	}
}

// Prepare prepares a SQL statement for execution.
func (t *Tx) Prepare(ctx context.Context, name, sql string) (*pgconn.StatementDescription, error) {
	if err := mockerCheck("Tx.Prepare", sql); err != nil {
		return nil, err
	}
	return t.Tx.Prepare(ctx, name, sql)
}

// Exec executes a SQL command and returns the number of affected rows and the last inserted ID.
func (t *Tx) Exec(ctx context.Context, sql string, args ...any) (insertID, affected int64, err error) {
	if err = mockerCheck("Tx.Exec", sql); err != nil {
		return 0, 0, err
	}
	res, err := t.Tx.Exec(ctx, sql, args...)
	if err == nil {
		affected = res.RowsAffected()
	}
	return
}

// ExecRow executes a SQL command and returns the last inserted ID.
func (t *Tx) ExecRow(ctx context.Context, sql string, args ...any) (insertID int64, err error) {
	if err = mockerCheck("Tx.Exec", sql); err != nil {
		return 0, err
	}
	var affected int64
	insertID, affected, err = t.Exec(ctx, sql, args...)
	if err == nil && affected < 1 {
		err = pgx.ErrNoRows
	}
	return
}

// Query executes a SQL query and returns the rows.
func (t *Tx) Query(ctx context.Context, sql string, args ...any) (rows crud.Rows, err error) {
	if err = mockerCheck("Tx.Query", sql); err != nil {
		return nil, err
	}
	raw, err := t.Tx.Query(ctx, sql, args...)
	if err == nil {
		rows = &Rows{SQL: sql, Rows: raw}
	}
	return
}

// QueryRow executes a SQL query and returns a single row.
func (t *Tx) QueryRow(ctx context.Context, sql string, args ...any) crud.Row {
	return &Row{
		SQL: sql,
		Row: t.Tx.QueryRow(ctx, sql, args...),
	}
}

// CrudExec executes a SQL command and returns the number of affected rows and the last inserted ID.
func (t *Tx) CrudExec(ctx context.Context, sql string, args ...any) (insertID, affected int64, err error) {
	if err = mockerCheck("Tx.Exec", sql); err != nil {
		return 0, 0, err
	}
	insertID, affected, err = t.Exec(ctx, sql, args...)
	return
}

// CrudExecRow executes a SQL command and returns the last inserted ID.
func (t *Tx) CrudExecRow(ctx context.Context, sql string, args ...any) (insertID int64, err error) {
	if err = mockerCheck("Tx.Exec", sql); err != nil {
		return 0, err
	}
	insertID, err = t.ExecRow(ctx, sql, args...)
	return
}

// CrudQuery executes a SQL query and returns the rows.
func (t *Tx) CrudQuery(ctx context.Context, sql string, args ...any) (rows crud.Rows, err error) {
	if err = mockerCheck("Tx.Query", sql); err != nil {
		return nil, err
	}
	rows, err = t.Query(ctx, sql, args...)
	return
}

// CrudQueryRow executes a SQL query and returns a single row.
func (t *Tx) CrudQueryRow(ctx context.Context, sql string, args ...any) (row crud.Row) {
	row = t.QueryRow(ctx, sql, args...)
	return
}

// PgQueryer is a query interface for PostgreSQL using pgx.
type PgQueryer struct {
	*pgxpool.Pool
}

// NewPgQueryer creates a new PgQueryer instance with the provided pgxpool.Pool.
func NewPgQueryer(pool *pgxpool.Pool) (queryer *PgQueryer) {
	queryer = &PgQueryer{Pool: pool}
	return
}

// Exec executes a SQL command and returns the number of affected rows and the last inserted ID.
func (p *PgQueryer) Exec(ctx context.Context, sql string, args ...any) (insertID, affected int64, err error) {
	if err = mockerCheck("Pool.Exec", sql); err != nil {
		return 0, 0, err
	}
	res, err := p.Pool.Exec(ctx, sql, args...)
	if err == nil {
		affected = res.RowsAffected()
	}
	return
}

// ExecRow executes a SQL command and returns the last inserted ID.
func (p *PgQueryer) ExecRow(ctx context.Context, sql string, args ...any) (insertID int64, err error) {
	if err = mockerCheck("Pool.Exec", sql); err != nil {
		return 0, err
	}
	insertID, affected, err := p.Exec(ctx, sql, args...)
	if err == nil && affected < 1 {
		err = pgx.ErrNoRows
	}
	return
}

// Query executes a SQL query and returns the rows.
func (p *PgQueryer) Query(ctx context.Context, sql string, args ...any) (rows crud.Rows, err error) {
	if err = mockerCheck("Pool.Query", sql); err != nil {
		return nil, err
	}
	raw, err := p.Pool.Query(ctx, sql, args...)
	if err == nil {
		rows = &Rows{SQL: sql, Rows: raw}
	}
	return
}

// QueryRow executes a SQL query and returns a single row.
func (p *PgQueryer) QueryRow(ctx context.Context, sql string, args ...any) crud.Row {
	return &Row{
		SQL: sql,
		Row: p.Pool.QueryRow(ctx, sql, args...),
	}
}

// CrudExec executes a SQL command and returns the number of affected rows and the last inserted ID.
func (p *PgQueryer) CrudExec(ctx context.Context, sql string, args ...any) (insertID, affected int64, err error) {
	if err = mockerCheck("Pool.Exec", sql); err != nil {
		return 0, 0, err
	}
	insertID, affected, err = p.Exec(ctx, sql, args...)
	return
}

// CrudExecRow executes a SQL command and returns the last inserted ID.
func (p *PgQueryer) CrudExecRow(ctx context.Context, sql string, args ...any) (insertID int64, err error) {
	if err = mockerCheck("Pool.Exec", sql); err != nil {
		return 0, err
	}
	insertID, err = p.ExecRow(ctx, sql, args...)
	return
}

// CrudQuery executes a SQL query and returns the rows.
func (p *PgQueryer) CrudQuery(ctx context.Context, sql string, args ...any) (rows crud.Rows, err error) {
	if err = mockerCheck("Pool.Query", sql); err != nil {
		return nil, err
	}
	rows, err = p.Query(ctx, sql, args...)
	return
}

// CrudQueryRow executes a SQL query and returns a single row.
func (p *PgQueryer) CrudQueryRow(ctx context.Context, sql string, args ...any) (row crud.Row) {
	row = p.QueryRow(ctx, sql, args...)
	return
}

// CopyFrom copies data from a source to a table in the database.
func (p *PgQueryer) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	if err := mockerCheck("Pool.CopyFrom", ""); err != nil {
		return 0, err
	}
	return p.Pool.CopyFrom(ctx, tableName, columnNames, rowSrc)
}

// SendBatch sends a batch of commands to the database and returns the results.
func (p *PgQueryer) SendBatch(ctx context.Context, b *pgx.Batch) *BatchResults {
	return &BatchResults{
		BatchResults: p.Pool.SendBatch(ctx, b),
	}
}

// Begin starts a pseudo nested transaction.
func (p *PgQueryer) Begin(ctx context.Context) (tx *Tx, err error) {
	if err = mockerCheck("Pool.Begin", ""); err != nil {
		return nil, err
	}
	raw, err := p.Pool.Begin(ctx)
	if err == nil {
		tx = &Tx{Tx: raw}
	}
	return
}

// Exec executes a SQL command and returns the number of affected rows and the last inserted ID.
func Exec(ctx context.Context, sql string, args ...any) (insertID, affected int64, err error) {
	return Shared.Exec(ctx, sql, args...)
}

// ExecRow executes a SQL command and returns the last inserted ID.
func ExecRow(ctx context.Context, sql string, args ...any) (insertID int64, err error) {
	return Shared.ExecRow(ctx, sql, args...)
}

// QueryRow executes a SQL query and returns a single row.
func QueryRow(ctx context.Context, sql string, args ...any) crud.Row {
	return Shared.QueryRow(ctx, sql, args...)
}

// Query executes a SQL query and returns the rows.
func Query(ctx context.Context, sql string, args ...any) (rows crud.Rows, err error) {
	return Shared.Query(ctx, sql, args...)
}

// Begin starts a pseudo nested transaction.
func Begin(ctx context.Context) (tx *Tx, err error) {
	return Shared.Begin(ctx)
}

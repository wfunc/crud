// Package sqlx provides wrappers and helpers for database/sql to simplify CRUD operations.
package sqlx

import (
	"context"
	"database/sql"

	"github.com/wfunc/crud"
)

// Shared is a global variable that holds the shared database queryer instance.
var Shared *DBQueryer

// Pool is a function that returns the shared database queryer instance.
var Pool = func() *DBQueryer {
	return Shared
}

// Bootstrap initializes the Shared variable with a new DBQueryer instance.
func Bootstrap(driverName, dataSourceName string) (db *sql.DB, err error) {
	db, err = sql.Open(driverName, dataSourceName)
	if err == nil {
		Shared = NewDBQueryer(db)
	}
	return
}

// Row represents a single row result from a query.
type Row struct {
	SQL string
	*sql.Row
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

// Rows represents multiple rows result from a query.
type Rows struct {
	SQL string
	*sql.Rows
}

// Scan scans the current row into the provided destination variables.
func (r *Rows) Scan(dest ...any) error {
	if err := mockerCheck("Rows.Scan", r.SQL); err != nil {
		return err
	}
	return r.Rows.Scan(dest...)
}

// Close closes the rows, releasing any resources.
func (r *Rows) Close() (err error) {
	err = r.Rows.Close()
	return
}

// TxQueryer is a wrapper around sql.Tx that implements the Queryer interface.
type TxQueryer struct {
	*sql.Tx
	ErrNoRows error
}

// NewTxQueryer creates a new TxQueryer instance.
func NewTxQueryer(tx *sql.Tx) (queryer *TxQueryer) {
	queryer = &TxQueryer{Tx: tx, ErrNoRows: crud.ErrNoRows}
	return
}

func (t *TxQueryer) getErrNoRows() (err error) {
	if t.ErrNoRows == nil {
		err = crud.ErrNoRows
	} else {
		err = t.ErrNoRows
	}
	return
}

// Commit commits the transaction.
func (t *TxQueryer) Commit() error {
	if err := mockerCheck("Tx.Commit", ""); err != nil {
		t.Tx.Rollback()
		return err
	}
	return t.Tx.Commit()
}

// Rollback rolls back the transaction.
func (t *TxQueryer) Rollback() error {
	if err := mockerCheck("Tx.Rollback", ""); err != nil {
		t.Tx.Rollback()
		return err
	}
	return t.Tx.Rollback()
}

// Exec executes a query that doesn't return rows, such as an INSERT, UPDATE, or DELETE.
func (t *TxQueryer) Exec(ctx context.Context, query string, args ...any) (insertID, affected int64, err error) {
	if err = mockerCheck("Tx.Exec", ""); err != nil {
		return 0, 0, err
	}
	res, err := t.ExecContext(ctx, query, args...)
	if err == nil {
		insertID, _ = res.LastInsertId() //ignore error for some driver is not supported
	}
	if err == nil {
		affected, err = res.RowsAffected()
	}
	return
}

// ExecRow executes a query that doesn't return rows and expects at least one row to be affected.
func (t *TxQueryer) ExecRow(ctx context.Context, query string, args ...any) (insertID int64, err error) {
	if err = mockerCheck("Tx.Exec", ""); err != nil {
		return 0, err
	}
	insertID, affected, err := t.Exec(ctx, query, args...)
	if err == nil && affected < 1 {
		err = t.getErrNoRows()
	}
	return
}

// Query executes a query that returns rows.
func (t *TxQueryer) Query(ctx context.Context, query string, args ...any) (rows crud.Rows, err error) {
	if err = mockerCheck("Tx.Query", ""); err != nil {
		return nil, err
	}
	raw, err := t.QueryContext(ctx, query, args...)
	if err == nil {
		rows = &Rows{Rows: raw, SQL: query}
	}
	return
}

// QueryRow executes a query that returns a single row.
func (t *TxQueryer) QueryRow(ctx context.Context, query string, args ...any) (row crud.Row) {
	raw := t.QueryRowContext(ctx, query, args...)
	row = &Row{Row: raw, SQL: query}
	return
}

// DBQueryer is a wrapper around sql.DB that implements the Queryer interface.
type DBQueryer struct {
	*sql.DB
	ErrNoRows error
}

// NewDBQueryer creates a new DBQueryer instance.
func NewDBQueryer(db *sql.DB) (queryer *DBQueryer) {
	queryer = &DBQueryer{DB: db, ErrNoRows: crud.ErrNoRows}
	return
}

func (d *DBQueryer) getErrNoRows() (err error) {
	if d.ErrNoRows == nil {
		err = crud.ErrNoRows
	} else {
		err = d.ErrNoRows
	}
	return
}

// Begin starts a new transaction.
func (d *DBQueryer) Begin(ctx context.Context) (tx *TxQueryer, err error) {
	if err = mockerCheck("Pool.Begin", ""); err != nil {
		return nil, err
	}
	raw, err := d.BeginTx(ctx, nil)
	if err == nil {
		tx = NewTxQueryer(raw)
		tx.ErrNoRows = d.ErrNoRows
	}
	return
}

// Exec executes a query that doesn't return rows, such as an INSERT, UPDATE, or DELETE.
func (d *DBQueryer) Exec(ctx context.Context, query string, args ...any) (insertID, affected int64, err error) {
	if err = mockerCheck("Pool.Exec", ""); err != nil {
		return 0, 0, err
	}
	res, err := d.ExecContext(ctx, query, args...)
	if err == nil {
		insertID, _ = res.LastInsertId() //ignore error for some driver is not supported
	}
	if err == nil {
		affected, err = res.RowsAffected()
	}
	return
}

// ExecRow executes a query that doesn't return rows and expects at least one row to be affected.
func (d *DBQueryer) ExecRow(ctx context.Context, query string, args ...any) (insertID int64, err error) {
	if err = mockerCheck("Pool.Exec", ""); err != nil {
		return 0, err
	}
	insertID, affected, err := d.Exec(ctx, query, args...)
	if err == nil && affected < 1 {
		err = d.getErrNoRows()
	}
	return
}

// Query executes a query that returns rows.
func (d *DBQueryer) Query(ctx context.Context, query string, args ...any) (rows crud.Rows, err error) {
	if err = mockerCheck("Pool.Query", ""); err != nil {
		return nil, err
	}
	raw, err := d.QueryContext(ctx, query, args...)
	if err == nil {
		rows = &Rows{Rows: raw, SQL: query}
	}
	return
}

// QueryRow executes a query that returns a single row.
func (d *DBQueryer) QueryRow(ctx context.Context, query string, args ...any) (row crud.Row) {
	raw := d.QueryRowContext(ctx, query, args...)
	row = &Row{Row: raw, SQL: query}
	return
}

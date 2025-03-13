package main

import (
	"context"
	"dagger/postgres-agent/internal/dagger"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

type DatabaseWorkspace struct {
	// +internal-use-only
	Conn *dagger.Secret
}

func New(
	// The connection string to the database
	conn *dagger.Secret,
) *DatabaseWorkspace {
	return &DatabaseWorkspace{Conn: conn}
}

// Get the type of a column in a table as a string
func (m *DatabaseWorkspace) ColumnType(ctx context.Context,
	// The table to get the column type from
	table string,
	// The column to get the type of
	column string,
	// The schema of the table (only used for postgres)
	// +default="public"
	schema string,
) (string, error) {
	connString, err := m.Conn.Plaintext(ctx)
	if err != nil {
		return "", err
	}

	connection, err := pgx.Connect(ctx, connString)
	if err != nil {
		return "", err
	}

	var columnType string
	if err := connection.
		QueryRow(
			ctx,
			"SELECT data_type::text FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2 AND column_name = $3", schema, table, column,
		).
		Scan(&columnType); err != nil {
		return "", err
	}

	if columnType == "" {
		return "", fmt.Errorf("column type %s not found in table %s", column, table)
	}

	return columnType, nil
}

// Tables returns a list of tables in the database as a comma-separated string
func (m *DatabaseWorkspace) Tables(ctx context.Context, conn *dagger.Secret, schema string) (string, error) {
	c, err := m.Conn.Plaintext(ctx)
	if err != nil {
		return "", err
	}

	connection, err := pgx.Connect(ctx, c)
	if err != nil {
		return "", err
	}

	// get all the tables in the database schema
	rows, err := connection.Query(ctx, "SELECT table_name FROM information_schema.tables WHERE table_schema = $1", schema)
	if err != nil {
		return "", err
	}

	defer rows.Close()

	var tables []string
	for rows.Next() {
		var table string
		err := rows.Scan(&table)
		if err != nil {
			return "", err
		}

		tables = append(tables, table)
	}

	return strings.Join(tables, ","), nil
}

// TableColumns returns a list of columns in a table as a comma-separated string
func (m *DatabaseWorkspace) TableColumns(ctx context.Context, schema string, name string) (string, error) {
	c, err := m.Conn.Plaintext(ctx)
	if err != nil {
		return "", err
	}

	connection, err := pgx.Connect(ctx, c)
	if err != nil {
		return "", err
	}

	rows, err := connection.Query(
		ctx,
		"SELECT column_name FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2", schema, name,
	)
	if err != nil {
		return "", err
	}

	defer rows.Close()

	var columns []string
	for rows.Next() {
		var column string
		err := rows.Scan(&column)
		if err != nil {
			return "", err
		}

		columns = append(columns, column)
	}

	return strings.Join(columns, ","), nil
}

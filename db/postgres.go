package db

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/lib/pq"
)

type PostgresConn struct {
	dataSourceName string
	conn           *sql.DB
}

//Creates a new postgres connection object
func NewPostgresConn(dataSourceName string) PostgresConn {
	return PostgresConn{
		dataSourceName: dataSourceName,
	}
}

//Connect to default database
func (p PostgresConn) GetConnection() (*sql.DB, error) {
	if p.conn != nil {
		return p.conn, nil
	}

	conn, err := sql.Open(`postgres`, p.dataSourceName)
	p.conn = conn //ide error? cant just do this above
	return p.conn, err
}

func (p PostgresConn) GetSelectSingle(tableName string) (*sql.Rows, error) {
	qry := fmt.Sprintf(`select * from %s limit 1`, tableName)
	conn, err := p.GetConnection()
	if err != nil {
		return nil, err
	}

	return conn.Query(qry)
}

func (p PostgresConn) DoesTableExist(tableName string) (bool, error) {
	conn, err := p.GetConnection()
	if err != nil {
		return false, err
	}

	query := fmt.Sprintf(`select to_regclass('%s')`, tableName)
	row := conn.QueryRow(query)

	var name string
	err = row.Scan(&name)
	if err != nil {
		return false, err
	}

	return name == tableName, nil
}

func (p PostgresConn) CreateTable(tableName string, columns []DBColumnDefinition) error {
	conn, err := p.GetConnection()
	if err != nil {
		return err
	}

	columnItems := []string{}
	for _, col := range columns {
		columnItems = append(columnItems, fmt.Sprintf(`%s %s`, col.ColumnName, col.ColumnType))
	}

	columnQuery := strings.Join(columnItems, `,`)

	query := `create table %s (
			id serial primary key,
			%s
		);`

	query = fmt.Sprintf(query, tableName, columnQuery)

	_, err = conn.Exec(query)
	return err
}

func (p PostgresConn) CreateTableIfNotExists(tableName string, columns []DBColumnDefinition) error {
	if ok, err := p.DoesTableExist(tableName); ok && err == nil {
		return nil
	}
	return p.CreateTable(tableName, columns)
}

func (p PostgresConn) DoesTableColumnExist(tableName string, columnName string) (bool, error) {
	conn, err := p.GetConnection()
	if err != nil {
		return false, err
	}

	query := fmt.Sprintf(`SELECT column_name 
		FROM information_schema.columns 
		WHERE table_name ilike '%s' and column_name ilike '%s'`, tableName, columnName)

	row := conn.QueryRow(query)

	var name string
	err = row.Scan(&name)
	if err != nil {
		return false, err
	}

	return name == columnName, nil
}

func (p PostgresConn) AddTableColumn(tableName string, column DBColumnDefinition) error {
	conn, err := p.GetConnection()
	if err != nil {
		return err
	}

	query := `alter table %s add column %s %s;`

	query = fmt.Sprintf(query, tableName, column.ColumnName, column.ColumnType)

	_, err = conn.Exec(query)
	return err
}

func (p PostgresConn) dbExists(dbName string) (bool, error) {
	conn, err := p.GetConnection()
	if err != nil {
		return false, err
	}

	row := conn.QueryRow(
		`SELECT datname FROM pg_catalog.pg_database WHERE datname = $1;`,
		dbName,
	)

	var name string
	err = row.Scan(&name)
	if err != nil {
		return false, err
	}

	return name == dbName, nil
}

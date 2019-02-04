package db

import (
	"database/sql"
	"fmt"

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
	defer conn.Close()
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
	defer conn.Close()

	query := fmt.Sprintf(`select to_regclass('%s')`, tableName)
	row := conn.QueryRow(query)

	var name string
	err = row.Scan(&name)
	if err != nil {
		return false, err
	}

	return name == tableName, nil
}

func (p PostgresConn) dbExists(dbName string) (bool, error) {
	conn, err := p.GetConnection()
	if err != nil {
		return false, err
	}
	defer conn.Close()

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

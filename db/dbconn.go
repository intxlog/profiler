package db

import "database/sql"

//Struct to house the required methods for use in profiler
type DBConn interface {
	//Returns an active db connection
	GetConnection() (*sql.DB, error)

	//query to return a single row from specifeid table in a sql.Rows object (so we get metadata)
	GetSelectSingle(tableName string) (*sql.Rows, error)

	//Checks if a table exists
	DoesTableExist(tableName string) (bool, error)
}

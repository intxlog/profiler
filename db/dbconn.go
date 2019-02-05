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

	//Creates a table with the specified colums and an "id" column as primary key if the table does not exist
	CreateTable(tableName string, columns []DBColumnDefinition) error

	//Wrapper to check if table exists and if not create table
	CreateTableIfNotExists(tableName string, columns []DBColumnDefinition) error

	//Checks it a table column exists
	DoesTableColumnExist(tableName string, columnName string) (bool, error)

	//Adds a table column to an existing table
	AddTableColumn(tableName string, column DBColumnDefinition) error
}

type DBColumnDefinition struct {
	ColumnName string
	ColumnType string
}

//Converts a [string]string map to an array of db column definitions
func ConvertMapToColumnDefinitions(defs map[string]string) []DBColumnDefinition {
	ret := []DBColumnDefinition{}
	for col, colType := range defs {
		ret = append(ret, DBColumnDefinition{
			ColumnName: col,
			ColumnType: colType,
		})
	}

	return ret
}

package db

import "database/sql"

//Struct to house the required methods for use in profiler
type DBConn interface {
	//Returns an active db connection to the specified database
	GetConnectionToDatabase(dbName string) (*sql.DB, error)

	//Returns an active db connection
	GetConnection() (*sql.DB, error)

	//Creates a database with the specified name if not exists already
	CheckAndCreateDB(dbName string) error

	//returns a sql string to select a single row from a table
	GetQuerySelectSingle(tableName string) string

	//Checks if a table exists
	DoesTableExist(dbName string, tableName string) (bool, error)

	//Returns a string that is the SQL for the primary id columns in a table
	//i.e. SERIAL
	GetIDTypeString() string

	//Returns a string that is the SQL for a date column on a table
	GetDateTypeString() string
}

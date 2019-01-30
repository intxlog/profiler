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
}

package profiler

import "bitbucket.org/intxlog/profiler/db"

type ProfileStore struct {
	dbConn db.DBConn
}

const PROFILE_RECORDS = `profile_records`
const TABLE_NAMES = `table_names`
const TABLE_COLUMN_TYPES = `table_column_types`
const TABLE_PROFILES = `table_profiles`
const TABLE_COLUMN_NAMES = `table_column_names`
const TABLE_COLUMN_PROFILE_PREFIX = `table_column_profiles_`

func NewProfileStore(dbConn db.DBConn) *ProfileStore {
	return &ProfileStore{
		dbConn: dbConn,
	}
}

//Ensures the core profile db data stores are built
func (p *ProfileStore) ScaffoldProfileStore() error {

	//build profile runs table
	columnDefinitions := db.ConvertMapToColumnDefinitions(
		map[string]string{
			"profile_date": "timestamp with timezone",
		},
	)
	err := p.dbConn.CreateTableIfNotExists(PROFILE_RECORDS, columnDefinitions)
	if err != nil {
		return err
	}

	//build tables table

	//build table profiles table

	//build table columns table

	//build table column types table

}

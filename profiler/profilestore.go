package profiler

import "bitbucket.org/intxlog/profiler/db"

type ProfileStore struct {
	dbConn      db.DBConn
	hasScaffold bool
}

const PROFILE_RECORDS = `profile_records`
const TABLE_NAMES = `table_names`
const TABLE_COLUMN_TYPES = `table_column_types`
const TABLE_PROFILES = `table_profiles`
const TABLE_COLUMN_NAMES = `table_column_names`
const TABLE_COLUMN_PROFILE_PREFIX = `table_column_profiles_`

func NewProfileStore(dbConn db.DBConn) *ProfileStore {
	p := &ProfileStore{
		dbConn:      dbConn,
		hasScaffold: false,
	}
	if err := p.ScaffoldProfileStore(); err != nil {
		panic(err)
	}

	return p

}

//Ensures the core profile db data stores are built
func (p *ProfileStore) ScaffoldProfileStore() error {

	//build profile runs table
	err := p.dbConn.CreateTableIfNotExists(PROFILE_RECORDS,
		db.ConvertMapToColumnDefinitions(
			map[string]string{
				"profile_date": "timestamp with time zone",
			},
		),
	)
	if err != nil {
		return err
	}

	//build tables table
	err = p.dbConn.CreateTableIfNotExists(TABLE_NAMES,
		db.ConvertMapToColumnDefinitions(
			map[string]string{
				"table_name": "varchar",
			},
		),
	)
	if err != nil {
		return err
	}

	//build table profiles table
	err = p.dbConn.CreateTableIfNotExists(TABLE_PROFILES,
		db.ConvertMapToColumnDefinitions(
			map[string]string{
				"table_name_id":   "int",
				"table_row_count": "int",
			},
		),
	)
	if err != nil {
		return err
	}

	//build table columns table
	err = p.dbConn.CreateTableIfNotExists(TABLE_COLUMN_NAMES,
		db.ConvertMapToColumnDefinitions(
			map[string]string{
				"table_name_id":        "int",
				"table_column_name":    "varchar",
				"table_column_type_id": "int",
			},
		),
	)
	if err != nil {
		return err
	}

	//build table column types table
	err = p.dbConn.CreateTableIfNotExists(TABLE_COLUMN_TYPES,
		db.ConvertMapToColumnDefinitions(
			map[string]string{
				"table_column_type": "varchar",
			},
		),
	)
	if err != nil {
		return err
	}

	p.hasScaffold = true
	return nil

}

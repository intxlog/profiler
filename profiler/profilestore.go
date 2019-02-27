package profiler

import (
	"fmt"
	"sync"
	"time"

	"bitbucket.org/intxlog/profiler/db"
)

type ProfileStore struct {
	dbConn      db.DBConn
	hasScaffold bool
	mux         sync.Mutex
}

const PROFILE_RECORDS = `profile_records`
const TABLE_NAMES = `table_names`
const TABLE_COLUMN_TYPES = `table_column_types`
const TABLE_PROFILES = `table_profiles`
const TABLE_COLUMN_NAMES = `table_column_names`
const TABLE_COLUMN_PROFILE_PREFIX = `table_column_profiles_`
const TABLE_COLUMN_NAME_ID = `table_column_name_id`
const TABLE_COLUMN_NAME_ID_TYPE = `int`

const PROFILE_RECORD_ID = `profile_record_id`
const PROFILE_RECORD_ID_TYPE = `int`

const TABLE_CUSTOM_COLUMN_NAME_ID = `table_column_name_id`
const TABLE_CUSTOM_COLUMN_NAME_ID_TYPE = `int`
const TABLE_CUSTOM_COLUMN_NAMES = `table_custom_column_names`
const TABLE_CUSTOM_COLUMN_PROFILE_PREFIX = `table_custom_column_profiles_`

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
				"table_name_id":     "int",
				"table_row_count":   "int",
				"profile_record_id": "int",
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

	//build table custom columns table
	err = p.dbConn.CreateTableIfNotExists(TABLE_CUSTOM_COLUMN_NAMES,
		db.ConvertMapToColumnDefinitions(
			map[string]string{
				"table_name_id":                  "int",
				"table_column_name":              "varchar",
				"table_column_type_id":           "int",
				"table_custom_column_definition": "text",
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

//Stores the custom column profile data, scaffolds the custom profile table for the value type if needed
func (p *ProfileStore) StoreCustomColumnProfileData(columnNamesID int, columnType string, profileID int, profileValue interface{}) error {

	profileTable := p.getCustomColumnProfileTableName(columnType)

	columnsMap := map[string]string{
		TABLE_CUSTOM_COLUMN_NAME_ID: TABLE_CUSTOM_COLUMN_NAME_ID_TYPE,
		PROFILE_RECORD_ID:           PROFILE_RECORD_ID_TYPE,
		`value`:                     columnType,
	}

	//error here just means does not exist
	tableExists, _ := p.dbConn.DoesTableExist(profileTable)

	if !tableExists {
		err := p.dbConn.CreateTable(profileTable, db.ConvertMapToColumnDefinitions(columnsMap))
		if err != nil {
			return err
		}
	}

	columnData := map[string]interface{}{
		TABLE_CUSTOM_COLUMN_NAME_ID: columnNamesID,
		PROFILE_RECORD_ID:           profileID,
		`value`:                     profileValue,
	}

	//At this point the table and columns exist, so insert data
	p.dbConn.InsertRowAndReturnID(profileTable, columnData)

	return nil
}

//TODO - make this function not horrible
func (p *ProfileStore) StoreColumnProfileData(columnNamesID int, columnType string, profileID int, profileResults []ColumnProfileData) error {

	profileTable := p.getColumnProfileTableName(columnType)

	columnsMap := map[string]string{
		TABLE_COLUMN_NAME_ID: TABLE_COLUMN_NAME_ID_TYPE,
		PROFILE_RECORD_ID:    PROFILE_RECORD_ID_TYPE,
	}
	for _, data := range profileResults {
		columnsMap[data.name] = data.dataType
	}

	//error here just means does not exist
	tableExists, _ := p.dbConn.DoesTableExist(profileTable)

	if !tableExists {
		err := p.dbConn.CreateTable(profileTable, db.ConvertMapToColumnDefinitions(columnsMap))
		if err != nil {
			return err
		}
	} else {
		//TODO - refactor this into a function call
		//Table exists so just make sure each column exists
		for _, data := range profileResults {
			columnExists, _ := p.dbConn.DoesTableColumnExist(profileTable, data.name)

			//if column does not exist then create it
			if !columnExists {
				err := p.dbConn.AddTableColumn(profileTable, db.DBColumnDefinition{
					ColumnName: data.name,
					ColumnType: data.dataType,
				})
				if err != nil {
					return err
				}
			}
		}
	}

	columnData := map[string]interface{}{
		TABLE_COLUMN_NAME_ID: columnNamesID,
		PROFILE_RECORD_ID:    profileID,
	}
	for _, data := range profileResults {
		columnData[data.name] = data.data
	}

	//At this point the table and columns exist, so insert data
	p.dbConn.InsertRowAndReturnID(profileTable, columnData)

	return nil
}

//Creates a new profile entry and returns the profile id
func (p *ProfileStore) NewProfile() (int, error) {
	return p.getOrInsertTableRowID(PROFILE_RECORDS, map[string]interface{}{
		"profile_date": time.Now(),
	})
}

func (p *ProfileStore) RegisterTableColumn(tableNameID int, columnTypeID int, columnName string) (int, error) {
	return p.getOrInsertTableRowID(TABLE_COLUMN_NAMES, map[string]interface{}{
		"table_name_id":        tableNameID,
		"table_column_name":    columnName,
		"table_column_type_id": columnTypeID,
	})
}

func (p *ProfileStore) RegisterTableCustomColumn(tableNameID int, columnTypeID int, columnName string, columnDefinition string) (int, error) {
	return p.getOrInsertTableRowID(TABLE_CUSTOM_COLUMN_NAMES, map[string]interface{}{
		"table_name_id":                  tableNameID,
		"table_column_name":              columnName,
		"table_column_type_id":           columnTypeID,
		"table_custom_column_definition": columnDefinition,
	})
}

func (p *ProfileStore) RegisterTable(tableName string) (int, error) {
	return p.getOrInsertTableRowID(TABLE_NAMES, map[string]interface{}{
		"table_name": tableName,
	})
}

func (p *ProfileStore) RegisterTableColumnType(columnDataType string) (int, error) {
	p.mux.Lock()
	defer p.mux.Unlock()
	return p.getOrInsertTableRowID(TABLE_COLUMN_TYPES, map[string]interface{}{
		"table_column_type": columnDataType,
	})
}

func (p *ProfileStore) RecordTableProfile(tableNameID int, rowCount int, profileID int) (int, error) {
	return p.getOrInsertTableRowID(TABLE_PROFILES, map[string]interface{}{
		"table_name_id":     tableNameID,
		"table_row_count":   rowCount,
		"profile_record_id": profileID,
	})
}

func (p *ProfileStore) getOrInsertTableRowID(tableName string, values map[string]interface{}) (int, error) {
	rows, err := p.dbConn.GetRowsSelectWhere(tableName, []string{`id`}, values)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var id int
	if rows.Next() {
		err = rows.Scan(&id)
		return id, err
	}

	id = p.dbConn.InsertRowAndReturnID(tableName, values)

	return id, nil
}

func (p *ProfileStore) getColumnProfileTableName(columnDataType string) string {
	return fmt.Sprintf(`%s%s`, TABLE_COLUMN_PROFILE_PREFIX, columnDataType)
}

func (p *ProfileStore) getCustomColumnProfileTableName(columnDataType string) string {
	return fmt.Sprintf(`%s%s`, TABLE_CUSTOM_COLUMN_PROFILE_PREFIX, columnDataType)
}

type ColumnProfileData struct {
	data     interface{}
	name     string
	dataType string
}

package profiler

import (
	"database/sql"
	"reflect"
	"fmt"
	"sync"
	"time"

	"bitbucket.org/intxlog/profiler/db"
)

//ProfileStore is used to store all the profile data that the profiler generates.
//This is a db wrapper essentially
type ProfileStore struct {
	dbConn      db.DBConn
	tablesHaveBeenRenamed bool
	mux         sync.Mutex
}

type ColumnProfileData struct {
	data     interface{}
	name     string
	dataType string
}

func NewProfileStore(dbConn db.DBConn) *ProfileStore {
	p := &ProfileStore{
		dbConn:      dbConn,
		tablesHaveBeenRenamed: false,
	}
	if err := p.ScaffoldProfileStore(); err != nil {
		panic(err)
	}

	return p

}

//Ensures the core profile db data stores are built
func (p *ProfileStore) ScaffoldProfileStore() error {

	
	//build profile runs table
	err := p.createTableForProfileStoreTableStruct(ProfileRecord{})
	if err != nil {
		return err
	}

	//build tables table
	err = p.createTableForProfileStoreTableStruct(TableName{})
	if err != nil {
		return err
	}

	//build table profiles table
	err = p.createTableForProfileStoreTableStruct(TableProfile{})
	if err != nil {
		return err
	}

	//build table columns table
	err = p.createTableForProfileStoreTableStruct(TableColumnName{})
	if err != nil {
		return err
	}

	//build table custom columns table
	err = p.createTableForProfileStoreTableStruct(TableCustomColumnName{})
	if err != nil {
		return err
	}

	//build table column types table
	err = p.createTableForProfileStoreTableStruct(TableColumnType{})
	if err != nil {
		return err
	}

	p.tablesHaveBeenRenamed = true
	return nil

}

//Stores the custom column profile data, scaffolds the custom profile table for the value type if needed
func (p *ProfileStore) StoreCustomColumnProfileData(columnNamesID int, columnType *sql.ColumnType, profileID int, profileValue interface{}) error {

	profileTable := p.getCustomColumnProfileTableName(columnType.DatabaseTypeName())

	//Build the column definitions
	//This is manual due to it being a dynamic table
	columnDefinitions := []db.DBColumnDefinition{}
	columnDefinitions = append(columnDefinitions, db.DBColumnDefinition{
		ColumnName: TABLE_CUSTOM_COLUMN_NAME_ID,
		ColumnType: reflect.TypeOf(0),
	})
	columnDefinitions = append(columnDefinitions, db.DBColumnDefinition{
		ColumnName: PROFILE_RECORD_ID,
		ColumnType: reflect.TypeOf(0),
	})

	columnDefinitions = append(columnDefinitions, db.DBColumnDefinition{
		ColumnName: `value`,
		ColumnType: reflect.TypeOf(profileValue),
	})

	//error here just means does not exist
	tableExists, _ := p.dbConn.DoesTableExist(profileTable)

	if !tableExists {
		err := p.dbConn.CreateTable(profileTable, columnDefinitions)
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

	//Build out the profile table by reflecting on the types we get
	columnDefinitions := []db.DBColumnDefinition{}
	columnDefinitions = append(columnDefinitions, db.DBColumnDefinition{
		ColumnName: TABLE_COLUMN_NAME_ID,
		ColumnType: reflect.TypeOf(0),
	})
	columnDefinitions = append(columnDefinitions, db.DBColumnDefinition{
		ColumnName: PROFILE_RECORD_ID,
		ColumnType: reflect.TypeOf(0),
	})
	for _, data := range profileResults {
		columnDefinitions = append(columnDefinitions, db.DBColumnDefinition{
			ColumnName: data.name,
			ColumnType: reflect.TypeOf(data.data),
		})
	}

	//error here just means does not exist
	tableExists, _ := p.dbConn.DoesTableExist(profileTable)

	if !tableExists {
		err := p.dbConn.CreateTable(profileTable, columnDefinitions)
		if err != nil {
			return err
		}
	} else {
		//Table exists so just make sure each column exists
		for _, data := range profileResults {
			columnExists, _ := p.dbConn.DoesTableColumnExist(profileTable, data.name)

			//if column does not exist then create it
			if !columnExists {
				err := p.dbConn.AddTableColumn(profileTable, db.DBColumnDefinition{
					ColumnName: data.name,
					ColumnType: reflect.TypeOf(data.data),
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

//TODO - this is stupid, redo it to not get everytime...
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

//TODO - check config for snake or pascal
func (p *ProfileStore) getColumnProfileTableName(columnDataType string) string {
	return fmt.Sprintf(`%s%s`, TABLE_COLUMN_PROFILE_PREFIX, columnDataType)
}

//TODO - check config for snake or pascal
func (p *ProfileStore) getCustomColumnProfileTableName(columnDataType string) string {
	return fmt.Sprintf(`%s%s`, TABLE_CUSTOM_COLUMN_PROFILE_PREFIX, columnDataType)
}

//Creates a table for the profile store table struct if not exists
func (p *ProfileStore) createTableForProfileStoreTableStruct(tableStruct interface{}) error {
	tableName, err := getTableNameFromStruct(tableStruct)
	if err != nil{
		return err
	}

	definitions, err := getColumnDataFromStructExcludePrimaryKey(tableStruct)
	if err != nil {
		return err
	}

	return p.dbConn.CreateTableIfNotExists(tableName, definitions)
}

//TODO - look for a config value to set as snake case or pascal case!
//Takes a struct and looks for a table tag on a field
//returns the string of the tag or error if none found
func getTableNameFromStruct(tableStruct interface{}) (string, error){
	fields := reflect.TypeOf(tableStruct)
	for i := 0; i < fields.NumField(); i++ {
		field := fields.Field(i)
		tableName, ok := field.Tag.Lookup(`table`)
		if ok {
			return tableName, nil			
		}
	}
	return ``, fmt.Errorf(`no table tag found on struct %v`, tableStruct)
}

//TODO - look at config for snake or pascal case
//Returns array of db column definitions using the db and primaryKey tags.
//Excludes the primary key from the column definitions
func getColumnDataFromStructExcludePrimaryKey(tableStruct interface{}) ([]db.DBColumnDefinition, error){
	definitions := []db.DBColumnDefinition{}

	fields := reflect.TypeOf(tableStruct)
	for i := 0; i < fields.NumField(); i++ {
		field := fields.Field(i)
		columnName, hasColumnName := field.Tag.Lookup(`db`)
		if hasColumnName {
			primaryKey, hasPrimaryKey := field.Tag.Lookup(`primaryKey`)
			if hasPrimaryKey && primaryKey == `true`{
				continue	//exclude primary key
			} else {
				definitions = append(definitions, db.DBColumnDefinition{
					ColumnName: columnName,
					ColumnType: field.Type,
				})
			}		
		}
	}

	return definitions, nil
}
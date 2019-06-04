package profiler

import (
	"strings"
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
	UsePascalCase bool
	dbConn      db.DBConn
	tablesHaveBeenCreated bool
	mux         sync.Mutex
}

type ColumnProfileData struct {
	data     interface{}
	name     string
	dbDataType string
	scanType reflect.Type
}

func NewProfileStore(dbConn db.DBConn) *ProfileStore {
	p := &ProfileStore{
		UsePascalCase: false,
		dbConn:      dbConn,
		tablesHaveBeenCreated: false,
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

	p.tablesHaveBeenCreated = true
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
		ColumnType: columnType.ScanType(),
	})

	columnDefinitions = p.handleDBColumnDefinitionArrNamingConvention(columnDefinitions)

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

	columnData = p.handleColumnDataNamingConvention(columnData)

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
			ColumnType: data.scanType,
		})
	}

	//Convert casing if we need to
	columnDefinitions = p.handleDBColumnDefinitionArrNamingConvention(columnDefinitions)

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
			columnName := p.handleNamingConvention(data.name)
			columnExists, _ := p.dbConn.DoesTableColumnExist(profileTable, columnName)

			//if column does not exist then create it
			if !columnExists {
				err := p.dbConn.AddTableColumn(profileTable, db.DBColumnDefinition{
					ColumnName: columnName,
					ColumnType: data.scanType,
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

	columnData = p.handleColumnDataNamingConvention(columnData)

	//At this point the table and columns exist, so insert data
	p.dbConn.InsertRowAndReturnID(profileTable, columnData)

	return nil
}

//Creates a new profile entry and returns the profile id
func (p *ProfileStore) NewProfile() (int, error) {
	return p.getOrInsertTableRowIDFromStruct(ProfileRecord{
		ProfileDate: time.Now(),
	})
}

func (p *ProfileStore) RegisterTableColumn(tableNameID int, columnTypeID int, columnName string) (int, error) {
	return p.getOrInsertTableRowIDFromStruct(TableColumnName{
		TableNameID: tableNameID,
		TableColumnName: columnName,
		TableColumnTypeID: columnTypeID,
	})
}

func (p *ProfileStore) RegisterTableCustomColumn(tableNameID int, columnTypeID int, columnName string, columnDefinition string) (int, error) {
	return p.getOrInsertTableRowIDFromStruct(TableCustomColumnName{
		TableNameID: tableNameID,
		TableColumnName: columnName,
		TableColumnTypeID: columnTypeID,
		CustomColumnDefinition: columnDefinition,
	})
}

func (p *ProfileStore) RegisterTable(tableName string) (int, error) {
	return p.getOrInsertTableRowIDFromStruct(TableName{
		TableName: tableName,
	})
}

func (p *ProfileStore) RegisterTableColumnType(columnDataType string) (int, error) {
	p.mux.Lock()
	defer p.mux.Unlock()
	return p.getOrInsertTableRowIDFromStruct(TableColumnType{
		TableColumnType: columnDataType,
	})
}

func (p *ProfileStore) RecordTableProfile(tableNameID int, rowCount int, profileID int) (int, error) {
	return p.getOrInsertTableRowIDFromStruct(TableProfile{
		TableNameID: tableNameID,
		TableRowCount: rowCount,
		ProfileRecordID: profileID,
	})
}

//Converts the struct to the params needed for getOrInsertTableRowID
//uses tag data, excludes primary key field
func (p *ProfileStore) getOrInsertTableRowIDFromStruct(tableStruct interface{}) (int, error) {
	tableName, err := p.getTableNameFromStruct(tableStruct)
	if err != nil{
		return 0, err
	}

	columnDataMap := map[string]interface{}{}

	fieldValues := reflect.ValueOf(tableStruct)	//for value references below
	fields := reflect.TypeOf(tableStruct)
	for i := 0; i < fields.NumField(); i++ {
		field := fields.Field(i)
		columnName, hasColumnName := field.Tag.Lookup(`db`)
		if hasColumnName {
			primaryKey, hasPrimaryKey := field.Tag.Lookup(`primaryKey`)
			if hasPrimaryKey && primaryKey == `true`{
				continue	//exclude primary key
			} else {
				columnDataMap[p.handleNamingConvention(columnName)] = fieldValues.Field(i).Interface()
			}		
		}
	}

	return p.getOrInsertTableRowID(tableName, columnDataMap)
}

func (p *ProfileStore) getOrInsertTableRowID(tableName string, values map[string]interface{}) (int, error) {
	//fix naming conventions
	tableName = p.handleNamingConvention(tableName)
	values = p.handleColumnDataNamingConvention(values)

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
	name := fmt.Sprintf(`%s%s`, TABLE_COLUMN_PROFILE_PREFIX, columnDataType)
	return p.handleNamingConvention(name)
}

func (p *ProfileStore) getCustomColumnProfileTableName(columnDataType string) string {
	name := fmt.Sprintf(`%s%s`, TABLE_CUSTOM_COLUMN_PROFILE_PREFIX, columnDataType)
	return p.handleNamingConvention(name)
}

//Creates a table for the profile store table struct if not exists
func (p *ProfileStore) createTableForProfileStoreTableStruct(tableStruct interface{}) error {
	tableName, err := p.getTableNameFromStruct(tableStruct)
	if err != nil{
		return err
	}

	definitions, err := p.getColumnDataFromStructExcludePrimaryKey(tableStruct)
	if err != nil {
		return err
	}

	return p.dbConn.CreateTableIfNotExists(tableName, definitions)
}

//Takes a struct and looks for a table tag on a field
//returns the string of the tag or error if none found
func (p *ProfileStore) getTableNameFromStruct(tableStruct interface{}) (string, error){
	fields := reflect.TypeOf(tableStruct)
	for i := 0; i < fields.NumField(); i++ {
		field := fields.Field(i)
		tableName, ok := field.Tag.Lookup(`table`)
		if ok {
			return p.handleNamingConvention(tableName), nil			
		}
	}
	return ``, fmt.Errorf(`no table tag found on struct %v`, tableStruct)
}

//Returns array of db column definitions using the db and primaryKey tags.
//Excludes the primary key from the column definitions
func (p *ProfileStore) getColumnDataFromStructExcludePrimaryKey(tableStruct interface{}) ([]db.DBColumnDefinition, error){
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
					ColumnName: p.handleNamingConvention(columnName),
					ColumnType: field.Type,
				})
			}		
		}
	}

	return definitions, nil
}

func (p *ProfileStore) handleColumnDataNamingConvention(data map[string]interface{}) map[string]interface{} {
	fixedData := map[string]interface{}{}
	for key := range data {
		fixedData[p.handleNamingConvention(key)] = data[key]
	}
	return fixedData
}

func (p *ProfileStore) handleDBColumnDefinitionArrNamingConvention(definitions []db.DBColumnDefinition) []db.DBColumnDefinition {
	for idx := range definitions {
		definitions[idx].ColumnName = p.handleNamingConvention(definitions[idx].ColumnName)
	}
	return definitions
}

func (p *ProfileStore) handleNamingConvention(name string) string {
	if p.UsePascalCase {
		return p.convertSnakeCaseToPascalCase(name)
	}
	return name
}

func (p *ProfileStore) convertSnakeCaseToPascalCase(name string) string {
	if !strings.ContainsAny(`_`, name) {
		return name
	}

	name = strings.ReplaceAll(name, `_`, ` `)
	name = strings.Title(name)
	name = strings.ReplaceAll(name, ` `, ``)

	return name
}
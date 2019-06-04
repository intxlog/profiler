package profiler

import (
	"reflect"
	
	"database/sql"
	"fmt"
	"strings"

	"bitbucket.org/intxlog/profiler/db"
)

type Profiler struct {
	targetDBConn  db.DBConn
	profileDBConn db.DBConn
	profileStore  *ProfileStore
	throwNullProfileError bool
}

type ProfilerOptions struct{
	UsePascalCase bool
	ThrowNullProfileError bool //if true then throw an error when a profile returns null, otherwise just skip it
}

// NewProfiler returns a new profiler with default options for the specified databases
// targetDBConn is the database to be profiled
// profileDBConn is the database to store the profile data
func NewProfiler(targetDBConn db.DBConn, profileDBConn db.DBConn) *Profiler {
	return NewProfilerWithOptions(targetDBConn, profileDBConn, ProfilerOptions{})
}

// NewProfilerWithOptions returns a new profiler with the specified options
func NewProfilerWithOptions(targetDBConn db.DBConn, profileDBConn db.DBConn, options ProfilerOptions) *Profiler {
	profiler := &Profiler{
		targetDBConn:  targetDBConn,
		profileDBConn: profileDBConn,
		profileStore:  NewProfileStore(profileDBConn),
	}

	profiler.throwNullProfileError = options.ThrowNullProfileError

	profiler.profileStore.UsePascalCase = options.UsePascalCase

	if err := profiler.profileStore.ScaffoldProfileStore(); err != nil {
		panic(err)
	}

	return profiler
}

//Run profiles on all provided tables and store
func (p *Profiler) ProfileTablesByName(tableNames []string) error {

	profileID, err := p.profileStore.NewProfile()
	if err != nil {
		return err
	}

	errChan := make(chan error)
	defer close(errChan)
	for _, tableName := range tableNames {
		go p.profileTableChannel(tableName, profileID, errChan)
	}

	err = p.waitForTableChannels(errChan, len(tableNames))
	if err != nil {
		return err
	}

	return nil
}

//Run profiles on all provided tables and store
func (p *Profiler) RunProfile(profile ProfileDefinition) error {

	profileID, err := p.profileStore.NewProfile()
	if err != nil {
		return err
	}

	//Profile full tables
	errChan := make(chan error)
	defer close(errChan)

	if len(profile.FullProfileTables) > 0 {
		for _, tableName := range profile.FullProfileTables {
			go p.profileTableChannel(tableName, profileID, errChan)
		}

		err := p.waitForTableChannels(errChan, len(profile.FullProfileTables))
		if err != nil {
			return err
		}
	}

	if len(profile.CustomProfileTables) > 0 {
		//Profile the custom profile definitions
		for _, table := range profile.CustomProfileTables {
			go p.profileTableCustomColumnsChannel(table, profileID, errChan)
		}

		err := p.waitForTableChannels(errChan, len(profile.CustomProfileTables))
		if err != nil {
			return err
		}
	}

	return nil
}

func (p *Profiler) waitForTableChannels(errChan chan error, totalResults int) error {
	tablesProfiled := 0
	for err := range errChan {
		if err != nil {
			return err
		}
		tablesProfiled++
		if tablesProfiled >= totalResults {
			break
		}
	}
	return nil
}

func (p *Profiler) profileTableCustomColumnsChannel(tableDef TableDefinition, profileID int, c chan error) {
	c <- p.profileTableCustomColumns(tableDef, profileID)
}

func (p *Profiler) profileTableChannel(tableName string, profileID int, c chan error) {
	c <- p.profileTable(tableName, profileID)
}

//Profiles the provided table
func (p *Profiler) profileTableCustomColumns(tableDef TableDefinition, profileID int) error {

	tableNameID, err := p.profileStore.RegisterTable(tableDef.TableName)
	if err != nil {
		return err
	}

	selects := []string{}
	for _, col := range tableDef.CustomColumns {
		selects = append(selects, fmt.Sprintf(`%s as %s`, col.ColumnDefinition, col.ColumnName))
	}

	rows, err := p.targetDBConn.GetRowsSelect(tableDef.TableName, selects)
	defer rows.Close()
	if err != nil {
		return err
	}

	columnsData, err := rows.ColumnTypes()
	if err != nil {
		return err
	}

	//Setup profile value pointers so we can scan into the array
	//we make the assumption that results return in the order of selects here
	profileValuePointers := make([]interface{}, len(columnsData))
	for idx := range profileValuePointers {
		profileValuePointers[idx] = new(interface{})
	}

	if rows.Next() {
		rows.Scan(profileValuePointers...)
	} else {
		return fmt.Errorf(`failed to get results from query`)
	}

	//Now that we tricked it to accepting interface pointers, cast back to pointers and get vals
	//assigning to new array just for readability, could go to the same one though
	profileValues := []interface{}{}
	for idx := range profileValuePointers {
		profileValues = append(profileValues, *(profileValuePointers[idx].(*interface{})))
	}

	for idx, columnData := range columnsData {
		columnTypeID, err := p.profileStore.RegisterTableColumnType(columnData.DatabaseTypeName())
		if err != nil {
			return err
		}

		//Find the column definition for this result by iterating our column definitions
		var colDefinition string
		for _, col := range tableDef.CustomColumns {
			if strings.ToLower(col.ColumnName) == columnData.Name() {
				colDefinition = col.ColumnDefinition
				break
			}
		}

		columnNamesID, err := p.profileStore.RegisterTableCustomColumn(tableNameID, columnTypeID, columnData.Name(), colDefinition)
		if err != nil {
			return err
		}
		err = p.profileStore.StoreCustomColumnProfileData(columnNamesID, columnData, profileID, profileValues[idx])
		if err != nil {
			return err
		}
	}

	rows.Close()

	if len(tableDef.Columns) > 0 {
		//profile the defined columns
		err := p.profileTableDefinedColumns(tableDef.TableName, profileID, tableDef.Columns)
		if err != nil {
			return err
		}
	}

	return nil
}

//does a  table profile but only with the specified columns instead of the full thing
func (p *Profiler) profileTableDefinedColumns(tableName string, profileID int, columns []string) error {
	// rows, err := p.targetDBConn.GetSelectAllColumnsSingle(tableName)
	rows, err := p.targetDBConn.GetSelectSingle(tableName, columns)
	if err != nil {
		return err
	}

	columnsData, err := rows.ColumnTypes()
	if err != nil {
		return err
	}

	rows.Close()

	return p.profileTableWithColumnsData(tableName, profileID, columnsData)
}

//Profiles the provided table
func (p *Profiler) profileTable(tableName string, profileID int) error {

	rows, err := p.targetDBConn.GetSelectAllColumnsSingle(tableName)
	if err != nil {
		return err
	}

	columnsData, err := rows.ColumnTypes()
	if err != nil {
		return err
	}

	rows.Close()

	return p.profileTableWithColumnsData(tableName, profileID, columnsData)
}

func (p *Profiler) profileTableWithColumnsData(tableName string, profileID int, columnsData []*sql.ColumnType) error {
	tableNameID, err := p.profileStore.RegisterTable(tableName)
	if err != nil {
		return err
	}

	tableNameObj := TableName{
		ID:        tableNameID,
		TableName: tableName,
	}

	err = p.recordTableRowCount(tableNameObj, profileID)
	if err != nil {
		return err
	}

	return p.handleProfileTableColumns(tableNameObj, profileID, columnsData)
}

func (p *Profiler) recordTableRowCount(tableName TableName, profileID int) error {
	rowCount, err := p.targetDBConn.GetTableRowCount(tableName.TableName)
	if err != nil {
		return err
	}

	_, err = p.profileStore.RecordTableProfile(tableName.ID, rowCount, profileID)

	return err
}

func (p *Profiler) handleProfileTableColumns(tableName TableName, profileID int, columnsData []*sql.ColumnType) error {
	for _, columnData := range columnsData {
		err := p.handleProfileTableColumn(tableName, profileID, columnData)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *Profiler) handleProfileTableColumn(tableName TableName, profileID int, columnData *sql.ColumnType) error {

	columnTypeID, err := p.profileStore.RegisterTableColumnType(columnData.DatabaseTypeName())
	if err != nil {
		return err
	}
	columnNamesID, err := p.profileStore.RegisterTableColumn(tableName.ID, columnTypeID, columnData.Name())
	if err != nil {
		return err
	}

	columnNameEscaped := fmt.Sprintf(`"%s"`, columnData.Name())
	//TODO - make this more generic
	profileSelects := []string{}
	profiles := p.targetDBConn.ProfilesByType(columnData.DatabaseTypeName())
	for col, pro := range profiles {
		profileSelects = append(profileSelects, fmt.Sprintf(`%s as "%s"`, fmt.Sprintf(pro, columnNameEscaped), col))
	}

	rows, err := p.targetDBConn.GetRowsSelect(tableName.TableName, profileSelects)
	if err != nil {
		return err
	}

	profileColumnData, err := rows.ColumnTypes()
	if err != nil {
		return err
	}

	//Setup profile value pointers so we can scan into the array
	profileValues := make([]interface{}, len(profileColumnData))
	profileValuePointers := make([]interface{}, len(profileColumnData))
	for idx := range profileValues {
		profileValuePointers[idx] = &profileValues[idx]
	}

	if rows.Next() {
		rows.Scan(profileValuePointers...)
	}
	rows.Close()

	profileResults := []ColumnProfileData{}
	for idx, val := range profileValues {
		//Skip a profile if there is no value
		if reflect.TypeOf(val) != nil {
			profileResults = append(profileResults, ColumnProfileData{
				data:     val,
				name:     profileColumnData[idx].Name(),
				dataType: profileColumnData[idx].DatabaseTypeName(),
			})
		} else if p.throwNullProfileError {
			return fmt.Errorf(`error: '%v' resulted in a null value and cannot be cast to a type, if you don't want this to throw an error please set the 'ThrowNullProfileError' option to false`,
				profileColumnData[idx].Name(),
			)
		}
	}

	return p.profileStore.StoreColumnProfileData(columnNamesID, columnData.DatabaseTypeName(), profileID, profileResults)
}

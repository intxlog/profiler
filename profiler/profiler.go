package profiler

import (
	"database/sql"
	"fmt"
	"strings"

	"bitbucket.org/intxlog/profiler/db"
)

type Profiler struct {
	targetDBConn  db.DBConn
	profileDBConn db.DBConn
	profileStore  *ProfileStore
}

//Returns a new Profiler
func NewProfiler(targetDBConn db.DBConn, profileDBConn db.DBConn) *Profiler {
	return &Profiler{
		targetDBConn:  targetDBConn,
		profileDBConn: profileDBConn,
		profileStore:  NewProfileStore(profileDBConn),
	}
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

	tablesProfiled := 0
	for err := range errChan {
		if err != nil {
			return err
		}
		tablesProfiled++
		if tablesProfiled >= len(tableNames) {
			break
		}
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

		tablesProfiled := 0
		for err := range errChan {
			if err != nil {
				return err
			}
			tablesProfiled++
			if tablesProfiled >= len(profile.FullProfileTables) {
				break
			}
		}
	}

	if len(profile.CustomProfileTables) > 0 {
		//Profile the custom profile definitions
		for _, table := range profile.CustomProfileTables {
			go p.profileTableDefinitionChannel(table, profileID, errChan)
		}

		//TODO - make this a function so we can reuse it
		tablesProfiled := 0
		for err := range errChan {
			if err != nil {
				return err
			}
			tablesProfiled++
			if tablesProfiled >= len(profile.CustomProfileTables) {
				break
			}
		}
	}

	return nil
}

func (p *Profiler) profileTableDefinitionChannel(tableDef TableDefinition, profileID int, c chan error) {
	c <- p.profileTableDefinition(tableDef, profileID)
}

func (p *Profiler) profileTableChannel(tableName string, profileID int, c chan error) {
	c <- p.profileTable(tableName, profileID)
}

//Profiles the provided table
func (p *Profiler) profileTableDefinition(tableDef TableDefinition, profileID int) error {

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

	//TODO - Group the types by the def and names and send the full thing to profile func
	columnsData, err := rows.ColumnTypes()
	if err != nil {
		return err
	}

	//TODO - make this do a lookup by column name instead
	//Setup profile value pointers so we can scan into the array
	//we make the assumption that results return in the order of selects here
	profileValues := make([]interface{}, len(columnsData))
	profileValuePointers := make([]interface{}, len(columnsData))
	for idx := range profileValues {
		profileValuePointers[idx] = &profileValues[idx]
	}

	if rows.Next() {
		rows.Scan(profileValuePointers...)
	} else {
		return fmt.Errorf(`failed to get results from query`)
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

		err = p.profileStore.StoreCustomColumnProfileData(columnNamesID, columnData.DatabaseTypeName(), profileID, profileValuePointers[idx])
		if err != nil {
			return err
		}
	}

	rows.Close()

	return nil
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

	//TODO - this should happen outside of this context
	tableNameID, err := p.profileStore.RegisterTable(tableName)
	if err != nil {
		return err
	}

	tableNameObj := TableName{
		ID:        tableNameID,
		TableName: tableName,
	}

	err = p.recordTableRowCount(tableNameObj)
	if err != nil {
		return err
	}

	return p.handleProfileTableColumns(tableNameObj, profileID, columnsData)
}

func (p *Profiler) recordTableRowCount(tableName TableName) error {
	rowCount, err := p.targetDBConn.GetTableRowCount(tableName.TableName)
	if err != nil {
		return err
	}

	_, err = p.profileStore.RecordTableProfile(tableName.ID, rowCount)

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
	for idx, _ := range profileValues {
		profileValuePointers[idx] = &profileValues[idx]
	}

	if rows.Next() {
		rows.Scan(profileValuePointers...)
	}
	rows.Close()

	profileResults := []ColumnProfileData{}
	for idx, val := range profileValues {
		profileResults = append(profileResults, ColumnProfileData{
			data:     val,
			name:     profileColumnData[idx].Name(),
			dataType: profileColumnData[idx].DatabaseTypeName(),
		})
	}

	return p.profileStore.StoreColumnProfileData(columnNamesID, columnData.DatabaseTypeName(), profileID, profileResults)
}

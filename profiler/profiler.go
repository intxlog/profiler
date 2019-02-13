package profiler

import (
	"database/sql"
	"fmt"

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
func (p *Profiler) ProfileTables(tableNames []string) error {

	profileID, err := p.profileStore.NewProfile()
	if err != nil {
		return err
	}

	for _, tableName := range tableNames {
		err := p.profileTable(tableName, profileID)
		if err != nil {
			return err
		}
	}
	return nil
}

//Profiles the provided table
func (p *Profiler) profileTable(tableName string, profileID int) error {

	// p.profileStore.RecordTableProfile()

	rows, err := p.targetDBConn.GetSelectSingle(tableName)
	if err != nil {
		return err
	}

	columnsData, err := rows.ColumnTypes()
	if err != nil {
		return err
	}

	rows.Close()

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

	profileSelects := []string{}
	profiles := p.targetDBConn.ProfilesByType(columnData.DatabaseTypeName())
	for col, pro := range profiles {
		profileSelects = append(profileSelects, fmt.Sprintf(`%s as %s`, fmt.Sprintf(pro, columnData.Name()), col))
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

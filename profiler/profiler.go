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
func (p *Profiler) ProfileTables(tableNames []string) error {

	for _, tableName := range tableNames {
		err := p.ProfileTable(tableName)
		if err != nil {
			return err
		}
	}
	return nil
}

//Profiles the provided table
func (p *Profiler) ProfileTable(tableName string) error {

	rows, err := p.targetDBConn.GetSelectSingle(tableName)
	if err != nil {
		return err
	}

	columnsData, err := rows.ColumnTypes()
	if err != nil {
		return err
	}

	return p.handleProfileTableColumns(tableName, columnsData)
}

func (p *Profiler) handleProfileTableColumns(tableName string, columnsData []*sql.ColumnType) error {
	for _, columnData := range columnsData {
		err := p.handleProfileTableColumn(tableName, columnData)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *Profiler) handleProfileTableColumn(tableName string, columnData *sql.ColumnType) error {
	fmt.Printf("Column name %s db column type %s scan type %s\n", columnData.Name(), columnData.DatabaseTypeName(), columnData.ScanType())
	len, ok := columnData.Length()
	if ok {
		fmt.Printf("column length %v\n", len)
	}

	prec, scale, ok := columnData.DecimalSize()
	if ok {
		fmt.Printf("column decimal size %v %v\n", prec, scale)
	}

	profileSelects := []string{}
	profiles := p.targetDBConn.ProfilesByType(columnData.DatabaseTypeName())
	for col, pro := range profiles {
		profileSelects = append(profileSelects, fmt.Sprintf(`%s as %s`, fmt.Sprintf(pro, columnData.Name()), col))
	}

	profileSelectStr := strings.Join(profileSelects, ",")

	query := fmt.Sprintf(`select %s from %s`, profileSelectStr, tableName)

	db, _ := p.targetDBConn.GetConnection()

	rows, err := db.Query(query)
	if err != nil {
		return err
	}

	//TODO - loop the columns we have back, store based on the primary column's data type into that table.
	//Make sure that the columns exist on that data type profile table
	columnsData, err := rows.ColumnTypes()
	if err != nil {
		return err
	}

	return nil
}

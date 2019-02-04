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
	fmt.Printf("Column name %s column type %s\n", columnData.Name(), columnData.DatabaseTypeName())
	len, ok := columnData.Length()
	if ok {
		fmt.Printf("column length %v\n", len)
	}

	prec, scale, ok := columnData.DecimalSize()
	if ok {
		fmt.Printf("column decimal size %v %v\n", prec, scale)
	}

	return nil
}

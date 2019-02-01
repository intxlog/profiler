package profiler

import (
	"database/sql"
	"fmt"

	"bitbucket.org/intxlog/profiler/db"
)

type Profiler struct {
	dbConnData          db.DBConn
	targetDB            string
	profileDB           string
	checkedForProfileDB bool
}

const DEFAULT_PROFILE_NAME = `dbprofiledata`

//Returns a new Profiler
func New(dbConnData db.DBConn, targetDB string) *Profiler {
	return &Profiler{
		dbConnData: dbConnData,
		targetDB:   targetDB,
		profileDB:  DEFAULT_PROFILE_NAME,
	}
}

//Override the profile db (where profile data is stored) if desired
func (p *Profiler) OverrideProfileDB(profileDB string) {
	p.checkedForProfileDB = false //new profile db so reset this flag
	p.profileDB = profileDB
}

func (p *Profiler) ProfileTables(tableNames []string) error {

	//TODO - do we want to do this?
	err := p.tryBuildProfileDB()
	if err != nil {
		return err
	}

	for _, tableName := range tableNames {
		err := p.ProfileTable(tableName)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *Profiler) ProfileTable(tableName string) error {

	//TODO - do we want to do this?
	err := p.tryBuildProfileDB()
	if err != nil {
		return err
	}

	//TODO - limit this correctly to one row for this first query
	//will require a new method in dbconn so it is agnostic to db
	rows, err := p.dbConnData.GetSelectSingle(p.targetDB, tableName)
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

//Tries to build the profile db if not done yet, and returns the error
func (p *Profiler) tryBuildProfileDB() error {
	if p.checkedForProfileDB {
		return nil
	}

	p.checkedForProfileDB = true //mark that we have done this now
	return p.dbConnData.CheckAndCreateDB(p.profileDB)
}

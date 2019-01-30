package profiler

import (
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

	conn, err := p.dbConnData.GetConnectionToDatabase(p.targetDB)
	if err != nil {
		return err
	}
	_, err = conn.Query(`select * from users`)
	return err
}

//Tries to build the profile db if not done yet, and returns the error
func (p *Profiler) tryBuildProfileDB() error {
	if p.checkedForProfileDB {
		return nil
	}

	p.checkedForProfileDB = true //mark that we have done this now
	return p.dbConnData.CheckAndCreateDB(p.profileDB)
}

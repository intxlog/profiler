package profiler

import "bitbucket.org/intxlog/profiler/db"

type ProfileStore struct {
	dbConnData          db.DBConn
	profileDB           string
	checkedForProfileDB bool
}

const DEFAULT_PROFILE_NAME = `dbprofiledata`

func NewProfileStore(dbConnData db.DBConn) *ProfileStore {
	return &ProfileStore{
		dbConnData:          dbConnData,
		profileDB:           DEFAULT_PROFILE_NAME,
		checkedForProfileDB: false,
	}
}

//Override the profile db (where profile data is stored) if desired
func (p *ProfileStore) OverrideProfileDB(profileDB string) {
	p.checkedForProfileDB = false //new profile db so reset this flag
	p.profileDB = profileDB
}

//Tries to build the profile db if not done yet, and returns the error
func (p *ProfileStore) tryBuildProfileDB() error {
	if p.checkedForProfileDB {
		return nil
	}

	p.checkedForProfileDB = true //mark that we have done this now
	return p.dbConnData.CheckAndCreateDB(p.profileDB)
}

package profiler

import "bitbucket.org/intxlog/profiler/db"

type ProfileStore struct {
	dbConn db.DBConn
}

const DEFAULT_PROFILE_NAME = `dbprofiledata`

func NewProfileStore(dbConn db.DBConn) *ProfileStore {
	return &ProfileStore{
		dbConn: dbConn,
	}
}

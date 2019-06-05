package main

import (
	"io/ioutil"
	"flag"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"bitbucket.org/intxlog/profiler/db"
	"bitbucket.org/intxlog/profiler/profiler"
)

func main() {
	run()
}

//Connection type for postgres db
const DB_CONN_POSTGRES = `postgres`

func run() {
	fmt.Printf("Starting profile...\n")
	start := time.Now()

	targetConnDBType := flag.String("targetDBType", DB_CONN_POSTGRES, "Target database type")
	targetConnString := flag.String("targetDB", "", "Target database connection string")

	profileConnDBType := flag.String("profileDBType", DB_CONN_POSTGRES, "Profile database type")
	profileConnString := flag.String("profileDB", "", "Profile store database connection string")

	profileDefinitionPath := flag.String("profileDefinition", "", "Path to profile definition JSON file")

	usePascalCase := flag.Bool("usePascalCase", false, "Use pascal case for table and column naming in profile database")
	
	flag.Parse()

	targetCon, err := getDBConnByType(*targetConnDBType, *targetConnString)
	if err != nil{
		panic(fmt.Errorf(`error getting target database connection: %v`, err))
	}

	profileCon, err := getDBConnByType(*profileConnDBType, *profileConnString)
	if err != nil{
		panic(fmt.Errorf(`error getting profile database connection: %v`, err))
	}

	options := profiler.ProfilerOptions{
		UsePascalCase: *usePascalCase,
	}

	//Read in the profile definition file
	fileData, err := ioutil.ReadFile(*profileDefinitionPath)
	if err != nil{
		panic(err)
	}

	var profile profiler.ProfileDefinition
	err = json.Unmarshal(fileData, &profile)
	if err != nil {
		panic(err)
	}

	p := profiler.NewProfilerWithOptions(targetCon, profileCon, options)

	err = p.RunProfile(profile)

	if err != nil {
		log.Fatal(err)
	} else {
		log.Println("Success")
	}

	end := time.Now()
	fmt.Printf("Finished... time taken: %v\n", end.Sub(start))
}

func getDBConnByType(dbType string, dbConnString string) (db.DBConn, error){
	if dbConnString == "" {
		return nil, fmt.Errorf(`database connection string is required`)
	}
	switch dbType{
	case DB_CONN_POSTGRES:
		return db.NewPostgresConn(dbConnString), nil
	default:
		return nil, fmt.Errorf(`target database connection type not found, looking for %v`, dbType)
	}
}

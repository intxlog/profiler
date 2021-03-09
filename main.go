package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"time"

	"github.com/intxlog/profiler/db"
	"github.com/intxlog/profiler/profiler"
)

func main() {
	run()
}

func run() {
	log.Println("Preparing profiler")
	targetConnDBType := flag.String("targetDBType", db.DB_CONN_POSTGRES, "Target database type")
	targetConnString := flag.String("targetDB", "", "Target database connection string")

	profileConnDBType := flag.String("profileDBType", db.DB_CONN_POSTGRES, "Profile database type")
	profileConnString := flag.String("profileDB", "", "Profile store database connection string")

	profileDefinitionPath := flag.String("profileDefinition", "", "Path to profile definition JSON file")

	usePascalCase := flag.Bool("usePascalCase", false, "Use pascal case for table and column naming in profile database")

	flag.Parse()

	targetCon, err := db.GetDBConnByType(*targetConnDBType, *targetConnString)
	if err != nil {
		log.Fatal(fmt.Errorf(`error getting target database connection: %v`, err))
	}

	profileCon, err := db.GetDBConnByType(*profileConnDBType, *profileConnString)
	if err != nil {
		log.Fatal(fmt.Errorf(`error getting profile database connection: %v`, err))
	}

	options := profiler.ProfilerOptions{
		UsePascalCase: *usePascalCase,
	}

	//Read in the profile definition file
	fileData, err := ioutil.ReadFile(*profileDefinitionPath)
	if err != nil {
		log.Fatal(err)
	}

	var profile profiler.ProfileDefinition
	err = json.Unmarshal(fileData, &profile)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Starting profile...\n")
	start := time.Now()

	p := profiler.NewProfilerWithOptions(targetCon, profileCon, options)

	err = p.RunProfile(profile)

	if err != nil {
		log.Fatal(err)
	} else {
		log.Println("Success")
	}

	end := time.Now()
	log.Printf("Finished... time taken: %v\n", end.Sub(start))
}

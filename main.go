package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"bitbucket.org/intxlog/profiler/db"
	"bitbucket.org/intxlog/profiler/profiler"
)

func main() {
	test()
}

func test() {
	fmt.Printf("Starting profile...\n")
	start := time.Now()
	connStr := os.Args[1]
	t := db.NewPostgresConn(connStr)

	pConnStr := os.Args[2]
	pConn := db.NewPostgresConn(pConnStr)

	p := profiler.NewProfiler(t, pConn)

	profile := profiler.ProfileDefinition{
		FullProfileTables: []string{"loads"},
		CustomProfileTables: []profiler.TableDefinition{
			profiler.TableDefinition{
				TableName: "loads",
				Columns:   []string{"*"},
				CustomColumns: []profiler.CustomColumnDefition{
					profiler.CustomColumnDefition{
						ColumnName:       "tripmilesmin",
						ColumnDefinition: "min(tripmiles)",
					},
				},
			},
		},
	}
	err := p.RunProfile(profile)

	if err != nil {
		log.Fatal(err)
	} else {
		log.Println("Success")
	}

	end := time.Now()
	fmt.Printf("Finished... time taken: %v", end.Sub(start))
}

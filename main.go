package main

import (
	"log"

	"bitbucket.org/intxlog/profiler/db"
	"bitbucket.org/intxlog/profiler/profiler"
)

func main() {
	test()
}

func test() {
	connStr := `user=dev password=dev host=localhost port=5432 dbname=test`
	t := db.NewPostgresConn(connStr)

	pConnStr := `user=dev password=dev host=localhost port=5432 dbname=dbprofiledata`
	pConn := db.NewPostgresConn(pConnStr)

	p := profiler.NewProfiler(t, pConn)
	err := p.ProfileTables([]string{"users"})

	if err != nil {
		log.Fatal(err)
	} else {
		log.Println("Success")
	}
}

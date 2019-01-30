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
	d := db.NewPostgresConn("dev", "dev", "localhost", 5432, "itms")
	p := profiler.New(d, "itms")
	err := p.ProfileTables([]string{"users"})
	if err != nil {
		log.Fatal(err)
	} else {
		log.Println("Success")
	}
}

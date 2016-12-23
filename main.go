package main

import (
	"flag"
	"fmt"
)

var wipe = flag.Bool("wipe", false, "wipe out all stored data and start from scratch")

func main() {
	flag.Parse()

	config := mustReadConfig()
	db := mustSetupMariaDB(config.Mariadb, *wipe)
	scrapes := db.mustLoadScrapes()
	kafka := mustSetupCluster(config.Kafka, scrapes)

	go scrape(kafka, config.Kafka.Topics, db)
	go mustServeBookie(fmt.Sprintf("0.0.0.0:%v", config.ServerPort), db)

	mustServeSnitch(fmt.Sprintf("0.0.0.0:%v", config.SnitchPort))
}

package main

import "fmt"

func main() {
	config := mustReadConfig()
	db := mustSetupMariaDB(config.Mariadb)
	scrapes := db.mustLoadScrapes()
	kafka := mustSetupCluster(config.Kafka, scrapes)

	go scrape(kafka, config.Kafka.Topics, db)
	go mustServeBookie(fmt.Sprintf("0.0.0.0:%v", config.ServerPort), db)

	mustServeSnitch(fmt.Sprintf("0.0.0.0:%v", config.SnitchPort))
}

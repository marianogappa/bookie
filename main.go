package main

import "fmt"

func main() {
	config := mustReadConfig()
	db := mustSetupCassandra(config.Cassandra)
	scrapes := db.mustLoadScrapes()
	kafka := mustSetupCluster(config.Kafka, scrapes)

	go scrape(kafka, config.Kafka.Topics, db)
	// go serve(bookie{db})

	serveSnitch(fmt.Sprintf("0.0.0.0:%v", config.SnitchPort))
}

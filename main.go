package main

import (
	"flag"
	"fmt"

	log "github.com/Sirupsen/logrus"
)

var wipe = flag.Bool("wipe", false, "wipe out all stored data and start from scratch")
var verbose = flag.Bool("v", false, "verbose")
var configFile = flag.String("config", "./config.json", "(required) location of the configuration file")

func main() {
	flag.Parse()

	config := mustReadConfig(*configFile)
	setupLogFormatter(config)
	log.WithField("config", config).Info("Loaded config.")
	db := mustSetupMariaDB(config.Mariadb, *wipe)
	scrapes := db.mustLoadScrapes()
	kafka := mustSetupCluster(config.Kafka, scrapes)

	go scrape(kafka, config.Kafka.Topics, db)
	go mustServeBookie(fmt.Sprintf("0.0.0.0:%v", config.ServerPort), db)

	mustServeSnitch(fmt.Sprintf("0.0.0.0:%v", config.SnitchPort))
}

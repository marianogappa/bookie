package main

import log "github.com/Sirupsen/logrus"

func setupLogFormatter(conf config) {
	switch conf.LogFormat {
	case "json":
		log.SetFormatter(&log.JSONFormatter{TimestampFormat: "2006-01-02T15:04:05.000"})
	default:
		log.SetFormatter(&log.TextFormatter{TimestampFormat: "2006-01-02T15:04:05.000", FullTimestamp: true})
	}
}

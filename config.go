package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
)

type mariadbConfig struct {
	URL string `json:"url"`
}

type kafkaConfig struct {
	Brokers string           `json:"brokers"`
	Topics  map[string]topic `json:"topics"`
}

type config struct {
	SnitchPort int           `json:"snitchPort"`
	ServerPort int           `json:"serverPort"`
	Mariadb    mariadbConfig `json:"mariadb"`
	Kafka      kafkaConfig   `json:"kafka"`
}

type topic struct {
	FSMID      string `json:"fsmID"`
	FSMIDAlias string `json:"fsmIDAlias"`
}

func mustReadConfig() config {
	raw, err := ioutil.ReadFile("./config.json")
	if err != nil {
		log.Fatal(err)
	}

	var c config
	if err := json.Unmarshal(raw, &c); err != nil {
		log.Fatal(err)
	}

	return c
}

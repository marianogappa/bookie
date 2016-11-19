package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
)

type cassandraConfig struct {
	ContactPoint string `json:"contactPoint"`
	NoAuth       bool   `json:"noAuth"`
	User         string `json:"user"`
	Pass         string `json:"pass"`
}

type kafkaConfig struct {
	Brokers string           `json:"brokers"`
	Topics  map[string]topic `json:"topics"`
}

type config struct {
	SnitchPort int             `json:"snitchPort"`
	ServerPort int             `json:"serverPort"`
	Cassandra  cassandraConfig `json:"cassandra"`
	Kafka      kafkaConfig     `json:"kafka"`
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

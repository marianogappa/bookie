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
	Brokers       string                 `json:"brokers"`
	Topics        map[string]topicConfig `json:"topics"`
	BootstrapFrom int64                  `json:"bootstrapFrom"`
}

type config struct {
	SnitchPort int           `json:"snitchPort"`
	ServerPort int           `json:"serverPort"`
	Mariadb    mariadbConfig `json:"mariadb"`
	Kafka      kafkaConfig   `json:"kafka"`
	LogFormat  string        `json:"logFormat"`
}

type topicConfig struct {
	FSMID        string            `json:"fsmID"`
	FSMIDAlias   string            `json:"fsmIDAlias"`
	Tags         map[string]string `json:"tags"`
	Accumulators map[string]string `json:"accumulators"`
	TimeLayout   string            `json:"timeLayout"`
}

func mustReadConfig(configFile string) config {
	raw, err := ioutil.ReadFile(configFile)
	if err != nil {
		log.Fatal(err)
	}

	var c config
	if err := json.Unmarshal(raw, &c); err != nil {
		log.Fatal(err)
	}

	return c
}

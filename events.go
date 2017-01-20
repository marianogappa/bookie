package main

import (
	"bytes"
	"fmt"
	"strconv"
	"text/template"

	log "github.com/Sirupsen/logrus"
)

func processMessage(m message, kt map[string]topicConfig) (string, string, map[string]string, map[string]float64, error) {
	tags := map[string]string{}
	accumulators := map[string]float64{}

	topicDef, ok := kt[m.Topic]
	if !ok {
		log.WithFields(log.Fields{"topic": m.Topic}).Warn("Processed message from unknown topic")
		return "", "", tags, accumulators, nil
	}

	bFSMId, err := parseTempl(topicDef.FSMID, m)
	if err != nil {
		return "", "", tags, accumulators, err
	}
	bFSMIdAlias, err := parseTempl(topicDef.FSMIDAlias, m)
	if err != nil {
		return "", "", tags, accumulators, err
	}

	if len(bFSMId) == 0 && len(bFSMIdAlias) == 0 {
		return "", "", tags, accumulators, fmt.Errorf("Empty fsmID and fsmIDAlias.")
	}

	fsmID := string(bFSMId)
	fsmIDAlias := string(bFSMIdAlias)

	if td := kt[m.Topic].Tags; len(td) > 0 {
		for k, v := range td {
			bV, err := parseTempl(v, m)
			if err == nil && len(bV) > 0 {
				tags[k] = string(bV)
			}
		}
	}

	if td := kt[m.Topic].Accumulators; len(td) > 0 {
		for k, v := range td {
			bV, err := parseTempl(v, m)
			if err == nil && len(bV) > 0 {
				if fv, err := strconv.ParseFloat(string(bV), 64); err == nil {
					accumulators[k] = fv
				}
			}
		}
	}

	return fsmID, fsmIDAlias, tags, accumulators, nil
}

func parseTempl(s string, m message) ([]byte, error) {
	t, err := template.New("").Parse(s)
	if err != nil {
		return []byte{}, err
	}

	var b bytes.Buffer
	if err := t.Option("missingkey=zero").Execute(&b, m); err != nil {
		return []byte{}, err
	}

	byt := b.Bytes()
	if string(byt) == "<no value>" { // missingkey=zero doesn't work with index :(
		return []byte{}, nil
	}

	return byt, nil
}

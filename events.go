package main

import (
	"bytes"
	"fmt"
	"text/template"

	log "github.com/Sirupsen/logrus"
)

func processMessage(m message, kt map[string]topicConfig) (string, string, map[string]string, error) {
	tags := map[string]string{}

	topicDef, ok := kt[m.Topic]
	if !ok {
		log.WithFields(log.Fields{"topic": m.Topic}).Warn("Processed message from unknown topic")
		return "", "", tags, nil
	}

	bFSMId, err := parseTempl(topicDef.FSMID, m)
	if err != nil {
		return "", "", tags, err
	}
	bFSMIdAlias, err := parseTempl(topicDef.FSMIDAlias, m)
	if err != nil {
		return "", "", tags, err
	}

	if len(bFSMId) == 0 && len(bFSMIdAlias) == 0 {
		return "", "", tags, fmt.Errorf("Empty fsmID and fsmIDAlias.")
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

	return fsmID, fsmIDAlias, tags, nil
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

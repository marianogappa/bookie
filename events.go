package main

import (
	"bytes"
	"text/template"

	log "github.com/Sirupsen/logrus"
)

func processMessage(m message, kt map[string]topic, fsmIdAliases map[string]string) (string, error) {
	topicDef, ok := kt[m.Topic]
	if !ok {
		log.WithFields(log.Fields{"topic": m.Topic}).Warn("Processed message from unknown topic")
		return "", nil
	}

	bFSMId, err := parseTempl(topicDef.FSMID, m)
	if err != nil {
		return "", err
	}
	bFSMIdAlias, err := parseTempl(topicDef.FSMIDAlias, m)
	if err != nil {
		return "", err
	}

	if len(bFSMId) == 0 {
		return "", nil
	}

	fsmId := string(bFSMId)
	fsmIdAlias := string(bFSMIdAlias)
	if fa, ok := fsmIdAliases[fsmId]; len(fa) > 0 && ok {
		fsmId = fa
	}
	if _, ok := fsmIdAliases[fsmIdAlias]; !ok && len(fsmIdAlias) > 0 && len(fsmId) > 0 { // if new id/alias pair
		fsmIdAliases[fsmIdAlias] = fsmId // save new alias definition

		// TODO Process incomplete events if any
	}

	return fsmId, nil
}

func parseTempl(s string, m message) ([]byte, error) {
	t, err := template.New("").Parse(s)
	if err != nil {
		return []byte{}, err
	}

	var b bytes.Buffer
	if err := t.Execute(&b, m); err != nil {
		return []byte{}, err
	}

	return b.Bytes(), nil
}

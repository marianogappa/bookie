package main

import (
	"bytes"
	"text/template"

	log "github.com/Sirupsen/logrus"
)

func processMessage(m message, kt map[string]topicConfig, fsmIDAliases map[string]string) (string, error) {
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

	fsmID := string(bFSMId)
	fsmIDAlias := string(bFSMIdAlias)
	if fa, ok := fsmIDAliases[fsmID]; len(fa) > 0 && ok {
		fsmID = fa
	}
	if _, ok := fsmIDAliases[fsmIDAlias]; !ok && len(fsmIDAlias) > 0 && len(fsmID) > 0 { // if new id/alias pair
		fsmIDAliases[fsmIDAlias] = fsmID // save new alias definition

		// TODO Process incomplete events if any
	}

	return fsmID, nil
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

package main

import (
	"bytes"
	"strings"
)

type tags struct {
	t map[string]map[string]string
}

func (t *tags) add(fsmId, fsmAlias, k, v string) {

	if t.t == nil {
		t.t = map[string]map[string]string{}
	}

	key := concatKeys(fsmId, fsmAlias)

	if _, ok := t.t[key]; !ok {
		t.t[key] = map[string]string{k: v}
		return
	}
	t.t[key][k] = v
}

func (t *tags) flush() *query {
	if len(t.t) == 0 {
		return nil
	}

	vs := []interface{}{}
	for key, aux := range t.t {
		for k, v := range aux {
			fsmID, fsmAlias := extractKeys(key)
			vs = append(vs, fsmID, fsmAlias, k, v)
		}
	}

	t.t = nil

	return &query{
		sql: `INSERT INTO bookie.tags(fsmID, fsmAlias, k, v) VALUES ` +
			buildInsertTuples(4, len(vs)/4) +
			` ON DUPLICATE KEY UPDATE v = VALUES(v)`,
		values: vs,
	}
}

func buildInsertTuples(colN, rowN int) string {
	s := bytes.NewBuffer([]byte{})
	for i := 1; i <= rowN; i++ {
		s.Write([]byte("("))
		for j := 1; j <= colN; j++ {
			s.Write([]byte("?"))
			if j != colN {
				s.Write([]byte(","))
			}
		}
		s.Write([]byte(")"))
		if i != rowN {
			s.Write([]byte(","))
		}
	}
	return s.String()
}

func concatKeys(a, b string) string { // TODO JSON Marshal/Unmarshal is safer, but slower. Do we care?
	return a + "@@separator@@" + b
}

func extractKeys(s string) (string, string) {
	ks := strings.Split(s, "@@separator@@")
	return ks[0], ks[1]
}

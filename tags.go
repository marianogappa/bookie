package main

import "bytes"

type tags struct {
	t map[string]map[string]string
}

func (t *tags) add(fsmId string, k string, v string) {
	if t.t == nil {
		t.t = map[string]map[string]string{}
	}

	if _, ok := t.t[fsmId]; !ok {
		t.t[fsmId] = map[string]string{k: v}
		return
	}
	t.t[fsmId][k] = v
}

func (t *tags) flush() (string, []interface{}) {
	vs := []interface{}{}
	for fsmId, aux := range t.t {
		for k, v := range aux {
			vs = append(vs, fsmId, k, v)
		}
	}

	t.t = nil

	return `INSERT INTO bookie.tags(fsmID, k, v) VALUES ` +
		buildInsertTuples(3, len(vs)/3) +
		` ON DUPLICATE KEY UPDATE v = VALUES(v)`, vs
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

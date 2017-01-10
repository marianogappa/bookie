package main

import "time"

type fsms struct {
	t map[string]time.Time
}

func (f *fsms) add(fsmId string, fsmAlias string, created string, layout string) {
	if f.t == nil {
		f.t = map[string]time.Time{}
	}

	key := concatKeys(fsmId, fsmAlias)
	tm := f.parseTime(created, layout)

	if _, ok := f.t[key]; !ok || f.t[key].After(tm) {
		f.t[key] = tm
	}
}

func (f *fsms) flush() []query {
	var qs []query

	if len(f.t) == 0 {
		return qs
	}

	var vs, tmpVs []interface{}
	for key, tm := range f.t {
		fsmId, fsmAlias := extractKeys(key)

		if fsmId == "" {
			tmpVs = append(tmpVs, fsmAlias, tm)
		} else {
			vs = append(vs, fsmId, tm)
		}
	}

	f.t = nil

	if len(tmpVs) > 0 {
		qs = append(qs, query{
			`INSERT INTO bookie.tmpFSM(fsmAlias, created) VALUES ` +
				buildInsertTuples(2, len(tmpVs)/2) +
				` ON DUPLICATE KEY UPDATE created = LEAST(created, VALUES(created))`,
			tmpVs,
		})
	}

	if len(vs) > 0 {
		qs = append(qs, query{
			`INSERT INTO bookie.fsm(fsmID, created) VALUES ` +
				buildInsertTuples(2, len(vs)/2) +
				` ON DUPLICATE KEY UPDATE created = LEAST(created, VALUES(created))`,
			vs,
		})
	}

	return qs
}

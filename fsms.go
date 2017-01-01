package main

import (
	"strconv"
	"time"
)

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

func (f *fsms) flush() *query {
	if len(f.t) == 0 {
		return nil
	}

	vs := make([]interface{}, len(f.t)*3)
	i := 0
	for key, tm := range f.t {
		fsmId, fsmAlias := extractKeys(key)
		vs[i] = fsmId
		i++
		vs[i] = fsmAlias
		i++
		vs[i] = tm
		i++
	}

	f.t = nil

	return &query{
		`INSERT INTO bookie.fsm(fsmID, fsmAlias, created) VALUES ` +
			buildInsertTuples(3, len(vs)/3) +
			` ON DUPLICATE KEY UPDATE created = LEAST(created, VALUES(created))`,
		vs,
	}
}

func (f fsms) parseTime(t string, layout string) time.Time {
	var err error
	var _icreated int64
	var created time.Time

	if layout == "unix" {
		_icreated, err = strconv.ParseInt(t, 10, 64)
		created = time.Unix(_icreated, 0)
	} else if layout == "unixNano" {
		_icreated, err = strconv.ParseInt(t, 10, 64)
		created = time.Unix(0, _icreated)
	} else {
		created, err = time.Parse(layout, t)
	}

	if err != nil {
		return time.Time{}
	}
	return created
}

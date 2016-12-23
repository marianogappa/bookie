package main

import (
	"strconv"
	"time"
)

type fsms struct {
	t map[string]time.Time
}

func (f *fsms) add(fsmId string, created string, layout string) {
	if f.t == nil {
		f.t = map[string]time.Time{}
	}

	tm := f.parseTime(created, layout)

	if _, ok := f.t[fsmId]; !ok || f.t[fsmId].After(tm) {
		f.t[fsmId] = tm
	}
}

func (f *fsms) flush() *query {
	if len(f.t) == 0 {
		return nil
	}

	vs := make([]interface{}, len(f.t)*2)
	i := 0
	for fsmId, tm := range f.t {
		vs[i] = fsmId
		i++
		vs[i] = tm
		i++
	}

	f.t = nil

	return &query{
		`INSERT INTO bookie.fsm(fsmID, created) VALUES ` +
			buildInsertTuples(2, len(vs)/2) +
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

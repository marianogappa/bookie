package main

import (
	"bytes"
	"strconv"
	"strings"
	"time"
)

type fsmDataPoint struct {
	fsmID       string
	topic       string
	partition   int32
	startOffset int64
	lastOffset  int64
	count       int64
	tags        map[string]string

	changed bool
	created time.Time
}

type partition struct {
	Start       int64 `json:"start"`
	End         int64 `json:"end"`
	LastScraped int64 `json:"lastScraped"`
	Count       int64 `json:"count"`
}

type topic struct {
	Partitions map[int32]partition `json:"partitions"`
	Count      int64               `json:"count"`
}

type fsm struct {
	Topics  map[string]topic  `json:"topics,omitempty"`
	Tags    map[string]string `json:"tags"`
	Created time.Time         `json:"created"`
	ID      string            `json:"id"`
}

type fsmSlice []fsm

func (f fsmSlice) Len() int {
	return len(f)
}

func (f fsmSlice) Less(i, j int) bool {
	return f[j].Created.After(f[i].Created)
}

func (f fsmSlice) Swap(i, j int) {
	f[i], f[j] = f[j], f[i]
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

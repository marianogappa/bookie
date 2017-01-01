package main

import "time"

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

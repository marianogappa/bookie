package main

import "time"

type FSMRow struct {
	FSMID       string //TODO include aliases and labels
	Topic       string
	Partition   int32
	StartOffset int64
	LastOffset  int64
	Updated     time.Time
}

type fsmDataPoint struct {
	fsmID       string
	topic       string
	partition   int32
	startOffset int64
	lastOffset  int64

	changed bool
	created time.Time
}

type partition struct {
	Start       int64
	End         int64
	LastScraped int64
	Count       int64
}

type topic struct {
	Partitions map[int32]partition `json:"partitions"`
	Count      int64               `json:"count"`
}

type fsm struct {
	Topics  map[string]topic `json:"topics"`
	Created time.Time        `json:"created"`
	ID      string           `json:"id"`
}

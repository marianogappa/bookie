package main

import "time"

type offsetInfo struct {
	startOffset int64
	lastOffset  int64
	count       int64
	updated     time.Time
}

type offsets struct {
	o map[string]map[string]map[int32]offsetInfo
}

func (o *offsets) add(fsmId string, topic string, topic_partition int32, offset int64) {
	if o.o == nil {
		o.o = map[string]map[string]map[int32]offsetInfo{}
	}

	oi := offsetInfo{
		startOffset: offset,
		lastOffset:  offset,
		count:       1,
		updated:     time.Now().In(time.UTC),
	}

	if _, ok := o.o[fsmId]; !ok {
		o.o[fsmId] = map[string]map[int32]offsetInfo{topic: map[int32]offsetInfo{topic_partition: oi}}
		return
	}

	if _, ok := o.o[fsmId][topic]; !ok {
		o.o[fsmId][topic] = map[int32]offsetInfo{topic_partition: oi}
		return
	}

	if _, ok := o.o[fsmId][topic][topic_partition]; !ok {
		o.o[fsmId][topic][topic_partition] = oi
		return
	}

	o.o[fsmId][topic][topic_partition] = offsetInfo{
		startOffset: o.o[fsmId][topic][topic_partition].startOffset,
		lastOffset:  offset,
		count:       o.o[fsmId][topic][topic_partition].count + 1,
		updated:     oi.updated,
	}
}

func (o *offsets) flush() (string, []interface{}) {
	vs := []interface{}{}
	for fsmId, aux := range o.o {
		for topic, aux2 := range aux {
			for topic_partition, oi := range aux2 {
				vs = append(vs, fsmId, topic, topic_partition, oi.startOffset, oi.lastOffset, oi.count, oi.updated)
			}
		}
	}

	o.o = nil

	return `INSERT INTO bookie.offset(fsmID, topic, topic_partition, startOffset, lastOffset, count, updated) VALUES ` +
		buildInsertTuples(7, len(vs)/7) +
		` ON DUPLICATE KEY UPDATE lastOffset = VALUES(lastOffset), count = count + VALUES(count), updated = UTC_TIMESTAMP)`, vs
}

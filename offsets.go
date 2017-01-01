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

func (o *offsets) add(fsmId, fsmAlias, topic string, topic_partition int32, offset int64) {
	if o.o == nil {
		o.o = map[string]map[string]map[int32]offsetInfo{}
	}

	oi := offsetInfo{
		startOffset: offset,
		lastOffset:  offset,
		count:       1,
		updated:     time.Now().In(time.UTC),
	}

	key := concatKeys(fsmId, fsmAlias)

	if _, ok := o.o[key]; !ok {
		o.o[key] = map[string]map[int32]offsetInfo{topic: map[int32]offsetInfo{topic_partition: oi}}
		return
	}

	if _, ok := o.o[key][topic]; !ok {
		o.o[key][topic] = map[int32]offsetInfo{topic_partition: oi}
		return
	}

	if _, ok := o.o[key][topic][topic_partition]; !ok {
		o.o[key][topic][topic_partition] = oi
		return
	}

	o.o[key][topic][topic_partition] = offsetInfo{
		startOffset: o.o[key][topic][topic_partition].startOffset,
		lastOffset:  offset,
		count:       o.o[key][topic][topic_partition].count + 1,
		updated:     oi.updated,
	}
}

func (o *offsets) flush() *query {
	if len(o.o) == 0 {
		return nil
	}

	vs := []interface{}{}
	for key, aux := range o.o {
		for topic, aux2 := range aux {
			for topic_partition, oi := range aux2 {
				fsmId, fsmAlias := extractKeys(key)
				vs = append(vs, fsmId, fsmAlias, topic, topic_partition, oi.startOffset, oi.lastOffset, oi.count, oi.updated)
			}
		}
	}

	o.o = nil

	return &query{
		sql: `INSERT INTO bookie.offset(fsmID, fsmAlias, topic, topic_partition, startOffset, lastOffset, count, updated) VALUES ` +
			buildInsertTuples(8, len(vs)/8) +
			` ON DUPLICATE KEY UPDATE lastOffset = VALUES(lastOffset), count = count + VALUES(count), updated = UTC_TIMESTAMP`,
		values: vs,
	}
}

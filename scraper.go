package main

import (
	"encoding/json"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"

	"github.com/Shopify/sarama"
)

func scrape(k cluster, kt map[string]topicConfig, db *mariaDB) {
	for _, ch := range k.chs {
		go scrapePartition(ch, kt, db)
	}
}

func scrapePartition(ch <-chan *sarama.ConsumerMessage, kt map[string]topicConfig, db *mariaDB) {
	fsms := fsms{}
	tags := tags{}
	offsets := offsets{}
	aliases := aliases{}
	m := message{}
	var err error

	timeInterval := 5 * time.Second
	offsetInterval := int64(2500)
	lastOffset := int64(0)
	timer := time.NewTimer(timeInterval)

	for {
		select {
		case cm := <-ch:
			m, err = newMessage(*cm)
			if err != nil {
				log.WithFields(log.Fields{"err": err, "message": m}).Warn("Could not unmarshal message.")
				continue
			}

			fsmID, fsmAlias, fsmTags, err := processMessage(m, kt)
			if err != nil {
				log.WithFields(log.Fields{"err": err, "message": m}).Warn("Could not process message.")
				continue
			}

			fsms.add(fsmID, fsmAlias, fsmTags["created"], kt[m.Topic].TimeLayout)
			for k, v := range fsmTags {
				if len(k) > 0 && len(v) > 0 {
					tags.add(fsmID, fsmAlias, k, v)
				}
			}
			offsets.add(fsmID, fsmAlias, m.Topic, m.Partition, m.Offset)
			if len(fsmID) > 0 && len(fsmAlias) > 0 {
				aliases.add(fsmID, fsmAlias)
			}

			if lastOffset == 0 {
				lastOffset = m.Offset
			}

			if m.Offset-lastOffset >= offsetInterval {
				mustFlush(&fsms, &tags, &offsets, &aliases, m, db)
				if !timer.Stop() {
					<-timer.C
				}
				timer.Reset(timeInterval)
				lastOffset = m.Offset
			}
		case <-timer.C:
			if m.Offset > 0 && m.Offset > lastOffset {
				mustFlush(&fsms, &tags, &offsets, &aliases, m, db)
				timer.Reset(timeInterval)
				lastOffset = m.Offset
			}
		}
	}
}

func mustFlush(fsms *fsms, tags *tags, offsets *offsets, aliases *aliases, m message, db *mariaDB) {
	qs := []query{}
	if q := fsms.flush(); q != nil {
		qs = append(qs, *q)
	}
	if q := tags.flush(); q != nil {
		qs = append(qs, *q)
	}
	if q := offsets.flush(); q != nil {
		qs = append(qs, *q)
	}
	if q := aliases.flush(); q != nil {
		uaqs := db.updateAliases()
		qs = append(qs, *q, uaqs[0], uaqs[1], uaqs[2])
	}
	qs = append(qs, db.saveScrape(m.Topic, m.Partition, m.Offset))

	db.mustRunTransaction(qs)

	if *verbose {
		log.WithFields(log.Fields{
			"topic":      m.Topic,
			"partition":  m.Partition,
			"lastOffset": m.Offset,
		}).Info("Persisted batch.")
	}
}

func newMessage(cm sarama.ConsumerMessage) (message, error) {
	d := json.NewDecoder(strings.NewReader(string(cm.Value)))
	d.UseNumber()

	var v interface{}
	if err := d.Decode(&v); err != nil {
		return message{}, err
	}

	return message{
		Key:       string(cm.Key),
		Value:     v.(map[string]interface{}),
		Topic:     cm.Topic,
		Partition: cm.Partition,
		Offset:    cm.Offset,
		Timestamp: cm.Timestamp,
	}, nil
}

type message struct {
	Key       string                 `json:"key"`
	Value     map[string]interface{} `json:"value"`
	Topic     string                 `json:"topic"`
	Partition int32                  `json:"partition"`
	Offset    int64                  `json:"offset"`
	Timestamp time.Time              `json:"timestamp"` // only set if kafka is version 0.10+
}

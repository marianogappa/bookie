package main

import (
	"encoding/json"
	"fmt"
	"math"
	"time"

	log "github.com/Sirupsen/logrus"

	"github.com/Shopify/sarama"
)

func scrape(k cluster, kt map[string]topic, db mariaDB) {
	for _, ch := range k.chs {
		go scrapePartition(ch, kt, db)
	}
}

func scrapePartition(ch <-chan *sarama.ConsumerMessage, kt map[string]topic, db mariaDB) {
	// TODO can't be 100% on memory
	// TODO has to be populated on startup or lazily, but if empty it will calculate incorrect StartOffsets and Counts
	fsms := map[string]*FSMDataPoint{}

	for {
		select {
		case cm := <-ch:
			m, err := newMessage(*cm)
			if err != nil {
				log.WithFields(log.Fields{"err": err, "message": m}).Warn("Could not unmarshal message.")
			}

			fsmIDAliases := map[string]string{} // TODO this has to be global to all scrapers
			fsmID, err := processMessage(m, kt, fsmIDAliases)
			if err != nil {
				log.WithFields(log.Fields{"err": err, "message": m}).Warn("Could not process message.")
			}

			if len(fsmID) > 0 {
				_, ok := fsms[fsmID]
				if !ok {
					fsms[fsmID] = &FSMDataPoint{
						FSMID:       fsmID,
						Partition:   m.Partition,
						Topic:       m.Topic,
						StartOffset: m.Offset, // TODO Counts, global Labels missing here
					}
				}
				fsms[fsmID].LastOffset = m.Offset
				fsms[fsmID].changed = true
			}

			// TODO persisting aliases

			if math.Mod(float64(m.Offset), 1000) == float64(0) { // TODO this is pointless if we don't populate fsms to current snapshot on startup
				for i := range fsms { // TODO this is very inefficient! Several connections seems to make sense here.
					if fsms[i].changed {
						db.saveFSM(*fsms[i])
						fmt.Println("Saved", fsms[i].FSMID)
						fsms[i].changed = false
					}
				}

				db.saveScrape(m.Topic, m.Partition, m.Offset)
			}
		}
	}
}

func newMessage(cm sarama.ConsumerMessage) (message, error) {
	var v interface{}
	if err := json.Unmarshal(cm.Value, &v); err != nil {
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

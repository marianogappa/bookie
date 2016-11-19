package main

import (
	"io/ioutil"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"

	"github.com/gocql/gocql"
)

type cassandra struct {
	session *gocql.Session
}

func mustSetupCassandra(config cassandraConfig) cassandra {
	cluster := gocql.NewCluster(config.ContactPoint)
	cluster.DisableInitialHostLookup = true
	cluster.IgnorePeerAddr = true
	cluster.Consistency = gocql.Quorum
	cluster.Timeout = time.Second * 5
	cluster.ProtoVersion = 4
	if !config.NoAuth {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: "user",
			Password: "pass",
		}
	}

	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("Error while creating session: err=%v", err)
	}

	db := cassandra{session: session}
	db.mustRunQueryFile("cassandra.cql")

	log.WithFields(log.Fields{"config": config}).Info("Successfully started Cassandra session.")
	return db
}

func (db cassandra) mustRunQueryFile(file string) {
	cql, err := ioutil.ReadFile(file)
	if err != nil {
		log.Fatalf("Could not read %s", file)
	}
	db.mustRunQuery(string(cql))
}

func (db cassandra) mustRunQuery(cql string) {
	for _, q := range strings.Split(cql, ";") {
		if strings.TrimSpace(q) != "" {
			err := db.session.Query(q).Exec()
			if err != nil {
				log.WithFields(log.Fields{"query": q, "err": err}).Fatal("Running query failed")
			}
		}
	}
}

func (db cassandra) saveScrape(topic string, partition int32, offset int64) error {
	q := `INSERT INTO bookie.scrape(topic, partition, lastOffset, updated) values (?, ?, ?, toTimestamp(now()))`

	return db.session.Query(q, topic, partition, offset).Exec()
}

type FSMDataPoint struct {
	FSMID       string
	Topic       string
	Partition   int32
	StartOffset int64
	LastOffset  int64

	changed bool
}

func (db cassandra) saveFSM(f FSMDataPoint) error {
	q := `INSERT INTO bookie.fsm(fsmID, topic, partition, startOffset, lastOffset, updated) values (?, ?, ?, ?, ?, toTimestamp(now()))`

	return db.session.Query(q, f.FSMID, f.Topic, f.Partition, f.StartOffset, f.LastOffset).Exec()
}

func (db cassandra) saveAlias(fsmID string, fsmAlias string) error {
	q := `INSERT INTO bookie.fsmAliases(id, alias) (?, ?)`

	return db.session.Query(q, fsmID, fsmAlias).Exec()
}

func (db cassandra) mustLoadScrapes() map[string]topicRecord {
	trs := map[string]topicRecord{}

	q := `SELECT topic, partition, lastOffset FROM bookie.scrape`
	iter := db.session.Query(q).Iter()

	var topic string
	var partition int32
	var lastOffset int64
	for iter.Scan(&topic, &partition, &lastOffset) {
		if _, ok := trs[topic]; !ok {
			trs[topic] = topicRecord{
				topic:      topic,
				partitions: map[int32]int64{},
			}
		}
		trs[topic].partitions[partition] = lastOffset
	}

	if err := iter.Close(); err != nil {
		log.Fatal(err)
	}

	return trs
}

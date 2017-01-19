package main

import (
	"database/sql"
	"sort"
	"strings"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	_ "github.com/go-sql-driver/mysql"
)

var (
	errMySQLDuplicateEntry = uint16(0x426)
)

type mariaDB struct {
	db *sql.DB
	m  sync.Mutex
}

type query struct {
	sql    string
	values []interface{}
}

func mustSetupMariaDB(config mariadbConfig, wipe bool) *mariaDB {
	db, err := setupMariaDB(config, wipe)
	if err != nil {
		log.WithFields(log.Fields{"err": err}).Fatal("Could not setup MariaDB.")
	}
	return db
}

func setupMariaDB(conf mariadbConfig, wipe bool) (*mariaDB, error) {
	db, err := sql.Open("mysql", conf.URL)
	if err != nil {
		return nil, err
	}

	if err = db.Ping(); err != nil {
		return nil, err
	}

	log.Infof("Set up MariaDB connection for %#v.", conf.URL)

	if wipe {
		log.Info("Wipe out all stored data if exists...")
		mustRunQuery(db, "DROP SCHEMA IF EXISTS bookie")
	}

	mustRunMariaDBSQL(db)

	log.Infof("Initialize schema and tables")

	return &mariaDB{db: db}, nil
}

func mustRunMariaDBSQL(db *sql.DB) {
	mustRunQuery(db, string(initSQL))
}

func mustRunQuery(db *sql.DB, sql string) {
	for _, q := range strings.Split(sql, ";") {
		if len(q) > 5 {
			_, err := db.Exec(q)
			if err != nil {
				log.WithFields(log.Fields{"query": q, "err": err}).Fatal("Failed to run sql in mariadb")
			}
		}
	}
}

func (m *mariaDB) mustRunTransaction(qs []query) {
	m.m.Lock()
	defer m.m.Unlock()

	tx, err := m.db.Begin()
	if err != nil {
		log.WithField("err", err).Fatal("Could not begin transaction.")
	}

	for i := range qs {
		if _, err := tx.Exec(qs[i].sql, qs[i].values...); err != nil {
			if err2 := tx.Rollback(); err2 != nil {
				log.WithField("err", err2).Error("Could not rollback transaction!")
			}
			log.WithFields(log.Fields{"query": qs[i].sql, "values": qs[i].values, "err": err}).Fatal("Could not exec query.")
		}
	}

	if err = tx.Commit(); err != nil {
		if err2 := tx.Rollback(); err2 != nil {
			log.WithField("err", err2).Error("Could not rollback transaction!")
		}
		log.WithField("err", err).Fatal("Could not commit transaction.")
	}
}

func (m *mariaDB) saveScrape(topic string, partition int32, offset int64) query {
	return query{
		sql: `INSERT INTO bookie.scrape
					(topic, topic_partition, lastOffset, updated)
				VALUES
					(?, ?, ?, UTC_TIMESTAMP())
				ON DUPLICATE KEY UPDATE
					lastOffset = ?,
					updated = UTC_TIMESTAMP()
				`,
		values: []interface{}{topic, partition, offset + 1, offset + 1},
	}
}

func (m *mariaDB) updateAliases() []query {
	return []query{
		{sql: `INSERT INTO bookie.tags (fsmID, k, v)
						SELECT fsmID, k, v FROM bookie.tmpTags JOIN bookie.fsmAliases USING (fsmAlias)
						ON DUPLICATE KEY UPDATE bookie.tags.fsmID = bookie.tags.fsmID`},
		{sql: `DELETE bookie.tmpTags FROM bookie.tmpTags JOIN bookie.fsmAliases USING (fsmAlias)`},
		{sql: `INSERT INTO bookie.offset (fsmID, topic, topic_partition, startOffset, lastOffset, count, updated)
						SELECT fsmID, topic, topic_partition, startOffset, lastOffset, count, updated FROM bookie.tmpOffset JOIN bookie.fsmAliases USING (fsmAlias)
						ON DUPLICATE KEY UPDATE bookie.offset.fsmID = bookie.offset.fsmID`},
		{sql: `DELETE bookie.tmpOffset FROM bookie.tmpOffset JOIN bookie.fsmAliases USING (fsmAlias)`},
		{sql: `INSERT INTO bookie.fsm (fsmID, created)
						SELECT fsmID, created FROM bookie.tmpFSM JOIN bookie.fsmAliases USING (fsmAlias)
						ON DUPLICATE KEY UPDATE bookie.fsm.fsmID = bookie.fsm.fsmID`},
		{sql: `DELETE bookie.tmpFSM FROM bookie.tmpFSM JOIN bookie.fsmAliases USING (fsmAlias)`},
	}
}

func (m *mariaDB) getLastNFSMs(n int) ([]fsm, error) {
	fsms := []fsm{} // TODO there must be one tag or it fails
	q := `SELECT
			f.fsmID, f.created, IFNULL(t.k, "") k, IFNULL(t.v, "") v
		  FROM
		    (SELECT fsmID, created FROM bookie.fsm ORDER BY created DESC LIMIT ?) f
		  LEFT JOIN
		    bookie.tags t USING(fsmID)
		  `

	rows, err := m.db.Query(q, n)
	if err != nil {
		return fsms, err
	}
	defer rows.Close()

	tm := map[string]map[string]string{}
	fsmMap := map[string]fsm{}
	for rows.Next() {
		var fsmID, _created, k, v string

		if err = rows.Scan(&fsmID, &_created, &k, &v); err != nil {
			log.WithFields(log.Fields{"err": err, "number": n}).Errorf("failed to get last n fsms")
			return fsms, err
		}

		tgs, ok := tm[fsmID]
		if !ok {
			tgs = map[string]string{}
		}
		tgs[k] = v
		tm[fsmID] = tgs

		created, err := time.Parse("2006-01-02 15:04:05", _created)
		if err != nil {
			return fsms, err
		}
		f, ok := fsmMap[fsmID]
		if !ok {
			f = fsm{
				ID:      fsmID,
				Created: created,
			}
		}

		ts := f.Tags
		if ts == nil {
			ts = map[string]string{}
		}
		ts[k] = v
		f.Tags = ts
		fsmMap[fsmID] = f
	}

	for _, v := range fsmMap {
		fsms = append(fsms, v)
	}

	sort.Sort(fsmSlice(fsms))

	return fsms, nil
}

func (m *mariaDB) mustLoadScrapes() map[string]topicRecord {
	trs := map[string]topicRecord{}

	q := `SELECT topic, topic_partition, lastOffset FROM bookie.scrape`
	rows, err := m.db.Query(q)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var topic string
		var partition int32
		var lastOffset int64

		if err = rows.Scan(&topic, &partition, &lastOffset); err != nil {
			log.Fatal(err)
		}
		if _, ok := trs[topic]; !ok {
			trs[topic] = topicRecord{
				topic:      topic,
				partitions: map[int32]int64{},
			}
		}
		trs[topic].partitions[partition] = lastOffset
	}

	return trs
}

func (m *mariaDB) findFSM(fsmID string) (fsm, error) {
	var (
		err error
		fs  = log.Fields{"fsmID": fsmID}
	)
	fsm := fsm{Topics: map[string]topic{}}

	q := `SELECT
					fsmID, o.topic, o.topic_partition, o.startOffset, o.lastOffset, f.created, o.updated, o.count, s.lastOffset
				FROM
					bookie.offset o
				JOIN
					bookie.fsm f USING(fsmID)
				JOIN
					bookie.scrape s USING(topic, topic_partition)
				WHERE
					fsmID = ?`
	rows, err := m.db.Query(q, fsmID)
	if err != nil {
		return fsm, err
	}
	defer rows.Close()

	topicCounts := map[string]int64{}
	for rows.Next() {
		var fsmID, topic string
		var part int32
		var startOffset, lastOffset, count, lastScrapedOffset int64
		var _created, _updated string

		if err = rows.Scan(&fsmID, &topic, &part, &startOffset, &lastOffset, &_created, &_updated, &count, &lastScrapedOffset); err != nil {
			fs["error"] = err
			log.WithFields(fs).Errorf("failed to scan fsm")
			return fsm, err
		}
		created, err := time.Parse("2006-01-02 15:04:05", _created)
		if err != nil {
			return fsm, err
		}

		fsm.Created = created
		fsm.ID = fsmID
		t, ok := fsm.Topics[topic]
		if !ok {
			t.Partitions = make(map[int32]partition)
		}

		t.Partitions[part] = partition{
			Start:       startOffset,
			End:         lastOffset,
			LastScraped: lastScrapedOffset,
			Count:       count,
		}
		fsm.Topics[topic] = t

		if _, ok := topicCounts[topic]; !ok {
			topicCounts[topic] = 0
		}
		topicCounts[topic] += count
	}

	for tn, c := range topicCounts {
		t, _ := fsm.Topics[tn]
		t.Count = c
		fsm.Topics[tn] = t
	}

	q = `SELECT k, v FROM bookie.tags WHERE fsmID = ?`
	rows, err = m.db.Query(q, fsmID)
	if err != nil {
		return fsm, err
	}
	defer rows.Close()

	tags := map[string]string{}
	for rows.Next() {
		var k, v string
		if err = rows.Scan(&k, &v); err != nil {
			fs["error"] = err
			log.WithFields(fs).Errorf("failed to scan tags")
			return fsm, err
		}
		tags[k] = v
	}
	fsm.Tags = tags

	return fsm, nil
}

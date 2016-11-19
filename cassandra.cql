CREATE KEYSPACE IF NOT EXISTS bookie WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 } ;

CREATE TABLE IF NOT EXISTS bookie.scrape (
				   topic varchar,
					 partition int,
					 lastOffset bigint,
					 updated timestamp,
					 PRIMARY KEY (topic, partition));

CREATE TABLE IF NOT EXISTS bookie.fsm (
				      fsmID varchar,
					    topic varchar,
					 partition int,
					 startOffset bigint,
					 lastOffset bigint,
					 updated timestamp,
					 PRIMARY KEY (fsmID, topic, partition));

CREATE TABLE IF NOT EXISTS bookie.fsmAliases (
				       id varchar,
					     alias varchar,
					     PRIMARY KEY (id, alias));

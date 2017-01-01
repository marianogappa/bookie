CREATE SCHEMA IF NOT EXISTS bookie;

CREATE TABLE IF NOT EXISTS bookie.scrape (
    id int(11) NOT NULL AUTO_INCREMENT,
    topic varchar(100) NOT NULL,
    topic_partition int(11) NOT NULL,
    lastOffset bigint (11) NOT NULL,
    updated datetime DEFAULT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uniqueScrape (topic, topic_partition)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS bookie.fsm (
    fsmID varchar(100) NOT NULL,
    fsmAlias varchar(100) NOT NULL,
    created datetime NOT NULL,
    PRIMARY KEY (fsmID, fsmAlias)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS bookie.offset (
    id int(11) NOT NULL AUTO_INCREMENT,
    fsmID varchar(100) NOT NULL,
    fsmAlias varchar(100) NOT NULL,
	topic varchar(100) NOT NULL,
    topic_partition int(11) NOT NULL,
    startOffset bigint(11) NOT NULL,
    lastOffset bigint(11) NOT NULL,
    count int(11) DEFAULT 0,
    updated datetime DEFAULT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uniqueScrapedFSM (fsmID, fsmAlias, topic, topic_partition)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS bookie.fsmAliases (
    fsmID varchar(100) NOT NULL,
    fsmAlias varchar(100) NOT NULL,
    PRIMARY KEY (fsmID, fsmAlias)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS bookie.tags (
    fsmID varchar(100) NOT NULL,
    fsmAlias varchar(100) NOT NULL,
    k varchar(100) NOT NULL,
	v text NOT NULL,
	PRIMARY KEY (fsmID, fsmAlias, k)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

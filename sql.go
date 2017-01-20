package main

var initSQL = `
CREATE SCHEMA IF NOT EXISTS bookie;

CREATE TABLE IF NOT EXISTS bookie.scrape (
    id int(11) NOT NULL AUTO_INCREMENT,
    topic varchar(100) NOT NULL,
    topic_partition int(11) NOT NULL,
    lastOffset bigint (11) NOT NULL,
    updated datetime DEFAULT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uniq (topic, topic_partition)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS bookie.fsm (
    id int(11) NOT NULL AUTO_INCREMENT,
    fsmID varchar(100) NOT NULL,
    created datetime NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uniq (fsmID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS bookie.tmpFSM (
    id int(11) NOT NULL AUTO_INCREMENT,
    fsmAlias varchar(100) NOT NULL,
    created datetime NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uniq (fsmAlias)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS bookie.offset (
    id int(11) NOT NULL AUTO_INCREMENT,
    fsmID varchar(100) NOT NULL,
    topic varchar(100) NOT NULL,
    topic_partition int(11) NOT NULL,
    startOffset bigint(11) NOT NULL,
    lastOffset bigint(11) NOT NULL,
    count int(11) DEFAULT 0,
    updated datetime DEFAULT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uniq (fsmID, topic, topic_partition)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS bookie.tmpOffset (
    id int(11) NOT NULL AUTO_INCREMENT,
    fsmAlias varchar(100) NOT NULL,
    topic varchar(100) NOT NULL,
    topic_partition int(11) NOT NULL,
    startOffset bigint(11) NOT NULL,
    lastOffset bigint(11) NOT NULL,
    count int(11) DEFAULT 0,
    updated datetime DEFAULT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uniq (fsmAlias, topic, topic_partition)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS bookie.fsmAliases (
    id int(11) NOT NULL AUTO_INCREMENT,
    fsmID varchar(100) NOT NULL,
    fsmAlias varchar(100) NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uniq (fsmID, fsmAlias)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS bookie.tags (
    id int(11) NOT NULL AUTO_INCREMENT,
    fsmID varchar(100) NOT NULL,
    k varchar(100) NOT NULL,
    v text NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uniq (fsmID, k)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS bookie.tmpTags (
    id int(11) NOT NULL AUTO_INCREMENT,
    fsmAlias varchar(100) NOT NULL,
    k varchar(100) NOT NULL,
    v text NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uniq (fsmAlias, k)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS bookie.accumulators (
    id int(11) NOT NULL AUTO_INCREMENT,
    fsmID varchar(100) NOT NULL,
    k varchar(100) NOT NULL,
    v float NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uniq (fsmID, k)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS bookie.tmpAccumulators (
    id int(11) NOT NULL AUTO_INCREMENT,
    fsmAlias varchar(100) NOT NULL,
    k varchar(100) NOT NULL,
    v float NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uniq (fsmAlias, k)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
`

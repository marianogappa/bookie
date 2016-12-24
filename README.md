# bookie
Indexing for Kafka queues.

[Movio Hackathon video explaining Bookie](https://www.youtube.com/watch?v=VVOk96cZW20)

## Installation

```
go get -u github.com/MarianoGappa/bookie
```
or get the [latest release binary](https://github.com/MarianoGappa/bookie/releases) for your OS

## Configuration

```
{
  "mariadb": {
    "url": "root@tcp(mc-red-test-2.movio-dev.co:3306)/vc_test"
  },
  "kafka": {
    "brokers": "localhost:9092",
    "topics": {
      "my.topic.name": {
        "fsmID": "{{ index .Value \"my-jsons-fsm-identifier-key\" }}"
      }
    }
  },
  "snitchPort": 9009,
  "serverPort": 9000,
  "logFormat": "json"
}
```

## Starting

```
$ bookie
INFO[2016-12-24T13:04:18.291] Set up MariaDB connection for "root@tcp(localhost:3306)/".
INFO[2016-12-24T13:04:18.292] Initialize schema and tables
INFO[2016-12-24T13:04:20.858] Consuming.                                    offset=18336 partition=0 topic=movies
INFO[2016-12-24T13:04:20.859] Serving snitch.                               address=0.0.0.0:9009
INFO[2016-12-24T13:04:20.859] Serving bookie.                               address=0.0.0.0:52345
```

## API

TODO

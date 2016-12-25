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

* Last n FSMs
```
$ curl -s localhost:52345/latest?n=3 | jq
[
  {
    "tags": {
      "created": "2012-12-20"
    },
    "created": "2012-12-20T00:00:00Z",
    "id": "Nutcracker_3D:_Mariisky_Theatre_2012"
  },
  {
    "tags": {
      "created": "2012-12-20"
    },
    "created": "2012-12-20T00:00:00Z",
    "id": "From_Up_On_Poppy_Hill"
  },
  {
    "tags": {
      "created": "2012-12-23"
    },
    "created": "2012-12-23T00:00:00Z",
    "id": "The_Lord_Of_The_Rings_Trilogy"
  }
]
```

* Offset info for a particular FSM by id
```
$ curl -s localhost:52345/fsm?id=The_Lord_Of_The_Rings_Trilogy | jq
{
  "topics": {
    "movies": {
      "partitions": {
        "0": {
          "start": 37120,
          "end": 37123,
          "lastScraped": 46362,
          "count": 4
        }
      },
      "count": 4
    }
  },
  "tags": {
    "created": "2012-12-23"
  },
  "created": "2012-12-23T00:00:00Z",
  "id": "The_Lord_Of_The_Rings_Trilogy"
}

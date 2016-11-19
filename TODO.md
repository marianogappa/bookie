# Bookie

## Problem that it solves

- Not knowing where to consume from on a large topic is a big problem.
- Consuming from a node far in terms of latency from where Kafka is located is 2 orders of magnitude slower than on an adjacent node according to our tests.

## Use cases

- Faster loading for p6
- Faster loading for flowbro (also considering summarising for large cardinality topics)
- Quick access to latest FSMs started on a system

## Functional requirements

- Consume multiple Kafka topics, potentially with multiple partitions
- Determine fsmId from each message on each queue
- Obtain (k,v) tags from each fsmId, based on message's content and some configuration file
- Keep track of the first and last offset for each fsmId.topic.partition in some storage
- Expose this information via a convenience HTTP endpoint, independently of the scraping process
- Ability to reconsume from a given offset, rather than starting from scratch
- It is unfeasible to keep the entire scrape in memory while scraping
- Maintain a count of messages per topic; useful when high cardinality makes consuming the topic impractical
- Maintain some form of fsm.timestamp field such that FSMs can be time-sorted

## Strategy

- Go templates for fsmId determining logic (see in flowbro)
- fsmIdAlias strategy for messages that don't contain the fsmId (see in flowbro)
- Go templates for determining (k,v) tags; an empty string as value should not override a non-empty one
- Basic idea is to go through the topics and take note of offsets where fsmIds were first and last seen
- Snapshots should be saved at regular offset intervals to allow refreshing data from a given offset; snapshots could contain identical data and endpoint use the latest available
- Scaping process should hold current snapshot interval (measured in messages); messages that aggregate on existing FSMs should be queried and retrieved. After snapshot interval finishes, results should be stored in a batch.

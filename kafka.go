package main

import (
	"fmt"
	"strings"

	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
)

type cluster struct {
	brokers  []string
	consumer sarama.Consumer
	client   sarama.Client

	partitionConsumers []sarama.PartitionConsumer
	chs                []<-chan *sarama.ConsumerMessage
}

func (c *cluster) addPartitionConsumer(pc sarama.PartitionConsumer) {
	c.partitionConsumers = append(c.partitionConsumers, pc)
}

func (c *cluster) addCh(ch <-chan *sarama.ConsumerMessage) {
	c.chs = append(c.chs, ch)
}

func (c *cluster) close() {
	log.Printf("Trying to close cluster with brokers %v", c.brokers)

	log.Printf("Trying to close %v partition consumers for cluster with brokers %v", len(c.partitionConsumers), c.brokers)
	for _, pc := range c.partitionConsumers {
		if err := pc.Close(); err != nil {
			log.Printf("Error while trying to close partition consumer for cluster with brokers %v. err=%v", c.brokers, err)
		}
	}

	log.Printf("Trying to close consumer for cluster with brokers %v", c.brokers)
	if err := c.consumer.Close(); err != nil {
		log.Printf("Error while trying to close consumer for cluster with brokers %v. err=%v", c.brokers, err)
	} else {
		log.Printf("Successfully closed consumer for cluster with brokers %v", c.brokers)
	}

	log.Printf("Trying to close client for cluster with brokers %v", c.brokers)
	if err := c.client.Close(); err != nil {
		log.Printf("Error while trying to close client for cluster with brokers %v. err=%v", c.brokers, err)
	} else {
		log.Printf("Successfully closed client for cluster with brokers %v", c.brokers)
	}

	log.Printf("Finished trying to close cluster with brokers %v", c.brokers)
}

type topicRecord struct {
	topic      string
	partitions map[int32]int64
}

func (c *cluster) addTopic(tr topicRecord, bootstrapFrom int64) error {
	client, consumer, brokers, topic := c.client, c.consumer, c.brokers, tr.topic

	partitions, err := resolvePartitions(topic, consumer)
	if err != nil {
		return err
	}

	for _, partition := range partitions {
		offset, err := resolveOffset(topic, partition, tr.partitions[partition], bootstrapFrom, client)
		if err != nil {
			return fmt.Errorf("Could not resolve offset for %v, %v, %v. err=%v", brokers, topic, partition, err)
		}

		partitionConsumer, err := consumer.ConsumePartition(topic, int32(partition), offset)
		if err != nil {
			return fmt.Errorf("Failed to consume partition %v err=%v\n", partition, err)
		}

		c.addPartitionConsumer(partitionConsumer)
		c.addCh(partitionConsumer.Messages())
		log.WithFields(log.Fields{"topic": topic, "partition": partition, "offset": offset}).Info("Consuming.")
	}

	return nil
}

func mustSetupCluster(conf kafkaConfig, scrapes map[string]topicRecord) cluster {
	c := cluster{brokers: strings.Split(conf.Brokers, ",")}

	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V0_10_0_0
	client, err := sarama.NewClient(c.brokers, saramaConfig)
	if err != nil {
		log.Fatalf("Error creating client. err=%v", err)
	}

	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		log.Fatalf("Error creating consumer. err=%v", err)
	}

	c.client = client
	c.consumer = consumer

	for n := range conf.Topics {
		if _, ok := scrapes[n]; !ok {
			scrapes[n] = topicRecord{topic: n, partitions: map[int32]int64{}}
		}
	}

	for _, tr := range scrapes {
		log.WithFields(log.Fields{"topic": tr.topic, "partitions": tr.partitions}).Info("Trying to consume...")
		err := c.addTopic(tr, conf.BootstrapFrom)
		if err != nil {
			log.WithError(err).Fatal("Could not add topic ", tr.topic)
		}
	}

	return c
}

func resolvePartitions(topic string, consumer sarama.Consumer) ([]int32, error) {
	partitions, err := consumer.Partitions(topic)
	if err != nil {
		return partitions, fmt.Errorf("Error fetching partitions for topic %v. err=%v", topic, err)
	}
	return partitions, nil
}

func resolveOffset(topic string, partition int32, candidateOffset, bootstrapFrom int64, client sarama.Client) (int64, error) {
	oldest, err := client.GetOffset(topic, partition, sarama.OffsetOldest)
	if err != nil {
		return 0, err
	}

	newest, err := client.GetOffset(topic, partition, sarama.OffsetNewest)
	if err != nil {
		return 0, err
	}

	if candidateOffset <= 0 {
		if bootstrapFrom == 0 || bootstrapFrom == sarama.OffsetOldest {
			return oldest, nil
		}
		if bootstrapFrom == sarama.OffsetNewest {
			return newest, nil
		}
		if bootstrapFrom < 0 {
			return max(newest+bootstrapFrom, oldest), nil
		}
		if bootstrapFrom > 0 {
			return min(bootstrapFrom, newest), nil
		}
	}

	if candidateOffset >= oldest && candidateOffset <= newest {
		return candidateOffset, nil
	}

	return 0, fmt.Errorf("Invalid value for consumer offset")
}

func max(a, b int64) int64 {
	if b > a {
		return b
	}
	return a
}

func min(a, b int64) int64 {
	if b < a {
		return b
	}
	return a
}

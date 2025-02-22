package main

import (
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092,localhost:9093,localhost:9094",
		"acks":              "1",
		"retries":           3,
	})
	if err != nil {
		log.Fatalf("Failed to create producer: %s", err)
	}
	defer producer.Close()

	topic := "sample-topic"

	message := "This is a sample message"

	err = producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(message),
	}, nil)

	if err != nil {
		log.Fatalf("Failed to produce a message: %s", err)
	} else {
		log.Printf("Messge produced: %s", message)
	}

	event := <-producer.Events()
	m, ok := event.(*kafka.Message)

	if ok && m.TopicPartition.Error == nil {
		log.Printf("Message delivered to partition %d at offset %v", m.TopicPartition.Partition, m.TopicPartition.Offset)
	} else {
		log.Printf("Failed to deliver message: %v", m.TopicPartition.Error)
	}
	producer.Flush(15 * 1000)
}

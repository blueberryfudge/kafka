package main

import (
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":       "localhost:9092,localhost:9093,localhost:9094",
		"group.id":                "at-least-once-group",
		"auto.offset.reset":       "earliest",
		"enable.auto.commit":      true,
		"auto.commit.interval.ms": 5000,
	})
	if err != nil {
		log.Fatalf("Failed to create consumer: %s", err)
	}

	defer consumer.Close()

	topic := "sample-topic"
	err = consumer.Subscribe(topic, nil)
	if err != nil {
		log.Fatalf("Failed to subscribe to topic: %s", err)
	}

	log.Println("Consumer started, waiting for messages...")

	for {
		msg, err := consumer.ReadMessage(-1)
		if err == nil {
			log.Printf("Consumed message: %s , Partition: %d, Offset: %d", string(msg.Value), msg.TopicPartition, msg.TopicPartition.Offset)
		} else {
			log.Printf("Consumer error: %s", err)
		}
	}
}

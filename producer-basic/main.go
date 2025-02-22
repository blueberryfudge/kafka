package main

import (
	"log"
	produce_message "producer-basic/produce_messsage"

	kkafka "producer-basic/infrastructure/kafka"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	config := &kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092,localhost:9093,localhost:9094",
		"acks":              "1",
		"retries":           3,
	}
	producer, err := kkafka.NewKafkaProducer(config)
	if err != nil {
		log.Fatalf("Failed to create kafka producer: %s", err)
	}

	defer producer.Close()

	ProduceMessage := produce_message.ProduceMessage{Producer: producer}

	topic := "sample-topic"
	message := "This is a sample message"

	if err := ProduceMessage.Produce(topic, message); err != nil {
		log.Fatalf("Failed to produce message: %s", err)
	} else {
		log.Printf("Message produced: %s", message)
	}
}

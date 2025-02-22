package kafka

import (
	"log"
	"producer-basic/domain"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type KafkaProducer struct {
	producer *kafka.Producer
}

func NewKafkaProducer(configMap *kafka.ConfigMap) (*KafkaProducer, error) {
	p, err := kafka.NewProducer(configMap)
	if err != nil {
		return nil, err
	}
	return &KafkaProducer{
		producer: p,
	}, nil
}

func (kp *KafkaProducer) Produce(msg domain.Message) error {
	kMsg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &msg.Topic, Partition: kafka.PartitionAny},
		Value:          []byte(msg.Body),
	}

	if err := kp.producer.Produce(kMsg, nil); err != nil {
		return err
	}

	event := <-kp.producer.Events()
	m, ok := event.(*kafka.Message)
	if ok && m.TopicPartition.Error != nil {
		log.Printf("Failed to deliver message: %v", m.TopicPartition.Error)
		return m.TopicPartition.Error
	}

	return nil
}

func (kp *KafkaProducer) Close() {
	kp.producer.Close()
}

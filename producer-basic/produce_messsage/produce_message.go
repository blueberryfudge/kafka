package produce_message

import "producer-basic/domain"

type ProduceMessage struct {
	Producer domain.Producer
}

func (pm *ProduceMessage) Produce(topic, body string) error {
	msg := domain.Message{
		Topic: topic,
		Body:  body,
	}
	return pm.Producer.Produce(msg)
}

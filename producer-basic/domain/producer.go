package domain

type Producer interface {
	Produce(msg Message) error
	Close()
}

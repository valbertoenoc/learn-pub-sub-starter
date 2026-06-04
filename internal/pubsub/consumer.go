package pubsub

import (
	"encoding/json"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
)

type SimpleQueueType int

const (
	SimpleQueueDurable SimpleQueueType = iota
	SimpleQueueTransient
)

func DeclareAndBind(conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	queue, err := ch.QueueDeclare(
		queueName,
		queueType == SimpleQueueDurable,   // durable
		queueType == SimpleQueueTransient, // auto delete
		queueType == SimpleQueueTransient, // exclusive
		false,                             // no-wait
		nil,
	)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	err = ch.QueueBind(
		queueName,
		key,
		exchange,
		false,
		nil,
	)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	return ch, queue, nil
}

func SubscribeJSON[T any](conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType, handler func(T) AckType) error {
	ch, queue, err := DeclareAndBind(
		conn,
		exchange,
		queueName,
		key,
		queueType,
	)
	if err != nil {
		return fmt.Errorf("Could not declare and bind queue: %v", err)
	}
	log.Printf("Queue %v declared and bound!", queue)

	deliveries, err := ch.Consume(
		queue.Name,
		"", // consumer name empty will be auto generated
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("Could not consume queue: %v", err)
	}

	go func() {
		for delivery := range deliveries {
			var msg T
			err := json.Unmarshal(delivery.Body, &msg)
			if err != nil {
				log.Printf("error while unmarshalling delivery body: %v", err)
				continue
			}
			ackType := handler(msg)

			switch ackType {
			case Ack:
				log.Printf("[ack] message processed successfully.")
				delivery.Ack(false)
			case NackRequeue:
				log.Printf("[nack requeue] message processing failed - requeueing.")
				delivery.Nack(false, true)
			case NackDiscard:
				log.Printf("[nack discard] message processing failed - discarding.")
				delivery.Nack(false, false)
			}

		}
	}()

	return nil
}

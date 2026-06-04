package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	connString := "amqp://guest:guest@localhost:5672/"

	fmt.Println("Starting Peril server...")

	conn, err := amqp.Dial(connString)
	if err != nil {
		log.Fatalf("Unable to connect to message broker: %v", err)
	}
	defer conn.Close()

	connChannel, err := conn.Channel()
	if err != nil {
		log.Fatalf("Unable to get connection channel: %v", err)
	}

	_, queue, err := pubsub.DeclareAndBind(
		conn,
		routing.ExchangePerilTopic,
		routing.GameLogSlug,
		routing.GameLogSlug+".*",
		pubsub.SimpleQueueDurable,
	)
	if err != nil {
		log.Fatalf("Unable to create and bind queue: %v", err)
	}
	log.Printf("Queue %v declared and bound!", queue)

	gamelogic.PrintServerHelp()
	for {
		args := gamelogic.GetInput()
		if len(args) == 0 {
			continue
		}

		cmd := args[0]

		switch cmd {
		case "pause":
			log.Printf("[pause] - sending pause message")
			pubsub.PublishJSON(connChannel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})
		case "resume":
			log.Printf("[resume] - sending resume message")
			pubsub.PublishJSON(connChannel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: false})
		case "quit":
			log.Printf("[quit] - exiting program")
			return
		default:
			log.Printf("[unknown] - unknown command: %s", cmd)
		}
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan

	fmt.Println("Interrupt signal received, shutting down server...")
}

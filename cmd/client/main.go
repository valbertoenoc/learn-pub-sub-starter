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
	fmt.Println("Starting Peril client...")

	conn, err := amqp.Dial(connString)
	if err != nil {
		log.Fatalf("Unable to connect to message broker.")
	}
	defer conn.Close()

	connChannel, err := conn.Channel()
	if err != nil {
		log.Fatalf("Unable to get connection channel: %v", err)
	}

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("Unable to connect to message broker.")
	}

	gameState := gamelogic.NewGameState(username)
	// subscribe pause handler
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilDirect,   // exchange name
		routing.PauseKey+"."+username, // queue name
		routing.PauseKey,              // queue key
		pubsub.SimpleQueueTransient,
		pubsub.HandlerPause(gameState),
	)
	if err != nil {
		log.Fatalf("Unable to subscribe to message broker.")
	}

	// subscribe move handler
	var keyMoveUser = routing.ArmyMovesPrefix + "." + username
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,   // exchange name
		keyMoveUser,                  // queue name
		routing.ArmyMovesPrefix+".*", // queue key
		pubsub.SimpleQueueTransient,
		pubsub.HandlerMove(gameState, connChannel),
	)
	if err != nil {
		log.Fatalf("Unable to subscribe to %s queue.", keyMoveUser)
	}

	// subscribe war handler
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,         // exchange name
		routing.WarRecognitionsPrefix,      // queue name
		routing.WarRecognitionsPrefix+".*", // queue key
		pubsub.SimpleQueueDurable,
		pubsub.HandlerWar(gameState, connChannel),
	)
	if err != nil {
		log.Fatal("Unable to subscribe to war queue.")
	}
	log.Println("Successfully subscribed to message broker.")

	for {
		args := gamelogic.GetInput()
		if len(args) == 0 {
			continue
		}

		cmd := args[0]

		switch cmd {
		case "spawn":
			err := gameState.CommandSpawn(args)
			if err != nil {
				log.Printf("[spawn] failed at spawning units: %v", err)
			}
		case "move":
			move, err := gameState.CommandMove(args)
			if err != nil {
				log.Printf("[move] failed at moving units: %v", err)
			}
			err = pubsub.PublishJSON(connChannel, routing.ExchangePerilTopic, keyMoveUser, move)
			if err != nil {
				log.Println("[move] could not publish message.")
			}
			log.Println("[move] move message published successfully.")
		case "status":
			gameState.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			log.Println("[spam] - Spamming not allowed!")
		case "quit":
			gamelogic.PrintQuit()
		default:
			log.Printf("[unknown] - unknown command: %s", cmd)
		}
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	<-sigCh
}

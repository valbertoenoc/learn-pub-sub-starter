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

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("Unable to connect to message broker.")
	}

	_, queue, err := pubsub.DeclareAndBind(
		conn,
		routing.ExchangePerilDirect,   // exchange name
		routing.PauseKey+"."+username, // queue name
		routing.PauseKey,              // queue key
		pubsub.SimpleQueueTransient,
	)
	log.Printf("Queue %v declared and bound!", queue)

	gameState := gamelogic.NewGameState(username)
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
			_, err := gameState.CommandMove(args)
			if err != nil {
				log.Printf("[move] failed at moving units: %v", err)
			}
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

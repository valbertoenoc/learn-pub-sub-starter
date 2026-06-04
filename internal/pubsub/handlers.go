package pubsub

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

func HandlerPause(gs *gamelogic.GameState) func(routing.PlayingState) AckType {
	return func(ps routing.PlayingState) AckType {
		defer fmt.Print("> ")
		gs.HandlePause(ps)

		return Ack
	}
}

func HandlerMove(gs *gamelogic.GameState) func(gamelogic.ArmyMove) AckType {
	return func(move gamelogic.ArmyMove) AckType {
		defer fmt.Print("> ")
		moveOutcome := gs.HandleMove(move)

		if moveOutcome == gamelogic.MoveOutComeSafe || moveOutcome == gamelogic.MoveOutcomeMakeWar {
			log.Printf("[move] move outcome: %v - acknowledging message", moveOutcome)
			return Ack
		} else {
			log.Printf("[move] move outcome: %v - discarding message", moveOutcome)
			return NackDiscard
		}
	}
}

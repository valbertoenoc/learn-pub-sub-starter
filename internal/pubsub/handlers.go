package pubsub

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func HandlerPause(gs *gamelogic.GameState) func(routing.PlayingState) AckType {
	return func(ps routing.PlayingState) AckType {
		defer fmt.Print("> ")
		gs.HandlePause(ps)

		return Ack
	}
}

func HandlerMove(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.ArmyMove) AckType {
	return func(move gamelogic.ArmyMove) AckType {
		defer fmt.Print("> ")
		moveOutcome := gs.HandleMove(move)

		switch moveOutcome {
		case gamelogic.MoveOutcomeSamePlayer:
			log.Printf("[move] move outcome: %v - discarding message", moveOutcome)
			return NackDiscard
		case gamelogic.MoveOutComeSafe:
			log.Printf("[move] move outcome: %v - acknowledging message", moveOutcome)
			return Ack
		case gamelogic.MoveOutcomeMakeWar:
			log.Printf("[move] move outcome: %v - acknowledging message", moveOutcome)
			key := routing.WarRecognitionsPrefix + "." + gs.GetUsername()
			err := PublishJSON(
				ch,
				routing.ExchangePerilTopic,
				key,
				gamelogic.RecognitionOfWar{
					Attacker: move.Player,
					Defender: gs.GetPlayerSnap(),
				},
			)
			if err != nil {
				log.Printf("[move] error publishing recognition of war: %v", err)
			}
			log.Println("[move] starting war")
			return NackRequeue
		}
		return NackDiscard
	}
}

func HandlerWar(gs *gamelogic.GameState) func(gamelogic.ArmyMove) AckType {
	return func(gamelogic.ArmyMove) AckType {
		defer fmt.Print("> ")

		warOutcome, _, _ := gs.HandleWar(gamelogic.RecognitionOfWar{Attacker: gs.Player, Defender: gs.GetPlayerSnap()})
		switch warOutcome {
		case gamelogic.WarOutcomeNotInvolved:
			return NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			return NackDiscard
		case gamelogic.WarOutcomeOpponentWon:
			return Ack
		case gamelogic.WarOutcomeYouWon:
			return Ack
		case gamelogic.WarOutcomeDraw:
			return Ack
		}

		return NackDiscard
	}

}

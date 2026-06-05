package pubsub

import (
	"fmt"
	"log"
	"time"

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
			return Ack
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
				return NackRequeue
			}
			log.Println("[move] starting war")
			return Ack
		}
		return NackDiscard
	}
}

func HandlerWar(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.RecognitionOfWar) AckType {
	return func(rw gamelogic.RecognitionOfWar) AckType {
		defer fmt.Print("> ")

		warOutcome, winner, loser := gs.HandleWar(rw)
		switch warOutcome {
		case gamelogic.WarOutcomeNotInvolved:
			return NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			return NackDiscard
		case gamelogic.WarOutcomeOpponentWon:
			err := PublishGameLog(ch, routing.GameLog{
				Message:     fmt.Sprintf("%s won a war against %s", winner, loser),
				Username:    gs.GetUsername(),
				CurrentTime: time.Now(),
			})
			if err != nil {
				return NackRequeue
			}
			return Ack
		case gamelogic.WarOutcomeYouWon:
			err := PublishGameLog(ch, routing.GameLog{
				Message:     fmt.Sprintf("%s won a war against %s", winner, loser),
				Username:    gs.GetUsername(),
				CurrentTime: time.Now(),
			})
			if err != nil {
				return NackRequeue
			}
			return Ack
		case gamelogic.WarOutcomeDraw:
			err := PublishGameLog(ch, routing.GameLog{
				Message:     fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser),
				Username:    gs.GetUsername(),
				CurrentTime: time.Now(),
			})
			if err != nil {
				return NackRequeue
			}
			return Ack
		}

		return NackDiscard
	}

}

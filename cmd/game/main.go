package main

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/Filip-Pajalic/EQGO/event"
)

const (
	enemyKilled  = "EnemyKilled"
	itemPickedUp = "ItemPickedUp"
)

type EnemyKilled struct {
	EnemyID string
	XP      int
}

type ItemPickedUp struct {
	ItemID string
	Name   string
}

type GameState struct {
	Frame      int
	XP         int
	QuestKills int
	Inventory  []string
	Sounds     []string
	UI         []string
}

func main() {
	ctx := context.Background()
	state := &GameState{}
	events := event.NewQueue(32)
	events.AddObserver(frameDebug{})

	mustSubscribe(events, enemyKilled, state.addXP)
	mustSubscribe(events, enemyKilled, state.advanceQuest)
	mustSubscribe(events, enemyKilled, state.playEnemySound)
	mustSubscribe(events, itemPickedUp, state.addItem)
	mustSubscribe(events, itemPickedUp, state.playPickupSound)
	mustSubscribe(events, itemPickedUp, state.showPickupToast)

	for frame := 1; frame <= 4; frame++ {
		state.Frame = frame
		state.Sounds = state.Sounds[:0]
		state.UI = state.UI[:0]

		fmt.Printf("\nframe %d: simulate gameplay\n", frame)
		simulateFrame(ctx, events, frame)

		dispatched, err := events.DispatchPending(ctx)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("frame %d: dispatched=%d xp=%d quest_kills=%d inventory=%s sounds=%s ui=%s\n",
			frame,
			dispatched,
			state.XP,
			state.QuestKills,
			strings.Join(state.Inventory, ","),
			strings.Join(state.Sounds, ","),
			strings.Join(state.UI, " | "),
		)
	}

	if err := events.Stop(ctx); err != nil {
		log.Fatal(err)
	}
}

func simulateFrame(ctx context.Context, q *event.Queue, frame int) {
	switch frame {
	case 1:
		publish(ctx, q, enemyKilled, "enemy-1", EnemyKilled{EnemyID: "slime", XP: 10})
	case 2:
		publish(ctx, q, itemPickedUp, "item-1", ItemPickedUp{ItemID: "potion-1", Name: "Potion"})
	case 3:
		publish(ctx, q, enemyKilled, "enemy-2", EnemyKilled{EnemyID: "bat", XP: 15})
		publish(ctx, q, itemPickedUp, "item-2", ItemPickedUp{ItemID: "gem-1", Name: "Blue Gem"})
	default:
		fmt.Println("no gameplay events this frame")
	}
}

func (s *GameState) addXP(_ context.Context, _ event.BaseEventInfo, killed EnemyKilled) error {
	s.XP += killed.XP
	return nil
}

func (s *GameState) advanceQuest(_ context.Context, _ event.BaseEventInfo, killed EnemyKilled) error {
	s.QuestKills++
	s.UI = append(s.UI, fmt.Sprintf("quest progress: defeated %s", killed.EnemyID))
	return nil
}

func (s *GameState) playEnemySound(_ context.Context, _ event.BaseEventInfo, killed EnemyKilled) error {
	s.Sounds = append(s.Sounds, "enemy_down:"+killed.EnemyID)
	return nil
}

func (s *GameState) addItem(_ context.Context, _ event.BaseEventInfo, item ItemPickedUp) error {
	s.Inventory = append(s.Inventory, item.Name)
	return nil
}

func (s *GameState) playPickupSound(_ context.Context, _ event.BaseEventInfo, item ItemPickedUp) error {
	s.Sounds = append(s.Sounds, "pickup:"+item.ItemID)
	return nil
}

func (s *GameState) showPickupToast(_ context.Context, _ event.BaseEventInfo, item ItemPickedUp) error {
	s.UI = append(s.UI, "picked up "+item.Name)
	return nil
}

type frameDebug struct{}

func (frameDebug) ObserveEvent(_ context.Context, result event.DispatchResult) {
	if result.Err != nil {
		fmt.Printf("  event %s failed: %v\n", result.Info.Name, result.Err)
		return
	}
	fmt.Printf("  event %s handled\n", result.Info.Name)
}

func mustSubscribe[T any](q *event.Queue, eventName string, h event.Handler[T]) {
	if err := event.Subscribe(q, eventName, h); err != nil {
		log.Fatal(err)
	}
}

func publish[T any](ctx context.Context, q *event.Queue, eventName, id string, payload T) {
	if err := q.Publish(ctx, event.New(event.NewInfo(id, eventName), payload)); err != nil {
		log.Fatal(err)
	}
}

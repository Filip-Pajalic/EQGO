package main

import (
	"app/event"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"sync"
	"time"
)

var logger = initLogger()

func initLogger() *slog.Logger {
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		AddSource:   false,
		ReplaceAttr: nil,
	})
	return slog.New(handler)
}

type UserCreated struct {
	Username string
	Email    string
}

type OrderPlaced struct {
	OrderID string
	Amount  float64
}

type ProductAdded struct {
	ProductID string
	Name      string
	Stock     int
}

func logUser(u UserCreated) {
	logger.Info("UserCreated",
		slog.String("username", u.Username),
		slog.String("email", u.Email),
	)
}

func sendInvoice(o OrderPlaced) {
	logger.Info("OrderPlaced",
		slog.String("order_id", o.OrderID),
		slog.Float64("amount", o.Amount),
	)
}

func updateInventory(p ProductAdded) {
	logger.Info("ProductAdded",
		slog.String("product_id", p.ProductID),
		slog.String("name", p.Name),
		slog.Int("stock", p.Stock),
	)
}

func main() {
	rand.Seed(time.Now().UnixNano())

	numEvents := 10
	eq := event.NewEventQueue(100)
	eq.AddGlobalHook(event.AuditLogger)
	eq.Start()

	var wg sync.WaitGroup

	for i := 0; i < numEvents; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			time.Sleep(time.Duration(rand.Intn(500)) * time.Millisecond)

			userEvt := &event.TypedEvent[UserCreated]{
				Info: event.BaseEventInfo{
					ID:        fmt.Sprintf("userEvt-%d", i),
					Name:      "UserCreated",
					Timestamp: time.Now(),
				},
				Payload: UserCreated{
					Username: fmt.Sprintf("alice%d", i),
					Email:    fmt.Sprintf("alice%d@example.com", i),
				},
			}
			userEvt.On(logUser)
			eq.Add(userEvt)
		}(i)
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			time.Sleep(time.Duration(rand.Intn(500)) * time.Millisecond)

			orderEvt := &event.TypedEvent[OrderPlaced]{
				Info: event.BaseEventInfo{
					ID:        fmt.Sprintf("orderEvt-%d", i),
					Name:      "OrderPlaced",
					Timestamp: time.Now(),
				},
				Payload: OrderPlaced{
					OrderID: fmt.Sprintf("ORD%03d", i),
					Amount:  100.0 + float64(i),
				},
			}
			orderEvt.On(sendInvoice)
			eq.Add(orderEvt)
		}(i)
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			time.Sleep(time.Duration(rand.Intn(500)) * time.Millisecond)

			productEvt := &event.TypedEvent[ProductAdded]{
				Info: event.BaseEventInfo{
					ID:        fmt.Sprintf("productEvt-%d", i),
					Name:      "ProductAdded",
					Timestamp: time.Now(),
				},
				Payload: ProductAdded{
					ProductID: fmt.Sprintf("P%03d", i),
					Name:      fmt.Sprintf("Gadget%d", i),
					Stock:     42 + i,
				},
			}
			productEvt.On(updateInventory)
			eq.Add(productEvt)
		}(i)
	}

	wg.Wait()
	eq.Stop()
}

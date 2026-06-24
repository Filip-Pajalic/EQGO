package main

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"sync"
	"time"

	"github.com/Filip-Pajalic/EQGO/event"
)

var logger = initLogger()

func initLogger() *slog.Logger {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)
	return logger
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

const (
	userCreated  = "UserCreated"
	orderPlaced  = "OrderPlaced"
	productAdded = "ProductAdded"
)

func sendWelcomeEmail(ctx context.Context, info event.BaseEventInfo, u UserCreated) error {
	logger.InfoContext(ctx, "send welcome email",
		slog.String("event_id", info.ID),
		slog.String("username", u.Username),
		slog.String("email", u.Email),
	)
	return nil
}

func sendInvoice(ctx context.Context, info event.BaseEventInfo, o OrderPlaced) error {
	logger.InfoContext(ctx, "send invoice",
		slog.String("event_id", info.ID),
		slog.String("order_id", o.OrderID),
		slog.Float64("amount", o.Amount),
	)
	return nil
}

func updateInventory(ctx context.Context, info event.BaseEventInfo, p ProductAdded) error {
	logger.InfoContext(ctx, "update inventory search index",
		slog.String("event_id", info.ID),
		slog.String("product_id", p.ProductID),
		slog.String("name", p.Name),
		slog.Int("stock", p.Stock),
	)
	return nil
}

func main() {
	ctx := context.Background()
	eq := event.NewQueue(100)
	eq.AddObserver(auditLogger{})
	mustSubscribe(eq, userCreated, sendWelcomeEmail)
	mustSubscribe(eq, orderPlaced, sendInvoice)
	mustSubscribe(eq, productAdded, updateInventory)

	if err := eq.Start(); err != nil {
		logger.Error("start queue", slog.Any("error", err))
		os.Exit(1)
	}

	var wg sync.WaitGroup
	for i := range 10 {
		i := i

		wg.Go(func() {
			time.Sleep(time.Duration(rand.IntN(500)) * time.Millisecond)
			userEvt := event.New(
				event.NewInfo(fmt.Sprintf("user-%d", i), userCreated),
				UserCreated{
					Username: fmt.Sprintf("alice%d", i),
					Email:    fmt.Sprintf("alice%d@example.com", i),
				},
			)
			publish(ctx, eq, userEvt)
		})

		wg.Go(func() {
			time.Sleep(time.Duration(rand.IntN(500)) * time.Millisecond)
			orderEvt := event.New(
				event.NewInfo(fmt.Sprintf("order-%d", i), orderPlaced),
				OrderPlaced{
					OrderID: fmt.Sprintf("ORD%03d", i),
					Amount:  100.0 + float64(i),
				},
			)
			publish(ctx, eq, orderEvt)
		})

		wg.Go(func() {
			time.Sleep(time.Duration(rand.IntN(500)) * time.Millisecond)
			productEvt := event.New(
				event.NewInfo(fmt.Sprintf("product-%d", i), productAdded),
				ProductAdded{
					ProductID: fmt.Sprintf("P%03d", i),
					Name:      fmt.Sprintf("Gadget%d", i),
					Stock:     42 + i,
				},
			)
			publish(ctx, eq, productEvt)
		})
	}

	wg.Wait()
	if err := eq.Stop(ctx); err != nil {
		logger.Error("stop queue", slog.Any("error", err))
		os.Exit(1)
	}
}

func mustSubscribe[T any](q *event.Queue, eventName string, h event.Handler[T]) {
	if err := event.Subscribe(q, eventName, h); err != nil {
		logger.Error("subscribe handler",
			slog.String("event", eventName),
			slog.Any("error", err),
		)
		os.Exit(1)
	}
}

type auditLogger struct{}

func (auditLogger) ObserveEvent(ctx context.Context, result event.DispatchResult) {
	attrs := []slog.Attr{
		slog.String("version", "1.0"),
		slog.String("source", "/example"),
		slog.String("id", result.Info.ID),
		slog.String("type", result.Info.Name),
		slog.Time("time", result.Info.Timestamp),
		slog.String("datacontenttype", "application/json"),
		slog.Any("data", result.Payload),
	}
	if result.Err != nil {
		attrs = append(attrs, slog.String("error", result.Err.Error()))
	}

	logger.LogAttrs(ctx, slog.LevelInfo, "event", attrs...)
}

func publish(ctx context.Context, q *event.Queue, e event.Event) {
	if err := q.Publish(ctx, e); err != nil {
		logger.ErrorContext(ctx, "publish event",
			slog.String("event", e.EventInfo().Name),
			slog.Any("error", err),
		)
	}
}

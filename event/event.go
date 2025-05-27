package event

import (
	"log/slog"
	"time"
)

type BaseEventInfo struct {
	ID        string
	Name      string
	Timestamp time.Time
}

type TypedEvent[T any] struct {
	Info     BaseEventInfo
	Payload  T
	handlers []func(T)
}

func (e *TypedEvent[T]) On(h func(T)) *TypedEvent[T] {
	e.handlers = append(e.handlers, h)
	return e
}

func (e *TypedEvent[T]) Trigger() {
	for _, h := range e.handlers {
		h(e.Payload)
	}
}

func (e *TypedEvent[T]) EventInfo() BaseEventInfo { return e.Info }
func (e *TypedEvent[T]) PayloadData() any         { return e.Payload }

func AuditLogger(info BaseEventInfo, payload any) {
	slog.Info("event",
		slog.String("version", "1.0"),
		slog.String("source", "/myapp"),
		slog.String("id", info.ID),
		slog.String("type", info.Name),
		slog.Time("time", info.Timestamp),
		slog.String("datacontenttype", "application/json"),
		slog.Any("data", payload),
	)
}

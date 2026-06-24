package event

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// BaseEventInfo carries metadata shared by every event.
type BaseEventInfo struct {
	ID        string
	Name      string
	Timestamp time.Time
}

// NewInfo creates event metadata with a UTC timestamp.
func NewInfo(id, name string) BaseEventInfo {
	return BaseEventInfo{
		ID:        id,
		Name:      name,
		Timestamp: time.Now().UTC(),
	}
}

// Handler handles a typed event payload.
type Handler[T any] func(context.Context, T) error

// TypedEvent stores a payload and the typed handlers that should receive it.
type TypedEvent[T any] struct {
	Info     BaseEventInfo
	Payload  T
	handlers []Handler[T]
}

// New creates a typed event with optional handlers.
func New[T any](info BaseEventInfo, payload T, handlers ...Handler[T]) *TypedEvent[T] {
	e := &TypedEvent[T]{
		Info:    info,
		Payload: payload,
	}
	return e.On(handlers...)
}

// On registers handlers and returns the event for chaining.
func (e *TypedEvent[T]) On(handlers ...Handler[T]) *TypedEvent[T] {
	for _, h := range handlers {
		if h != nil {
			e.handlers = append(e.handlers, h)
		}
	}
	return e
}

// Dispatch runs handlers in order until one fails or the context is canceled.
func (e *TypedEvent[T]) Dispatch(ctx context.Context) (err error) {
	if ctx == nil {
		ctx = context.Background()
	}

	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("event %q handler panicked: %v", e.Info.Name, recovered)
		}
	}()

	for _, h := range e.handlers {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := h(ctx, e.Payload); err != nil {
			return err
		}
	}
	return nil
}

// EventInfo returns the event metadata.
func (e *TypedEvent[T]) EventInfo() BaseEventInfo { return e.Info }

// PayloadData returns the typed payload as any for queue hooks.
func (e *TypedEvent[T]) PayloadData() any { return e.Payload }

// AuditLogger logs event metadata, payload data, and handler errors with slog.
func AuditLogger(ctx context.Context, info BaseEventInfo, payload any, err error) {
	if ctx == nil {
		ctx = context.Background()
	}

	attrs := []slog.Attr{
		slog.String("version", "1.0"),
		slog.String("source", "/eqgo"),
		slog.String("id", info.ID),
		slog.String("type", info.Name),
		slog.Time("time", info.Timestamp),
		slog.String("datacontenttype", "application/json"),
		slog.Any("data", payload),
	}
	if err != nil {
		attrs = append(attrs, slog.String("error", err.Error()))
	}

	slog.LogAttrs(ctx, slog.LevelInfo, "event", attrs...)
}

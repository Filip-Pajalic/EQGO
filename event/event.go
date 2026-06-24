package event

import (
	"context"
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

// Event is the common interface used by Queue for all payload types.
type Event interface {
	EventInfo() BaseEventInfo
	PayloadData() any
}

// Handler handles a typed event payload.
type Handler[T any] func(context.Context, BaseEventInfo, T) error

// TypedEvent stores event metadata and a typed payload.
type TypedEvent[T any] struct {
	Info    BaseEventInfo
	Payload T
}

// New creates a typed event.
func New[T any](info BaseEventInfo, payload T) *TypedEvent[T] {
	return &TypedEvent[T]{
		Info:    info,
		Payload: payload,
	}
}

// EventInfo returns the event metadata.
func (e *TypedEvent[T]) EventInfo() BaseEventInfo { return e.Info }

// PayloadData returns the typed payload as any for queue observers.
func (e *TypedEvent[T]) PayloadData() any { return e.Payload }

# EQGO

EQGO is a small in-process event queue for Go. It keeps payload handlers type-safe with generics while exposing one simple queue for mixed event types.

It is meant for lightweight application events, demos, and local workflows. It is not a distributed broker, persistent queue, or retry system.

## Why Not Just Channels?

Use plain channels when one payload type and one consumer loop are enough.

EQGO is useful when an app has mixed event types but still wants typed handlers at the edges, context-aware publishing, lifecycle-controlled draining, and one observer surface for logging, metrics, or tracing. It keeps that plumbing in one small package without adding persistence, retries, or distributed delivery guarantees.

## Features

- Type-safe event payloads with generic handlers
- Context-aware publishing
- Safe `Start` / `Stop` lifecycle with queue draining
- Observer interface for auditing, metrics, and error reporting
- Handler error reporting without killing the queue worker
- Panic recovery around event handlers and observers
- No external dependencies

## Requirements

- Go 1.26.4 or newer

## Example

Run the bundled example with:

```sh
go run ./cmd/example
```

```go
package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"

	"github.com/Filip-Pajalic/EQGO/event"
)

type UserCreated struct {
	Username string
	Email    string
}

func main() {
	ctx := context.Background()
	q := event.NewQueue(32)
	q.AddObserver(auditLogger{
		logger: slog.New(slog.NewJSONHandler(os.Stdout, nil)),
	})

	if err := q.Start(); err != nil {
		log.Fatal(err)
	}

	user := event.New(
		event.NewInfo("user-1", "UserCreated"),
		UserCreated{Username: "alice", Email: "alice@example.com"},
		func(ctx context.Context, payload UserCreated) error {
			fmt.Println("created:", payload.Username)
			return nil
		},
	)

	if err := q.Publish(ctx, user); err != nil {
		log.Fatal(err)
	}
	if err := q.Stop(ctx); err != nil {
		log.Fatal(err)
	}
}

type auditLogger struct {
	logger *slog.Logger
}

func (a auditLogger) ObserveEvent(ctx context.Context, result event.DispatchResult) {
	attrs := []slog.Attr{
		slog.String("id", result.Info.ID),
		slog.String("type", result.Info.Name),
		slog.Time("time", result.Info.Timestamp),
		slog.Any("data", result.Payload),
	}
	if result.Err != nil {
		attrs = append(attrs, slog.String("error", result.Err.Error()))
	}

	a.logger.LogAttrs(ctx, slog.LevelInfo, "event", attrs...)
}
```

## API Shape

Create a queue:

```go
q := event.NewQueue(64)
```

Register observers before or after start:

```go
q.AddObserver(event.ObserverFunc(func(ctx context.Context, result event.DispatchResult) {
	// result.Err is the handler error or recovered panic, if one occurred.
}))
```

Start, publish, and stop:

```go
if err := q.Start(); err != nil {
	return err
}
if err := q.Publish(ctx, evt); err != nil {
	return err
}
if err := q.Stop(ctx); err != nil {
	return err
}
```

`Stop` closes the queue to new publishes, drains queued events, and waits for the worker to finish. `Publish` returns `ErrNotStarted`, `ErrClosed`, `ErrNilEvent`, or the context error when appropriate.

# EQGO

EQGO is a small in-process event queue for Go. It keeps payload handlers type-safe with generics while exposing one simple queue for mixed event types.

It is meant for lightweight application events, demos, and local workflows. It is not a distributed broker, persistent queue, or retry system.

## When to Use EQGO

EQGO is useful when an app has mixed event types but still wants typed handlers at the edges, context-aware publishing, lifecycle-controlled draining, and one observer surface for logging, metrics, or tracing. It keeps that plumbing in one small package without adding persistence, retries, or distributed delivery guarantees.

Publishers add facts to the queue. Subscribers decide what those facts cause, such as sending email, updating a search index, writing metrics, or calling another internal service.

## Features

- Type-safe event payloads with generic handlers
- Context-aware publishing
- Async worker mode with `Start`
- Manual dispatch mode with `DispatchPending`
- Safe `Stop` lifecycle with queue draining
- Observer interface for auditing, metrics, and error reporting
- Handler error reporting without killing the queue worker
- Panic recovery around event handlers and observers
- No external dependencies

## Requirements

- Go 1.26.4 or newer

## Example

Run the bundled async worker example with:

```sh
go run ./cmd/example
```

Run the manual-dispatch game loop example with:

```sh
go run ./cmd/game
```

The game example publishes `EnemyKilled` and `ItemPickedUp` facts during simulation, then calls `DispatchPending` once per frame so XP, quest, audio, inventory, and UI systems mutate state at a controlled point.

```go
package main

import (
	"context"
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
	if err := event.Subscribe(q, "UserCreated", sendWelcomeEmail); err != nil {
		log.Fatal(err)
	}

	if err := q.Start(); err != nil {
		log.Fatal(err)
	}

	created := event.New(
		event.NewInfo("user-1", "UserCreated"),
		UserCreated{Username: "alice", Email: "alice@example.com"},
	)
	if err := q.Publish(ctx, created); err != nil {
		log.Fatal(err)
	}
	if err := q.Stop(ctx); err != nil {
		log.Fatal(err)
	}
}

func sendWelcomeEmail(ctx context.Context, info event.BaseEventInfo, user UserCreated) error {
	slog.InfoContext(ctx, "send welcome email",
		slog.String("event_id", info.ID),
		slog.String("username", user.Username),
		slog.String("email", user.Email),
	)
	return nil
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

Register typed subscribers once:

```go
err := event.Subscribe(q, "UserCreated", func(ctx context.Context, info event.BaseEventInfo, user UserCreated) error {
	return nil
})
```

Register observers before or after start:

```go
q.AddObserver(event.ObserverFunc(func(ctx context.Context, result event.DispatchResult) {
	// result.Err is the handler error or recovered panic, if one occurred.
}))
```

Use async worker mode for background application events:

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

Use manual dispatch mode when an owner loop should decide when side effects run:

```go
if err := q.Publish(ctx, evt); err != nil {
	return err
}

dispatched, err := q.DispatchPending(ctx)
if err != nil {
	return err
}
_ = dispatched
```

For games, call `DispatchPending` at a known point in the frame, such as after simulation and before UI/audio updates. Do not call `Start` on queues that are manually dispatched.

`Stop` closes the queue to new publishes, drains queued events, and waits for dispatch to finish. `Publish` returns `ErrClosed`, `ErrNilEvent`, or the context error when appropriate. `DispatchPending` returns `ErrAsyncStarted` if the async worker has already been started.

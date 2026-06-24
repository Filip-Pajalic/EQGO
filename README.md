# EQGO

EQGO is a small in-process event queue for Go. It keeps payload handlers type-safe with generics while exposing one simple queue for mixed event types.

It is meant for lightweight application events, demos, and local workflows. It is not a distributed broker, persistent queue, or retry system.

## Features

- Type-safe event payloads with generic handlers
- Context-aware publishing
- Safe `Start` / `Stop` lifecycle with queue draining
- Global hooks for auditing, metrics, and error reporting
- Handler error reporting without killing the queue worker
- Panic recovery around event handlers and hooks
- No external dependencies

## Requirements

- Go 1.26.4 or newer

## Example

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/Filip-Pajalic/EQGO/event"
)

type UserCreated struct {
	Username string
	Email    string
}

func main() {
	ctx := context.Background()
	q := event.NewQueue(32)
	q.AddGlobalHook(event.AuditLogger)

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
```

## API Shape

Create a queue:

```go
q := event.NewQueue(64)
```

Register hooks before or after start:

```go
q.AddGlobalHook(func(ctx context.Context, info event.BaseEventInfo, payload any, err error) {
	// err is the handler error or recovered panic, if one occurred.
})
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

package event

import (
	"context"
	"errors"
	"slices"
	"sync"
)

var (
	// ErrNilEvent is returned when Publish receives a nil event.
	ErrNilEvent = errors.New("event queue: nil event")

	// ErrNotStarted is returned when Publish is called before Start.
	ErrNotStarted = errors.New("event queue: not started")

	// ErrClosed is returned when the queue has already been stopped.
	ErrClosed = errors.New("event queue: closed")
)

// ExecutableEvent is the common interface used by Queue for all payload types.
type ExecutableEvent interface {
	Dispatch(context.Context) error
	EventInfo() BaseEventInfo
	PayloadData() any
}

// Hook observes every dispatched event and its handler result.
type Hook func(context.Context, BaseEventInfo, any, error)

// Queue dispatches mixed event types through one serial in-process worker.
type Queue struct {
	events  chan ExecutableEvent
	done    chan struct{}
	stopped chan struct{}

	stateMu sync.Mutex
	state   *sync.Cond
	started bool
	closed  bool
	active  int

	hooksMu sync.RWMutex
	hooks   []Hook

	wg sync.WaitGroup
}

// NewQueue creates a queue with the given event buffer size.
func NewQueue(buffer int) *Queue {
	q := &Queue{
		events:  make(chan ExecutableEvent, max(buffer, 0)),
		done:    make(chan struct{}),
		stopped: make(chan struct{}),
	}
	q.state = sync.NewCond(&q.stateMu)
	return q
}

// Start starts the queue worker. Calling Start more than once is a no-op.
func (q *Queue) Start() error {
	q.stateMu.Lock()
	defer q.stateMu.Unlock()

	if q.closed {
		return ErrClosed
	}
	if q.started {
		return nil
	}

	q.started = true
	q.wg.Go(func() {
		defer close(q.stopped)
		q.run()
	})
	return nil
}

// Publish queues an event or returns when the context is canceled or the queue closes.
func (q *Queue) Publish(ctx context.Context, e ExecutableEvent) error {
	if e == nil {
		return ErrNilEvent
	}
	if ctx == nil {
		ctx = context.Background()
	}

	q.stateMu.Lock()
	if !q.started {
		q.stateMu.Unlock()
		return ErrNotStarted
	}
	if q.closed {
		q.stateMu.Unlock()
		return ErrClosed
	}
	q.active++
	q.stateMu.Unlock()
	defer q.finishPublish()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-q.done:
		return ErrClosed
	case q.events <- e:
		return nil
	}
}

// Stop closes the queue, unblocks pending publishes, drains queued events, and waits for the worker.
func (q *Queue) Stop(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	q.stateMu.Lock()
	wasStarted := q.started
	if !q.closed {
		q.closed = true
		close(q.done)
		q.state.Broadcast()
	}
	q.stateMu.Unlock()

	if !wasStarted {
		return nil
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-q.stopped:
		return nil
	}
}

// AddGlobalHook registers a hook that runs after each event dispatch.
func (q *Queue) AddGlobalHook(h Hook) {
	if h == nil {
		return
	}

	q.hooksMu.Lock()
	defer q.hooksMu.Unlock()
	q.hooks = append(q.hooks, h)
}

func (q *Queue) run() {
	for {
		select {
		case e := <-q.events:
			q.dispatch(context.Background(), e)
		case <-q.done:
			q.waitForPublishers()
			q.drain()
			return
		}
	}
}

func (q *Queue) finishPublish() {
	q.stateMu.Lock()
	defer q.stateMu.Unlock()
	q.active--
	q.state.Broadcast()
}

func (q *Queue) waitForPublishers() {
	q.stateMu.Lock()
	defer q.stateMu.Unlock()
	for q.active > 0 {
		q.state.Wait()
	}
}

func (q *Queue) drain() {
	for {
		select {
		case e := <-q.events:
			q.dispatch(context.Background(), e)
		default:
			return
		}
	}
}

func (q *Queue) dispatch(ctx context.Context, e ExecutableEvent) {
	err := e.Dispatch(ctx)

	q.hooksMu.RLock()
	hooks := slices.Clone(q.hooks)
	q.hooksMu.RUnlock()

	for _, h := range hooks {
		func() {
			defer func() {
				_ = recover()
			}()
			h(ctx, e.EventInfo(), e.PayloadData(), err)
		}()
	}
}

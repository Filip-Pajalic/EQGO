package event

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
)

var (
	// ErrNilEvent is returned when Publish receives a nil event.
	ErrNilEvent = errors.New("event queue: nil event")

	// ErrClosed is returned when the queue has already been stopped.
	ErrClosed = errors.New("event queue: closed")

	// ErrAsyncStarted is returned when manual dispatch is used after Start.
	ErrAsyncStarted = errors.New("event queue: async worker started")

	// ErrNilHandler is returned when Subscribe receives a nil handler.
	ErrNilHandler = errors.New("event queue: nil handler")
)

// DispatchResult is the outcome observed after an event is dispatched.
type DispatchResult struct {
	Info    BaseEventInfo
	Payload any
	Err     error
}

// Observer receives the result of each dispatched event.
type Observer interface {
	ObserveEvent(context.Context, DispatchResult)
}

// ObserverFunc adapts a function into an Observer.
type ObserverFunc func(context.Context, DispatchResult)

// ObserveEvent calls f with the dispatch result.
func (f ObserverFunc) ObserveEvent(ctx context.Context, result DispatchResult) {
	if f != nil {
		f(ctx, result)
	}
}

// Queue dispatches mixed event types through either manual drains or one async worker.
type Queue struct {
	events  chan Event
	done    chan struct{}
	stopped chan struct{}

	stateMu sync.Mutex
	state   *sync.Cond
	started bool
	closed  bool
	active  int

	subscribersMu sync.RWMutex
	subscribers   map[string][]subscriber

	dispatchMu sync.Mutex

	observersMu sync.RWMutex
	observers   []Observer

	wg sync.WaitGroup
}

// NewQueue creates a queue with the given event buffer size.
func NewQueue(buffer int) *Queue {
	q := &Queue{
		events:      make(chan Event, max(buffer, 0)),
		done:        make(chan struct{}),
		stopped:     make(chan struct{}),
		subscribers: make(map[string][]subscriber),
	}
	q.state = sync.NewCond(&q.stateMu)
	return q
}

// Subscribe registers a typed handler for events with eventName.
func Subscribe[T any](q *Queue, eventName string, h Handler[T]) error {
	if h == nil {
		return ErrNilHandler
	}

	q.subscribersMu.Lock()
	defer q.subscribersMu.Unlock()
	q.subscribers[eventName] = append(q.subscribers[eventName], typedSubscriber[T]{handler: h})
	return nil
}

// Start starts the async queue worker. Calling Start more than once is a no-op.
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
// Queued events are dispatched by Start's worker or by DispatchPending.
func (q *Queue) Publish(ctx context.Context, e Event) error {
	if e == nil {
		return ErrNilEvent
	}
	if ctx == nil {
		ctx = context.Background()
	}

	q.stateMu.Lock()
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

// DispatchPending synchronously dispatches queued events without starting a worker.
// Use this from a game loop or another owner-controlled dispatch point.
func (q *Queue) DispatchPending(ctx context.Context) (int, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	q.stateMu.Lock()
	if q.closed {
		q.stateMu.Unlock()
		return 0, ErrClosed
	}
	if q.started {
		q.stateMu.Unlock()
		return 0, ErrAsyncStarted
	}
	q.stateMu.Unlock()

	dispatched := 0
	q.dispatchMu.Lock()
	defer q.dispatchMu.Unlock()

	for {
		select {
		case <-ctx.Done():
			return dispatched, ctx.Err()
		case e := <-q.events:
			q.dispatch(ctx, e)
			dispatched++
		default:
			return dispatched, nil
		}
	}
}

// Stop closes the queue, unblocks pending publishes, drains queued events, and waits for dispatch to finish.
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
		q.waitForPublishers()
		q.drainWithContext(ctx)
		return ctx.Err()
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-q.stopped:
		return nil
	}
}

// AddObserver registers an observer that runs after each event dispatch.
func (q *Queue) AddObserver(observer Observer) {
	if observer == nil {
		return
	}

	q.observersMu.Lock()
	defer q.observersMu.Unlock()
	q.observers = append(q.observers, observer)
}

func (q *Queue) run() {
	for {
		select {
		case e := <-q.events:
			q.dispatchMu.Lock()
			q.dispatch(context.Background(), e)
			q.dispatchMu.Unlock()
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
	q.dispatchMu.Lock()
	defer q.dispatchMu.Unlock()

	for {
		select {
		case e := <-q.events:
			q.dispatch(context.Background(), e)
		default:
			return
		}
	}
}

func (q *Queue) drainWithContext(ctx context.Context) {
	q.dispatchMu.Lock()
	defer q.dispatchMu.Unlock()

	for {
		select {
		case <-ctx.Done():
			return
		case e := <-q.events:
			q.dispatch(ctx, e)
		default:
			return
		}
	}
}

func (q *Queue) dispatch(ctx context.Context, e Event) {
	err := q.dispatchToSubscribers(ctx, e)
	result := DispatchResult{
		Info:    e.EventInfo(),
		Payload: e.PayloadData(),
		Err:     err,
	}

	q.observersMu.RLock()
	observers := slices.Clone(q.observers)
	q.observersMu.RUnlock()

	for _, observer := range observers {
		func() {
			defer func() {
				_ = recover()
			}()
			observer.ObserveEvent(ctx, result)
		}()
	}
}

func (q *Queue) dispatchToSubscribers(ctx context.Context, e Event) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("event %q handler panicked: %v", e.EventInfo().Name, recovered)
		}
	}()

	q.subscribersMu.RLock()
	subscribers := slices.Clone(q.subscribers[e.EventInfo().Name])
	q.subscribersMu.RUnlock()

	for _, subscriber := range subscribers {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := subscriber.dispatch(ctx, e); err != nil {
			return err
		}
	}
	return nil
}

type subscriber interface {
	dispatch(context.Context, Event) error
}

type typedSubscriber[T any] struct {
	handler Handler[T]
}

func (s typedSubscriber[T]) dispatch(ctx context.Context, e Event) error {
	payload, ok := e.PayloadData().(T)
	if !ok {
		var expected T
		return fmt.Errorf("event %q payload has type %T, handler expects %T", e.EventInfo().Name, e.PayloadData(), expected)
	}
	return s.handler(ctx, e.EventInfo(), payload)
}

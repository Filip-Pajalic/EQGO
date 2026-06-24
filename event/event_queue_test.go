package event

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestQueuePublishesTypedEventToSubscriberAndObserver(t *testing.T) {
	t.Parallel()

	q := NewQueue(1)
	observed := make(chan struct{})
	var handled atomic.Int64

	if err := Subscribe(q, "NumberAccepted", func(_ context.Context, info BaseEventInfo, n int) error {
		if info.ID != "number-1" {
			t.Errorf("handler saw event ID %q, want number-1", info.ID)
		}
		handled.Store(int64(n))
		return nil
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	q.AddObserver(ObserverFunc(func(_ context.Context, result DispatchResult) {
		defer close(observed)
		if result.Err != nil {
			t.Errorf("unexpected handler error: %v", result.Err)
		}
		if result.Info.ID != "number-1" || result.Info.Name != "NumberAccepted" {
			t.Errorf("unexpected event info: %+v", result.Info)
		}
		if got, ok := result.Payload.(int); !ok || got != 42 {
			t.Errorf("unexpected payload: %#v", result.Payload)
		}
	}))

	if err := q.Start(); err != nil {
		t.Fatalf("start queue: %v", err)
	}
	t.Cleanup(func() {
		if err := q.Stop(context.Background()); err != nil {
			t.Fatalf("stop queue: %v", err)
		}
	})

	evt := New(NewInfo("number-1", "NumberAccepted"), 42)
	if err := q.Publish(context.Background(), evt); err != nil {
		t.Fatalf("publish event: %v", err)
	}

	select {
	case <-observed:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for observer")
	}

	if got := handled.Load(); got != 42 {
		t.Fatalf("handler saw %d, want 42", got)
	}
}

func TestQueueLifecycleErrors(t *testing.T) {
	t.Parallel()

	q := NewQueue(1)

	if err := q.Publish(context.Background(), nil); !errors.Is(err, ErrNilEvent) {
		t.Fatalf("nil event error = %v, want %v", err, ErrNilEvent)
	}
	if err := Subscribe[string](q, "Name", nil); !errors.Is(err, ErrNilHandler) {
		t.Fatalf("nil handler error = %v, want %v", err, ErrNilHandler)
	}
	if err := q.Stop(context.Background()); err != nil {
		t.Fatalf("stop unopened queue: %v", err)
	}
	if err := q.Start(); !errors.Is(err, ErrClosed) {
		t.Fatalf("start after stop error = %v, want %v", err, ErrClosed)
	}
}

func TestDispatchPendingProcessesEventsWithoutAsyncWorker(t *testing.T) {
	t.Parallel()

	q := NewQueue(4)
	var handled atomic.Int64

	if err := Subscribe(q, "Counted", func(context.Context, BaseEventInfo, int) error {
		handled.Add(1)
		return nil
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	for i := range 3 {
		if err := q.Publish(context.Background(), New(NewInfo("id", "Counted"), i)); err != nil {
			t.Fatalf("publish event %d: %v", i, err)
		}
	}

	dispatched, err := q.DispatchPending(context.Background())
	if err != nil {
		t.Fatalf("dispatch pending: %v", err)
	}
	if dispatched != 3 {
		t.Fatalf("dispatched %d events, want 3", dispatched)
	}
	if got := handled.Load(); got != 3 {
		t.Fatalf("handled %d events, want 3", got)
	}
}

func TestDispatchPendingRejectsAsyncWorkerMode(t *testing.T) {
	t.Parallel()

	q := NewQueue(1)
	if err := q.Start(); err != nil {
		t.Fatalf("start queue: %v", err)
	}
	t.Cleanup(func() {
		if err := q.Stop(context.Background()); err != nil {
			t.Fatalf("stop queue: %v", err)
		}
	})

	if _, err := q.DispatchPending(context.Background()); !errors.Is(err, ErrAsyncStarted) {
		t.Fatalf("dispatch pending error = %v, want %v", err, ErrAsyncStarted)
	}
}

func TestStopDrainsManualQueue(t *testing.T) {
	t.Parallel()

	q := NewQueue(2)
	var handled atomic.Int64

	if err := Subscribe(q, "Counted", func(context.Context, BaseEventInfo, int) error {
		handled.Add(1)
		return nil
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	for i := range 2 {
		if err := q.Publish(context.Background(), New(NewInfo("id", "Counted"), i)); err != nil {
			t.Fatalf("publish event %d: %v", i, err)
		}
	}
	if err := q.Stop(context.Background()); err != nil {
		t.Fatalf("stop queue: %v", err)
	}
	if got := handled.Load(); got != 2 {
		t.Fatalf("handled %d events, want 2", got)
	}
}

func TestStopDrainsQueuedEventsAndClosesQueue(t *testing.T) {
	t.Parallel()

	q := NewQueue(4)
	var handled atomic.Int64

	if err := Subscribe(q, "Counted", func(context.Context, BaseEventInfo, int) error {
		handled.Add(1)
		return nil
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	if err := q.Start(); err != nil {
		t.Fatalf("start queue: %v", err)
	}

	for i := range 4 {
		if err := q.Publish(context.Background(), New(NewInfo("id", "Counted"), i)); err != nil {
			t.Fatalf("publish event %d: %v", i, err)
		}
	}
	if err := q.Stop(context.Background()); err != nil {
		t.Fatalf("stop queue: %v", err)
	}
	if got := handled.Load(); got != 4 {
		t.Fatalf("handled %d events, want 4", got)
	}
	if err := q.Publish(context.Background(), New(NewInfo("late", "Late"), 1)); !errors.Is(err, ErrClosed) {
		t.Fatalf("publish after stop error = %v, want %v", err, ErrClosed)
	}
}

func TestHandlerErrorIsReportedAndQueueContinues(t *testing.T) {
	t.Parallel()

	q := NewQueue(2)
	wantErr := errors.New("handler failed")
	results := make(chan error, 2)

	if err := Subscribe(q, "Bad", func(context.Context, BaseEventInfo, int) error {
		return wantErr
	}); err != nil {
		t.Fatalf("subscribe bad handler: %v", err)
	}
	if err := Subscribe(q, "Good", func(context.Context, BaseEventInfo, int) error {
		return nil
	}); err != nil {
		t.Fatalf("subscribe good handler: %v", err)
	}

	q.AddObserver(ObserverFunc(func(_ context.Context, result DispatchResult) {
		results <- result.Err
	}))

	if err := q.Start(); err != nil {
		t.Fatalf("start queue: %v", err)
	}
	t.Cleanup(func() {
		if err := q.Stop(context.Background()); err != nil {
			t.Fatalf("stop queue: %v", err)
		}
	})

	if err := q.Publish(context.Background(), New(NewInfo("bad", "Bad"), 1)); err != nil {
		t.Fatalf("publish bad event: %v", err)
	}
	if err := q.Publish(context.Background(), New(NewInfo("good", "Good"), 2)); err != nil {
		t.Fatalf("publish good event: %v", err)
	}

	if err := receiveError(t, results); !errors.Is(err, wantErr) {
		t.Fatalf("first observer error = %v, want %v", err, wantErr)
	}
	if err := receiveError(t, results); err != nil {
		t.Fatalf("second observer error = %v, want nil", err)
	}
}

func TestHandlerPanicIsRecoveredAndQueueContinues(t *testing.T) {
	t.Parallel()

	q := NewQueue(2)
	results := make(chan error, 2)
	var handled atomic.Bool

	if err := Subscribe(q, "Panic", func(context.Context, BaseEventInfo, int) error {
		panic("boom")
	}); err != nil {
		t.Fatalf("subscribe panic handler: %v", err)
	}
	if err := Subscribe(q, "Good", func(context.Context, BaseEventInfo, int) error {
		handled.Store(true)
		return nil
	}); err != nil {
		t.Fatalf("subscribe good handler: %v", err)
	}

	q.AddObserver(ObserverFunc(func(_ context.Context, result DispatchResult) {
		results <- result.Err
	}))

	if err := q.Start(); err != nil {
		t.Fatalf("start queue: %v", err)
	}
	t.Cleanup(func() {
		if err := q.Stop(context.Background()); err != nil {
			t.Fatalf("stop queue: %v", err)
		}
	})

	if err := q.Publish(context.Background(), New(NewInfo("panic", "Panic"), 1)); err != nil {
		t.Fatalf("publish panic event: %v", err)
	}
	if err := q.Publish(context.Background(), New(NewInfo("good", "Good"), 2)); err != nil {
		t.Fatalf("publish good event: %v", err)
	}

	if err := receiveError(t, results); err == nil {
		t.Fatal("first observer error = nil, want panic error")
	}
	if err := receiveError(t, results); err != nil {
		t.Fatalf("second observer error = %v, want nil", err)
	}
	if !handled.Load() {
		t.Fatal("queue did not continue after panic")
	}
}

func TestPayloadTypeMismatchIsReported(t *testing.T) {
	t.Parallel()

	q := NewQueue(1)
	results := make(chan error, 1)

	if err := Subscribe(q, "WrongPayload", func(context.Context, BaseEventInfo, int) error {
		t.Fatal("handler should not run for wrong payload type")
		return nil
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	q.AddObserver(ObserverFunc(func(_ context.Context, result DispatchResult) {
		results <- result.Err
	}))

	if err := q.Start(); err != nil {
		t.Fatalf("start queue: %v", err)
	}
	t.Cleanup(func() {
		if err := q.Stop(context.Background()); err != nil {
			t.Fatalf("stop queue: %v", err)
		}
	})

	if err := q.Publish(context.Background(), New(NewInfo("wrong", "WrongPayload"), "not an int")); err != nil {
		t.Fatalf("publish event: %v", err)
	}
	if err := receiveError(t, results); err == nil {
		t.Fatal("observer error = nil, want payload type error")
	}
}

func TestPublishReturnsContextErrorWhenQueueIsFull(t *testing.T) {
	t.Parallel()

	q := NewQueue(0)
	release := make(chan struct{})

	if err := Subscribe(q, "Blocker", func(context.Context, BaseEventInfo, int) error {
		<-release
		return nil
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	if err := q.Start(); err != nil {
		t.Fatalf("start queue: %v", err)
	}
	t.Cleanup(func() {
		close(release)
		if err := q.Stop(context.Background()); err != nil {
			t.Fatalf("stop queue: %v", err)
		}
	})

	if err := q.Publish(context.Background(), New(NewInfo("blocker", "Blocker"), 1)); err != nil {
		t.Fatalf("publish blocker: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	err := q.Publish(ctx, New(NewInfo("blocked", "Blocked"), 2))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("publish full queue error = %v, want %v", err, context.DeadlineExceeded)
	}
}

func TestStopUnblocksPendingPublishAndDrainsInFlightSends(t *testing.T) {
	t.Parallel()

	q := NewQueue(0)
	release := make(chan struct{})

	if err := Subscribe(q, "Blocker", func(context.Context, BaseEventInfo, int) error {
		<-release
		return nil
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	if err := q.Start(); err != nil {
		t.Fatalf("start queue: %v", err)
	}

	if err := q.Publish(context.Background(), New(NewInfo("blocker", "Blocker"), 1)); err != nil {
		t.Fatalf("publish blocker: %v", err)
	}

	publishDone := make(chan error, 1)
	go func() {
		publishDone <- q.Publish(context.Background(), New(NewInfo("blocked", "Blocked"), 2))
	}()

	stopDone := make(chan error, 1)
	go func() {
		stopDone <- q.Stop(context.Background())
	}()

	select {
	case err := <-publishDone:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("blocked publish error = %v, want %v", err, ErrClosed)
		}
	case <-time.After(time.Second):
		t.Fatal("stop did not unblock pending publish")
	}

	close(release)

	select {
	case err := <-stopDone:
		if err != nil {
			t.Fatalf("stop queue: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for stop")
	}
}

func receiveError(t *testing.T, ch <-chan error) error {
	t.Helper()

	select {
	case err := <-ch:
		return err
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for observer")
		return nil
	}
}

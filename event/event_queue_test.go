package event

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestQueuePublishesTypedEventAndNotifiesObserver(t *testing.T) {
	t.Parallel()

	q := NewQueue(1)
	observed := make(chan struct{})
	var handled atomic.Int64

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

	evt := New(NewInfo("number-1", "NumberAccepted"), 42, func(_ context.Context, n int) error {
		handled.Store(int64(n))
		return nil
	})

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
	evt := New(NewInfo("id", "Name"), "payload")

	if err := q.Publish(context.Background(), evt); !errors.Is(err, ErrNotStarted) {
		t.Fatalf("publish before start error = %v, want %v", err, ErrNotStarted)
	}
	if err := q.Publish(context.Background(), nil); !errors.Is(err, ErrNilEvent) {
		t.Fatalf("nil event error = %v, want %v", err, ErrNilEvent)
	}
	if err := q.Stop(context.Background()); err != nil {
		t.Fatalf("stop unopened queue: %v", err)
	}
	if err := q.Start(); !errors.Is(err, ErrClosed) {
		t.Fatalf("start after stop error = %v, want %v", err, ErrClosed)
	}
}

func TestStopDrainsQueuedEventsAndClosesQueue(t *testing.T) {
	t.Parallel()

	q := NewQueue(4)
	var handled atomic.Int64
	handler := func(context.Context, int) error {
		handled.Add(1)
		return nil
	}

	if err := q.Start(); err != nil {
		t.Fatalf("start queue: %v", err)
	}

	for i := range 4 {
		if err := q.Publish(context.Background(), New(NewInfo("id", "Counted"), i, handler)); err != nil {
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

	bad := New(NewInfo("bad", "Bad"), 1, func(context.Context, int) error {
		return wantErr
	})
	good := New(NewInfo("good", "Good"), 2, func(context.Context, int) error {
		return nil
	})

	if err := q.Publish(context.Background(), bad); err != nil {
		t.Fatalf("publish bad event: %v", err)
	}
	if err := q.Publish(context.Background(), good); err != nil {
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

	panicEvent := New(NewInfo("panic", "Panic"), 1, func(context.Context, int) error {
		panic("boom")
	})
	good := New(NewInfo("good", "Good"), 2, func(context.Context, int) error {
		handled.Store(true)
		return nil
	})

	if err := q.Publish(context.Background(), panicEvent); err != nil {
		t.Fatalf("publish panic event: %v", err)
	}
	if err := q.Publish(context.Background(), good); err != nil {
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

func TestPublishReturnsContextErrorWhenQueueIsFull(t *testing.T) {
	t.Parallel()

	q := NewQueue(0)
	release := make(chan struct{})
	blocker := New(NewInfo("blocker", "Blocker"), 1, func(context.Context, int) error {
		<-release
		return nil
	})

	if err := q.Start(); err != nil {
		t.Fatalf("start queue: %v", err)
	}
	t.Cleanup(func() {
		close(release)
		if err := q.Stop(context.Background()); err != nil {
			t.Fatalf("stop queue: %v", err)
		}
	})

	if err := q.Publish(context.Background(), blocker); err != nil {
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
	blocker := New(NewInfo("blocker", "Blocker"), 1, func(context.Context, int) error {
		<-release
		return nil
	})

	if err := q.Start(); err != nil {
		t.Fatalf("start queue: %v", err)
	}

	if err := q.Publish(context.Background(), blocker); err != nil {
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

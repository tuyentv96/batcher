package batcher

import (
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
)

type testClient[T any] struct {
	MaxBatchSize         uint
	BatchTimeout         time.Duration
	MaxConcurrentBatches uint
	PendingWorkCapacity  uint
	Fire                 func(items []T, notifier Notifier)
	batcher              Batcher[T]
}

func (c *testClient[T]) Start() error {
	c.batcher.maxBatchSize = c.MaxBatchSize
	c.batcher.batchTimeout = c.BatchTimeout
	c.batcher.MaxConcurrentBatches = c.MaxConcurrentBatches
	c.batcher.pendingWorkCapacity = c.PendingWorkCapacity
	c.batcher.batchMaker = func() Batch[T] { return &testBatch[T]{Client: c} }
	return c.batcher.Start()
}

func (c *testClient[T]) Stop() error {
	return c.batcher.Stop()
}

func (c *testClient[T]) Add(item T) {
	c.batcher.work <- item
}

type testBatch[T any] struct {
	Client *testClient[T]
	Items  []T
}

func (b *testBatch[T]) Add(item T) {
	b.Items = append(b.Items, item)
}

func (b *testBatch[T]) Fire(notifier Notifier) {
	b.Client.Fire(b.Items, notifier)
}

type fatal interface {
	Fatal(args ...interface{})
}

func errCall(t fatal, f func() error) {
	if err := f(); err != nil {
		t.Fatal(err)
	}
}

func expectFire(
	t *testing.T,
	finished chan struct{},
	expected [][]string,
) func(actual []string, notifier Notifier) {

	return func(actual []string, notifier Notifier) {
		defer notifier.Done()
		defer close(finished)
		for _, batch := range expected {
			if !reflect.DeepEqual(actual, batch) {
				t.Fatalf("expected %v\nactual %v", batch, actual)
			}
		}
	}
}

func addExpected(c *testClient[string], expected [][]string) {
	for _, b := range expected {
		for _, v := range b {
			c.Add(v)
		}
	}
}

func TestMaxBatch(t *testing.T) {
	t.Parallel()
	expected := [][]string{{"milk", "yogurt", "butter"}}
	finished := make(chan struct{})
	c := &testClient[string]{
		MaxBatchSize:        uint(len(expected[0][0])),
		BatchTimeout:        20 * time.Millisecond,
		Fire:                expectFire(t, finished, expected),
		PendingWorkCapacity: 100,
		batcher:             NewBatcher[string](),
	}
	errCall(t, c.Start)
	addExpected(c, expected)
	<-finished
}

func TestBatchTimeout(t *testing.T) {
	t.Parallel()
	expected := [][]string{{"milk", "yogurt"}}
	finished := make(chan struct{})
	c := &testClient[string]{
		MaxBatchSize:        3,
		BatchTimeout:        20 * time.Millisecond,
		Fire:                expectFire(t, finished, expected),
		PendingWorkCapacity: 100,
		batcher:             NewBatcher[string](),
	}
	errCall(t, c.Start)
	addExpected(c, expected)
	time.Sleep(30 * time.Millisecond)
	<-finished
}

func TestStop(t *testing.T) {
	t.Parallel()
	expected := [][]string{{"milk", "yogurt"}}
	finished := make(chan struct{})
	c := &testClient[string]{
		MaxBatchSize:        3,
		BatchTimeout:        time.Hour,
		Fire:                expectFire(t, finished, expected),
		PendingWorkCapacity: 100,
		batcher:             NewBatcher[string](),
	}
	errCall(t, c.Start)
	addExpected(c, expected)
	errCall(t, c.Stop)
	<-finished
}

func TestZeroMaxBatchSize(t *testing.T) {
	t.Parallel()
	expected := [][]string{{"milk", "yogurt"}}
	finished := make(chan struct{})
	c := &testClient[string]{
		BatchTimeout:        20 * time.Millisecond,
		Fire:                expectFire(t, finished, expected),
		PendingWorkCapacity: 100,
		batcher:             NewBatcher[string](),
	}
	errCall(t, c.Start)
	addExpected(c, expected)
	time.Sleep(30 * time.Millisecond)
	<-finished
}

func TestZeroBatchTimeout(t *testing.T) {
	t.Parallel()
	expected := [][]string{{"milk", "yogurt"}}
	finished := make(chan struct{})
	c := &testClient[string]{
		MaxBatchSize:        3,
		Fire:                expectFire(t, finished, expected),
		PendingWorkCapacity: 100,
		batcher:             NewBatcher[string](),
	}
	errCall(t, c.Start)
	addExpected(c, expected)
	time.Sleep(30 * time.Millisecond)
	select {
	case <-finished:
		t.Fatal("should not be finished yet")
	default:
	}
	errCall(t, c.Stop)
	<-finished
}

func TestZeroBoth(t *testing.T) {
	t.Parallel()
	c := &testClient[string]{}
	if c.Start() == nil {
		t.Fatal("was expecting error")
	}
}

func TestEmptyStop(t *testing.T) {
	t.Parallel()
	c := &testClient[string]{
		MaxBatchSize: 3,
		Fire: func(actual []string, notifier Notifier) {
			defer notifier.Done()
			t.Fatal("should not get called")
		},
		PendingWorkCapacity: 100,
		batcher:             NewBatcher[string](),
	}
	errCall(t, c.Start)
	errCall(t, c.Stop)
}

func TestContiniousSendWithTimeoutOnlyBlocking(t *testing.T) {
	t.Parallel()
	var fireTotal, addedTotal uint64
	c := &testClient[string]{
		BatchTimeout: 5 * time.Millisecond,
		Fire: func(actual []string, notifier Notifier) {
			defer notifier.Done()
			atomic.AddUint64(&fireTotal, uint64(len(actual)))
		},
		batcher: NewBatcher[string](),
	}
	klock := clock.NewMock()
	c.batcher.clock = klock
	errCall(t, c.Start)

	finished := make(chan struct{})
	go func() {
		defer close(finished)
		for {
			select {
			case <-finished:
				return
			case c.batcher.work <- "42":
				atomic.AddUint64(&addedTotal, 1)
			default:
			}
		}
	}()

	for i := 0; i < 3; i++ {
		klock.Add(c.BatchTimeout)
	}
	finished <- struct{}{}
	<-finished

	errCall(t, c.Stop)
	if fireTotal != addedTotal {
		t.Fatalf("fireTotal=%d VS addedTotal=%d", fireTotal, addedTotal)
	}
}

func TestContiniousSendWithTimeoutOnly(t *testing.T) {
	t.Parallel()
	var fireTotal, addedTotal uint64
	c := &testClient[string]{
		BatchTimeout: 5 * time.Millisecond,
		Fire: func(actual []string, notifier Notifier) {
			defer notifier.Done()
			atomic.AddUint64(&fireTotal, uint64(len(actual)))
		},
		PendingWorkCapacity: 100,
		batcher:             NewBatcher[string](),
	}
	klock := clock.NewMock()
	c.batcher.clock = klock
	errCall(t, c.Start)

	finished := make(chan struct{})
	go func() {
		defer close(finished)
		for {
			select {
			case <-finished:
				return
			case c.batcher.work <- "42":
				atomic.AddUint64(&addedTotal, 1)
			default:
			}
		}
	}()

	for i := 0; i < 3; i++ {
		klock.Add(c.BatchTimeout)
	}
	finished <- struct{}{}
	<-finished

	errCall(t, c.Stop)
	if fireTotal != addedTotal {
		t.Fatalf("fireTotal=%d VS addedTotal=%d", fireTotal, addedTotal)
	}
}

func TestMaxConcurrentBatches(t *testing.T) {
	t.Parallel()
	expected := [][]string{{"milk", "yogurt"}}
	gotMilk := make(chan struct{})
	gotYogurt := make(chan struct{})
	c := &testClient[string]{
		MaxBatchSize:         1,
		MaxConcurrentBatches: 1,
		Fire: func(items []string, notifier Notifier) {
			defer notifier.Done()
			if len(items) == 1 && items[0] == "milk" {
				gotMilk <- struct{}{}
				return
			}
			if len(items) == 1 && items[0] == "yogurt" {
				gotYogurt <- struct{}{}
				return
			}
			t.Fatalf("unexpected items: %v", items)
		},
		batcher: NewBatcher[string](),
	}
	errCall(t, c.Start)
	addExpected(c, expected)
	select {
	case c.batcher.work <- "never sent":
		t.Fatal("should not get here")
	case <-time.After(1 * time.Millisecond):
	}
	<-gotMilk
	<-gotYogurt
	errCall(t, c.Stop)
}

func BenchmarkFlow(b *testing.B) {
	c := &testClient[string]{
		MaxBatchSize:        3,
		BatchTimeout:        time.Hour,
		PendingWorkCapacity: 100,
		Fire: func(actual []string, notifier Notifier) {
			notifier.Done()
		},
		batcher: NewBatcher[string](),
	}
	errCall(b, c.Start)
	for i := 0; i < b.N; i++ {
		c.Add("42")
	}
	errCall(b, c.Stop)
}

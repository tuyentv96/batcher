package batcher

import (
	"errors"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
)

var errZeroBoth = errors.New(
	"batcher: MaxBatchSize and BatchTimeout can't both be zero",
)

// Notifier is used to indicate to the Client when a batch has finished
// processing.
type Notifier interface {
	// Calling Done will indicate the batch has finished processing.
	Done()
}

// Batch collects added items. Fire will be called exactly once. The Batch does
// not need to be safe for concurrent access; synchronization will be handled
// by the Client.
type Batch[T any] interface {
	// This should add the given single item to the Batch. This is the "other
	// end" of the Client.Work channel where your application will send items.
	Add(item T)

	// Fire off the Batch. It should call Notifier.Done() when it has finished
	// processing the Batch.
	Fire(notifier Notifier)
}

// The Batcher manages the background process that makes, populates & fires
// Batches.
type Batcher[T any] struct {
	// Maximum number of items in a batch. If this is zero batches will only be
	// dispatched upon hitting the BatchTimeout. It is an error for both this and
	// the BatchTimeout to be zero.
	maxBatchSize uint

	// Duration after which to send a pending batch. If this is zero batches will
	// only be dispatched upon hitting the MaxBatchSize. It is an error for both
	// this and the MaxBatchSize to be zero.
	batchTimeout time.Duration

	// Capacity of work channel. If this is zero, the Work channel will be
	// blocking.
	pendingWorkCapacity uint

	// This function should create a new empty Batch on each invocation.
	batchMaker func() Batch[T]

	// Once this Client has been started, send work items here to add to batch.
	work chan T

	workGroup *sync.WaitGroup

	clock clock.Clock
}

func withMaxBatchSize[T any](maxbatchSize uint) func(*Batcher[T]) {
	return func(b *Batcher[T]) {
		b.maxBatchSize = maxbatchSize
	}
}

func withBatchTimeout[T any](timeout time.Duration) func(*Batcher[T]) {
	return func(b *Batcher[T]) {
		b.batchTimeout = timeout
	}
}

func withPendingWorkCapacity[T any](capacity uint) func(*Batcher[T]) {
	return func(b *Batcher[T]) {
		b.pendingWorkCapacity = capacity
	}
}

func NewBatcher[T any](opts ...func(*Batcher[T])) Batcher[T] {
	b := Batcher[T]{
		clock: clock.New(),
	}

	for _, opt := range opts {
		opt(&b)
	}

	return b
}

// Start the background worker goroutines and get ready for accepting requests.
func (c *Batcher[T]) Start() error {
	if int64(c.batchTimeout) == 0 && c.maxBatchSize == 0 {
		return errZeroBoth
	}

	c.workGroup = &sync.WaitGroup{}
	c.work = make(chan T, c.pendingWorkCapacity)
	c.workGroup.Add(1) // this is the worker itself
	go c.worker()
	return nil
}

// Stop gracefully and return once all processing has finished.
func (c *Batcher[T]) Stop() error {
	close(c.work)
	c.workGroup.Wait()
	return nil
}

// Background process.
func (c *Batcher[T]) worker() {
	defer c.workGroup.Done()
	var batch = c.batchMaker()
	var count uint
	var batchTimer *clock.Ticker
	var batchTimeout <-chan time.Time
	send := func() {
		c.workGroup.Add(1)
		go batch.Fire(c.workGroup)
		batch = c.batchMaker()
		count = 0
		if batchTimer != nil {
			batchTimer.Stop()
		}
	}
	recv := func(item T, open bool) bool {
		if !open {
			if count != 0 {
				send()
			}
			return true
		}
		batch.Add(item)
		count++
		if c.maxBatchSize != 0 && count >= c.maxBatchSize {
			send()
		} else if int64(c.batchTimeout) != 0 && count == 1 {
			batchTimer = c.clock.Ticker(c.batchTimeout)
			batchTimeout = batchTimer.C
		}
		return false
	}
	for {
		// We use two selects in order to first prefer draining the work queue.
		select {
		case item, ok := <-c.work:
			if recv(item, ok) {
				return
			}
		default:
			select {
			case item, ok := <-c.work:
				if recv(item, ok) {
					return
				}
			case <-batchTimeout:
				send()
			}
		}
	}
}

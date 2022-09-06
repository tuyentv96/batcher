package batcher

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

var errZeroBoth = errors.New(
	"muster: MaxBatchSize and BatchTimeout can't both be zero",
)

type waitGroup interface {
	Add(delta int)
	Done()
	Wait()
}

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
	MaxBatchSize uint

	// Duration after which to send a pending batch. If this is zero batches will
	// only be dispatched upon hitting the MaxBatchSize. It is an error for both
	// this and the MaxBatchSize to be zero.
	BatchTimeout time.Duration

	// MaxConcurrentBatches determines how many parallel batches we'll allow to
	// be "in flight" concurrently. Once these many batches are in flight, the
	// PendingWorkCapacity determines when sending to the Work channel will start
	// blocking. In other words, once MaxConcurrentBatches hits, the system
	// starts blocking. This allows for tighter control over memory utilization.
	// If not set, the number of parallel batches in-flight will not be limited.
	MaxConcurrentBatches uint

	// Capacity of work channel. If this is zero, the Work channel will be
	// blocking.
	PendingWorkCapacity uint

	// This function should create a new empty Batch on each invocation.
	BatchMaker func() Batch[T]

	// Once this Client has been started, send work items here to add to batch.
	Work chan T

	workGroup waitGroup
}

// Start the background worker goroutines and get ready for accepting requests.
func (c *Batcher[T]) Start() error {
	if int64(c.BatchTimeout) == 0 && c.MaxBatchSize == 0 {
		return errZeroBoth
	}

	c.workGroup = &sync.WaitGroup{}
	c.Work = make(chan T, c.PendingWorkCapacity)
	c.workGroup.Add(1) // this is the worker itself
	go c.worker()
	return nil
}

// Stop gracefully and return once all processing has finished.
func (c *Batcher[T]) Stop() error {
	close(c.Work)
	c.workGroup.Wait()
	return nil
}

// Background process.
func (c *Batcher[T]) worker() {
	defer c.workGroup.Done()
	var batch = c.BatchMaker()
	var count uint
	var batchTimer *time.Ticker
	var batchTimeout <-chan time.Time
	send := func() {
		c.workGroup.Add(1)
		go batch.Fire(c.workGroup)
		batch = c.BatchMaker()
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
		if c.MaxBatchSize != 0 && count >= c.MaxBatchSize {
			send()
		} else if int64(c.BatchTimeout) != 0 && count == 1 {
			batchTimer = time.NewTicker(c.BatchTimeout)
			batchTimeout = batchTimer.C
		}
		return false
	}
	for {
		// We use two selects in order to first prefer draining the work queue.
		select {
		case item, open := <-c.Work:
			if recv(item, open) {
				return
			}
		default:
			select {
			case item, open := <-c.Work:
				if recv(item, open) {
					return
				}
			case <-batchTimeout:
				send()
			}
		}
	}
}

// The ShoppingClient manages the shopping list and dispatches shoppers.
type ShoppingClient struct {
	MaxBatchSize        uint          // How much a shopper can carry at a time.
	BatchTimeout        time.Duration // How long we wait once we need to get something.
	PendingWorkCapacity uint          // How long our shopping list can be.
	batcher             Batcher[string]
}

// The ShoppingClient has to be started in order to initialize the underlying
// work channel as well as the background goroutine that handles the work.
func (s *ShoppingClient) Start() error {
	s.batcher.MaxBatchSize = s.MaxBatchSize
	s.batcher.BatchTimeout = s.BatchTimeout
	s.batcher.PendingWorkCapacity = s.PendingWorkCapacity
	s.batcher.BatchMaker = func() Batch[string] { return &batch[string]{Client: s} }

	return s.batcher.Start()
}

// Similarly the ShoppingClient has to be stopped in order to ensure we flush
// pending items and wait for in progress batches.
func (s *ShoppingClient) Stop() error {
	return s.batcher.Stop()
}

// The ShoppingClient provides a typed Add method which enqueues the work.
func (s *ShoppingClient) Add(item string) {
	s.batcher.Work <- item
}

// The batch is the collection of items that will be dispatched together.
type batch[T any] struct {
	Client *ShoppingClient
	Items  []T
}

// The batch provides an untyped Add to satisfy the muster.Batch interface. As
// is the case here, the Batch implementation is internal to the user of muster
// and not exposed to the users of ShoppingClient.
func (b *batch[T]) Add(item T) {
	b.Items = append(b.Items, item)
}

// Once a Batch is ready, it will be Fired. It must call notifier.Done once the
// batch has been processed.
func (b *batch[T]) Fire(notifier Notifier) {
	fmt.Println("Delivery", b.Items)
}

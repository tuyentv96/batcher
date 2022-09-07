package batcher

import (
	"fmt"
	"time"
)

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
	s.batcher.maxBatchSize = s.MaxBatchSize
	s.batcher.batchTimeout = s.BatchTimeout
	s.batcher.pendingWorkCapacity = s.PendingWorkCapacity
	s.batcher.batchMaker = func() Batch[string] { return &batch[string]{Client: s} }

	return s.batcher.Start()
}

// Similarly the ShoppingClient has to be stopped in order to ensure we flush
// pending items and wait for in progress batches.
func (s *ShoppingClient) Stop() error {
	return s.batcher.Stop()
}

// The ShoppingClient provides a typed Add method which enqueues the work.
func (s *ShoppingClient) Add(item string) {
	s.batcher.work <- item
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

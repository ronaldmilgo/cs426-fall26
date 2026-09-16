package lab0

import (
	"container/list"
	"context"
	"sync"
)

// Semaphore mirrors Go's library package `semaphore.Weighted`
// but with a smaller, simpler interface.
//
// Recall that a counting semaphore has two jobs:
//   - keep track of a number of available resources
//   - when resources are depleted, block waiters and resume them
//     when resources are available
type Semaphore struct {
	mu      sync.Mutex
	value   int
	waiters list.List
}

func NewSemaphore() *Semaphore {
	return &Semaphore{}
}

// Post increments the semaphore value by one. If there are any
// callers waiting, it signals exactly one to wake up.
//
// Analagous to Release(1) in semaphore.Weighted. One important difference
// is that calling Release before any Acquire will panic in semaphore.Weighted,
// but calling Post() before Wait() should neither block nor panic in our interface.
func (s *Semaphore) Post() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if waiter := s.waiters.Front(); waiter != nil {
		s.waiters.Remove(waiter)
		// Give this permit directly to one waiter without blocking the poster.
		close(waiter.Value.(chan struct{}))
		return
	}
	s.value++
}

// Wait decrements the semaphore value by one, if there are resources
// remaining from previous calls to Post. If there are no resources remaining,
// waits until resources are available or until the context is done, whichever
// is first.
//
// If the context is done with an error, returns that error. Returns `nil`
// in all other cases.
//
// Analagous to Acquire(ctx, 1) in semaphore.Weighted.
func (s *Semaphore) Wait(ctx context.Context) error {
	s.mu.Lock()
	if err := ctx.Err(); err != nil {
		s.mu.Unlock()
		return err
	}
	if s.value > 0 {
		s.value--
		s.mu.Unlock()
		return nil
	}
	ready := make(chan struct{})
	waiter := s.waiters.PushBack(ready)
	s.mu.Unlock()

	select {
	case <-ready:
		if err := ctx.Err(); err != nil {
			// Return the assigned permit if cancellation won the race.
			s.Post()
			return err
		}
		return nil
	case <-ctx.Done():
		s.mu.Lock()
		select {
		case <-ready:
			// Post already removed this waiter and assigned it a permit.
			s.mu.Unlock()
			s.Post()
		default:
			s.waiters.Remove(waiter)
			s.mu.Unlock()
		}
		return ctx.Err()
	}
}

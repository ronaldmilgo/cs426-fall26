package lab0_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"cs426.cloud/lab0"
	"github.com/stretchr/testify/require"
)

func TestSemaphore(t *testing.T) {
	t.Run("semaphore basic", func(t *testing.T) {
		s := lab0.NewSemaphore()
		go func() {
			s.Post()
		}()
		err := s.Wait(context.Background())
		require.NoError(t, err)
	})
	t.Run("semaphore starts with zero available resources", func(t *testing.T) {
		s := lab0.NewSemaphore()
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		err := s.Wait(ctx)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})
	t.Run("semaphore post before wait does not block", func(t *testing.T) {
		s := lab0.NewSemaphore()
		s.Post()
		s.Post()
		s.Post()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		err := s.Wait(ctx)
		require.NoError(t, err)
	})
	t.Run("post after wait releases the wait", func(t *testing.T) {
		s := lab0.NewSemaphore()
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		go func() {
			time.Sleep(20 * time.Millisecond)
			s.Post()
		}()
		err := s.Wait(ctx)
		require.NoError(t, err)
	})
}

func TestSemaphoreAdditional(t *testing.T) {
	// Tests that every stored post supplies exactly one permit.
	t.Run("stored permits are counted", func(t *testing.T) {
		s := lab0.NewSemaphore()
		for i := 0; i < 100; i++ {
			s.Post()
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		for i := 0; i < 100; i++ {
			require.NoError(t, s.Wait(ctx))
		}
		emptyCtx, stop := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer stop()
		require.ErrorIs(t, s.Wait(emptyCtx), context.DeadlineExceeded)
	})

	// Tests that one post releases one waiter and cancellation releases the rest.
	t.Run("one post releases one waiter", func(t *testing.T) {
		s := lab0.NewSemaphore()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		results := make(chan error, 8)
		for i := 0; i < 8; i++ {
			go func() { results <- s.Wait(ctx) }()
		}
		s.Post()
		require.NoError(t, <-results)
		select {
		case err := <-results:
			t.Fatalf("an extra waiter returned: %v", err)
		case <-time.After(20 * time.Millisecond):
		}
		cancel()
		for i := 0; i < 7; i++ {
			require.ErrorIs(t, <-results, context.Canceled)
		}
	})

	// Tests that a canceled wait leaves a stored permit available.
	t.Run("already canceled does not consume permit", func(t *testing.T) {
		s := lab0.NewSemaphore()
		s.Post()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		require.ErrorIs(t, s.Wait(ctx), context.Canceled)
		liveCtx, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		require.NoError(t, s.Wait(liveCtx))
	})

	// Tests that racing cancellation and posting never loses or duplicates a permit.
	t.Run("cancellation races with post", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			s := lab0.NewSemaphore()
			ctx, cancel := context.WithCancel(context.Background())
			result := make(chan error, 1)
			go func() { result <- s.Wait(ctx) }()
			var wg sync.WaitGroup
			wg.Add(2)
			go func() { defer wg.Done(); s.Post() }()
			go func() { defer wg.Done(); cancel() }()
			wg.Wait()
			err := <-result
			if err != nil {
				require.ErrorIs(t, err, context.Canceled)
				liveCtx, stop := context.WithTimeout(context.Background(), time.Second)
				err = s.Wait(liveCtx)
				stop()
				require.NoError(t, err, "cancellation lost the posted permit")
			}
			emptyCtx, stop := context.WithTimeout(context.Background(), time.Millisecond)
			err = s.Wait(emptyCtx)
			stop()
			require.ErrorIs(t, err, context.DeadlineExceeded)
		}
	})
}

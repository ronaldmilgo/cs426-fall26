package lab0_test

import (
	"context"
	"testing"
	"time"

	"cs426.cloud/lab0"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func chanToSlice[T any](ch chan T) []T {
	vals := make([]T, 0)
	for item := range ch {
		vals = append(vals, item)
	}
	return vals
}

type mergeFunc = func(chan string, chan string, chan string)

func runMergeTest(t *testing.T, merge mergeFunc) {
	t.Run("empty channels", func(t *testing.T) {
		a := make(chan string)
		b := make(chan string)
		out := make(chan string)
		close(a)
		close(b)

		merge(a, b, out)
		// If your lab0 hangs here, make sure you are closing your channels!
		require.Empty(t, chanToSlice(out))
	})

	// Please write your own tests
}

func TestMergeChannels(t *testing.T) {
	runMergeTest(t, func(a, b, out chan string) {
		lab0.MergeChannels(a, b, out)
	})
}

func TestMergeOrCancel(t *testing.T) {
	runMergeTest(t, func(a, b, out chan string) {
		_ = lab0.MergeChannelsOrCancel(context.Background(), a, b, out)
	})

	t.Run("already canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		a := make(chan string, 1)
		b := make(chan string, 1)
		out := make(chan string, 10)

		eg, _ := errgroup.WithContext(context.Background())
		eg.Go(func() error {
			return lab0.MergeChannelsOrCancel(ctx, a, b, out)
		})
		err := eg.Wait()
		a <- "a"
		b <- "b"

		require.Error(t, err)
		require.Equal(t, []string{}, chanToSlice(out))
	})

	t.Run("cancel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		a := make(chan string)
		b := make(chan string)
		out := make(chan string, 10)

		eg, _ := errgroup.WithContext(context.Background())
		eg.Go(func() error {
			return lab0.MergeChannelsOrCancel(ctx, a, b, out)
		})
		a <- "a"
		b <- "b"
		// Confirm delivery before canceling: a received input may still be
		// waiting to be forwarded when cancellation happens.
		var received []string
		for len(received) < 2 {
			select {
			case value, ok := <-out:
				require.True(t, ok, "output closed before both values arrived")
				received = append(received, value)
			case <-time.After(time.Second):
				cancel()
				t.Fatal("merge did not forward both values")
			}
		}
		cancel()

		err := eg.Wait()
		require.ErrorIs(t, err, context.Canceled)
		require.ElementsMatch(t, []string{"a", "b"}, received)
		require.Empty(t, chanToSlice(out))
	})
}

func TestMergeOrCancelBlockedOutput(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	a := make(chan string)
	b := make(chan string)
	out := make(chan string)
	done := make(chan error, 1)

	go func() {
		done <- lab0.MergeChannelsOrCancel(ctx, a, b, out)
	}()

	// On failure, release a stuck sender so this test does not leave the
	// current implementation's workers running after the test ends.
	t.Cleanup(func() {
		cancel()
		close(a)
		close(b)
		timer := time.NewTimer(time.Second)
		defer timer.Stop()
		for {
			select {
			case _, ok := <-out:
				if !ok {
					return
				}
			case <-timer.C:
				t.Error("merge did not close output during cleanup")
				return
			}
		}
	})

	// This unbuffered send proves a worker received the value before we
	// cancel. With no output reader, forwarding the value cannot complete.
	select {
	case a <- "blocked value":
	case <-time.After(time.Second):
		t.Fatal("merge did not receive the input value")
	}
	cancel()

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("MergeChannelsOrCancel did not return after cancellation while output was blocked")
	}

	select {
	case _, ok := <-out:
		require.False(t, ok, "output should be closed without forwarding the blocked value")
	default:
		t.Fatal("output was not closed before merge returned")
	}
}

type channelFetcher struct {
	ch chan string
}

func newChannelFetcher(ch chan string) *channelFetcher {
	return &channelFetcher{ch: ch}
}

func (f *channelFetcher) Fetch() (string, bool) {
	v, ok := <-f.ch
	return v, ok
}

func TestMergeFetches(t *testing.T) {
	runMergeTest(t, func(a, b, out chan string) {
		lab0.MergeFetches(newChannelFetcher(a), newChannelFetcher(b), out)
	})
}

func TestMergeFetchesAdditional(t *testing.T) {
	tests := []struct {
		name string
		a    []string
		b    []string
	}{
		{
			name: "both fetchers have data including duplicates and an empty string",
			a:    []string{"a", "shared", ""},
			b:    []string{"b", "shared"},
		},
		{
			name: "first fetcher is empty",
			b:    []string{"b1", "b2"},
		},
		{
			name: "second fetcher is empty",
			a:    []string{"a1", "a2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := make(chan string, len(tt.a))
			b := make(chan string, len(tt.b))
			for _, value := range tt.a {
				a <- value
			}
			for _, value := range tt.b {
				b <- value
			}
			close(a)
			close(b)

			out := make(chan string)
			go lab0.MergeFetches(newChannelFetcher(a), newChannelFetcher(b), out)

			// Reading finishes only when MergeFetches closes out. The go test
			// timeout catches a missing close or a deadlock.
			actual := chanToSlice(out)
			expected := append(append([]string{}, tt.a...), tt.b...)
			// Interleaving may vary, but each value's count must match.
			require.ElementsMatch(t, expected, actual)
		})
	}
}

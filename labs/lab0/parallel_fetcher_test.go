package lab0_test

import (
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cs426.cloud/lab0"
	"github.com/stretchr/testify/require"
	// "golang.org/x/sync/errgroup"
)

type MockFetcher struct {
	data  []string
	index int
	mu    sync.Mutex

	delay time.Duration
	// Int32 is an atomic int32
	activeFetches atomic.Int32
}

func NewMockFetcher(data []string, delay time.Duration) *MockFetcher {
	return &MockFetcher{
		data:  data,
		delay: delay,
	}
}

func (f *MockFetcher) Fetch() (string, bool) {
	f.activeFetches.Add(1)
	defer f.activeFetches.Add(-1)

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.index >= len(f.data) {
		return "", false
	}
	v := f.data[f.index]
	f.index++

	// Don't hold the lock while simulating the delay
	f.mu.Unlock()
	// add random jitter to delay
	time.Sleep(f.delay + time.Duration(rand.Intn(10))*time.Millisecond)
	f.mu.Lock()

	return v, true
}

func (f *MockFetcher) ActiveFetches() int32 {
	return f.activeFetches.Load()
}

func sliceToMap(slice []string) map[string]bool {
	m := make(map[string]bool)
	for _, v := range slice {
		m[v] = true
	}
	return m
}

func checkResultSet(t *testing.T, expected []string, actual []string) {
	require.ElementsMatch(t, expected, actual)
}

func callFetchNTimes(pf *lab0.ParallelFetcher, n int) []string {
	actual := make([]string, n)
	for i := 0; i < n; i++ {
		v, ok := pf.Fetch()
		if !ok {
			break
		}
		actual[i] = v
	}
	return actual
}

func TestParallelFetcher(t *testing.T) {
	t.Run("fetch basic", func(t *testing.T) {
		data := []string{"a", "b", "c", "d", "e"}
		pf := lab0.NewParallelFetcher(NewMockFetcher(data, 0), 1)

		results := callFetchNTimes(pf, 5)
		checkResultSet(t, data, results)

		// next call returns false
		_, ok := pf.Fetch()
		require.False(t, ok)
	})
	t.Run("fetch concurrency limits", func(t *testing.T) {
		N := 100
		data := make([]string, N)
		for i := 0; i < N; i++ {
			data[i] = strconv.Itoa(i)
		}
		mf := NewMockFetcher(data, 10*time.Millisecond)
		pf := lab0.NewParallelFetcher(mf, 3)

		done := make(chan struct{})
		go func() {
			for {
				select {
				case <-done:
					return
				default:
					require.LessOrEqual(t, mf.ActiveFetches(), int32(3))
				}
			}
		}()

		wg := sync.WaitGroup{}
		for i := 0; i < N; i++ {
			wg.Add(1)
			go func() {
				v, ok := pf.Fetch()
				require.True(t, ok)
				require.NotEmpty(t, v)
				wg.Done()
			}()
		}
		wg.Wait()

		// next call returns false
		_, ok := pf.Fetch()
		require.False(t, ok)

		done <- struct{}{}
	})
}

type exhaustedFetcher struct {
	calls atomic.Int32
}

func (f *exhaustedFetcher) Fetch() (string, bool) {
	f.calls.Add(1)
	return "", false
}

// Tests that waiting and later callers do not fetch again after exhaustion.
func TestParallelFetcherAdditional(t *testing.T) {
	f := &exhaustedFetcher{}
	pf := lab0.NewParallelFetcher(f, 1)
	start := make(chan struct{})
	results := make(chan bool, 20)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, ok := pf.Fetch()
			results <- ok
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	for ok := range results {
		require.False(t, ok)
	}
	value, ok := pf.Fetch()
	require.False(t, ok)
	require.Empty(t, value)
	require.Equal(t, int32(1), f.calls.Load())
}

type fetchResult struct {
	value string
	ok    bool
}

// controlledFetcher lets tests decide when each underlying call finishes.
type controlledFetcher struct {
	entered chan chan fetchResult
	stop    chan struct{}
	calls   atomic.Int32
}

func (f *controlledFetcher) Fetch() (string, bool) {
	f.calls.Add(1)
	reply := make(chan fetchResult, 1)
	select {
	case f.entered <- reply:
	case <-f.stop:
		return "", false
	}
	select {
	case result := <-reply:
		return result.value, result.ok
	case <-f.stop:
		return "", false
	}
}

func newControlledFetcher(t *testing.T) *controlledFetcher {
	f := &controlledFetcher{entered: make(chan chan fetchResult, 64), stop: make(chan struct{})}
	t.Cleanup(func() { close(f.stop) })
	return f
}

func receiveFetcherTestValue[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for fetch progress")
		var zero T
		return zero
	}
}

func startTestFetch(pf *lab0.ParallelFetcher, results chan<- fetchResult) {
	go func() {
		value, ok := pf.Fetch()
		results <- fetchResult{value, ok}
	}()
}

// Tests actual overlap, blocking at capacity, and reuse of a released permit.
func TestParallelFetcherSaturation(t *testing.T) {
	for _, limit := range []int{1, 3} {
		t.Run(strconv.Itoa(limit), func(t *testing.T) {
			f := newControlledFetcher(t)
			pf := lab0.NewParallelFetcher(f, limit)
			results := make(chan fetchResult, limit+1)
			replies := make([]chan fetchResult, 0, limit)
			for i := 0; i < limit; i++ {
				startTestFetch(pf, results)
				replies = append(replies, receiveFetcherTestValue(t, f.entered))
			}
			startTestFetch(pf, results)
			select {
			case <-f.entered:
				t.Fatal("underlying fetch exceeded the concurrency limit")
			case <-results:
				t.Fatal("fetch returned while all underlying calls were blocked")
			case <-time.After(50 * time.Millisecond):
			}

			replies[0] <- fetchResult{"first", true}
			require.Equal(t, fetchResult{"first", true}, receiveFetcherTestValue(t, results))
			next := receiveFetcherTestValue(t, f.entered)
			next <- fetchResult{"next", true}
			for _, reply := range replies[1:] {
				reply <- fetchResult{"remaining", true}
			}
			for i := 0; i < limit; i++ {
				require.True(t, receiveFetcherTestValue(t, results).ok)
			}
			require.Equal(t, int32(limit+1), f.calls.Load())
		})
	}
}

// Tests exhaustion with active calls, queued callers, and subsequent callers.
func TestParallelFetcherExhaustionWithActiveCalls(t *testing.T) {
	f := newControlledFetcher(t)
	pf := lab0.NewParallelFetcher(f, 3)
	results := make(chan fetchResult, 23)
	var replies []chan fetchResult
	for i := 0; i < 3; i++ {
		startTestFetch(pf, results)
		replies = append(replies, receiveFetcherTestValue(t, f.entered))
	}
	for i := 0; i < 20; i++ {
		startTestFetch(pf, results)
	}
	replies[0] <- fetchResult{}
	for i := 0; i < 21; i++ {
		require.Equal(t, fetchResult{}, receiveFetcherTestValue(t, results))
	}
	// Existing calls can finish after exhaustion without admitting new calls.
	for _, reply := range replies[1:] {
		reply <- fetchResult{}
	}
	for i := 0; i < 2; i++ {
		require.Equal(t, fetchResult{}, receiveFetcherTestValue(t, results))
	}
	startTestFetch(pf, results)
	require.Equal(t, fetchResult{}, receiveFetcherTestValue(t, results))
	require.Equal(t, int32(3), f.calls.Load())
}

// Tests exact delivery of duplicates and empty strings with more callers than data.
func TestParallelFetcherExactResults(t *testing.T) {
	expected := []string{"", "duplicate", "duplicate", "last"}
	input := make(chan string, len(expected))
	for _, value := range expected {
		input <- value
	}
	close(input)
	pf := lab0.NewParallelFetcher(newChannelFetcher(input), 8)
	results := make(chan fetchResult, 30)
	for i := 0; i < 30; i++ {
		startTestFetch(pf, results)
	}
	var actual []string
	for i := 0; i < 30; i++ {
		result := receiveFetcherTestValue(t, results)
		if result.ok {
			actual = append(actual, result.value)
		}
	}
	require.ElementsMatch(t, expected, actual)
}

package logger

import (
	"sync"
	"testing"
)

// Requests and cron jobs log concurrently; an unguarded append to logBuffer loses entries.
func TestAddToBufferKeepsEveryConcurrentEntry(t *testing.T) {
	logBuffer = nil
	t.Cleanup(func() { logBuffer = nil })

	const writers, perWriter = 50, 100
	var wg sync.WaitGroup
	for range writers {
		wg.Go(func() {
			for range perWriter {
				addToBuffer("INFO", "entry")
			}
		})
	}
	wg.Wait()

	if got := len(logBuffer); got != writers*perWriter {
		t.Fatalf("buffer holds %d entries, want %d", got, writers*perWriter)
	}
}

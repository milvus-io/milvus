package mlog

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// cleanupRecorder captures the cleanups CaptureGlobalLogs registers so that the
// restore path can be asserted explicitly instead of only at test teardown.
type cleanupRecorder struct {
	*testing.T
	cleanups []func()
}

func (r *cleanupRecorder) Cleanup(fn func()) {
	r.cleanups = append(r.cleanups, fn)
}

func (r *cleanupRecorder) runCleanups() {
	for i := len(r.cleanups) - 1; i >= 0; i-- {
		r.cleanups[i]()
	}
	r.cleanups = nil
}

func TestCaptureGlobalLogsCapturesAndRestores(t *testing.T) {
	before := L()
	recorder := &cleanupRecorder{T: t}
	t.Cleanup(recorder.runCleanups)

	sink := CaptureGlobalLogs(recorder, &Config{Level: "debug", DisableTimestamp: true})
	require.NotNil(t, sink)
	require.NotSame(t, before, L())

	Info(context.Background(), "captured by test sink", String("key", "value"))
	assert.Contains(t, sink.String(), "captured by test sink")
	assert.Contains(t, sink.String(), "key=value")

	recorder.runCleanups()
	assert.Same(t, before, L())
}

// TestCaptureGlobalLogsSinkIsConcurrencySafe is the regression test for the
// race that a bare bytes.Buffer sink produces: the global logger is shared with
// unrelated goroutines, so writes race with each other and with the test's own
// reads. It only fails under -race, which is how CI runs the Go unit tests.
func TestCaptureGlobalLogsSinkIsConcurrencySafe(t *testing.T) {
	sink := CaptureGlobalLogs(t, &Config{Level: "debug", DisableTimestamp: true})

	const (
		writers          = 4
		entriesPerWriter = 100
		reads            = 200
	)

	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < entriesPerWriter; j++ {
				Info(ctx, "concurrent background log entry")
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < reads; j++ {
			_ = sink.String()
		}
	}()
	wg.Wait()

	assert.Contains(t, sink.String(), "concurrent background log entry")
}

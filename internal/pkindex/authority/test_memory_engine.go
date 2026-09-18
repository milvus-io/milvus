//go:build test
// +build test

package authority

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"
)

// This file is compiled only with the "test" build tag. It provides an in-memory
// Engine so that unit tests, WAL chain tests and integration tests can exercise
// the primary key index before the persistent engine exists.
// It keeps nothing across restarts and must not be used in production.
// Production binaries have no engine factory, so the index stays disabled there.
// Remove this file once the persistent engine is wired in.

func init() {
	RegisterEngineFactory(NewMemoryEngine)
}

// errMemoryEngineClosed is an internal sentinel. It never crosses a component
// boundary on its own: a caller that needs it on the wire must translate it first.
var errMemoryEngineClosed = errors.New("memory engine is closed")

type memoryEngine struct {
	mu     sync.RWMutex
	data   map[string][]byte
	closed bool
}

// NewMemoryEngine creates an empty in-memory engine.
func NewMemoryEngine(string) (Engine, error) {
	return &memoryEngine{data: make(map[string][]byte)}, nil
}

func (e *memoryEngine) MultiGet(_ context.Context, keys [][]byte) ([][]byte, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closed {
		return nil, errMemoryEngineClosed
	}
	values := make([][]byte, len(keys))
	for i, key := range keys {
		if value, ok := e.data[string(key)]; ok {
			values[i] = value
		}
	}
	return values, nil
}

func (e *memoryEngine) Write(_ context.Context, muts []Mutation) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return errMemoryEngineClosed
	}
	for _, mut := range muts {
		if mut.Delete {
			delete(e.data, string(mut.Key))
			continue
		}
		e.data[string(mut.Key)] = append([]byte(nil), mut.Value...)
	}
	return nil
}

func (e *memoryEngine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.closed = true
	e.data = nil
	return nil
}

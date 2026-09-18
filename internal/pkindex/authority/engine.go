package authority

import (
	"context"
	"sync"
)

// Mutation is one change to the engine.
type Mutation struct {
	Key    []byte
	Value  []byte
	Delete bool // when true, Value is ignored and Key becomes absent
}

// Engine is the byte-level key-value store behind one vchannel's Authority.
// It is defined here, by its consumer. Storage implementations adapt to it.
//
// TODO: replace the test-only memory engine with the persistent engine.
type Engine interface {
	// MultiGet returns the values aligned with keys. A nil value means the key is absent.
	// The caller must not modify the returned values.
	MultiGet(ctx context.Context, keys [][]byte) ([][]byte, error)
	// Write applies all mutations atomically. A concurrent MultiGet sees either none or all of them.
	// Mutations are applied in slice order. A later mutation of the same key wins.
	Write(ctx context.Context, muts []Mutation) error
	Close() error
}

// EngineFactory creates the engine of one vchannel.
type EngineFactory func(vchannel string) (Engine, error)

var (
	engineFactoryMu sync.RWMutex
	engineFactory   EngineFactory
)

// RegisterEngineFactory sets the process-wide engine factory. Passing nil removes it.
func RegisterEngineFactory(f EngineFactory) {
	engineFactoryMu.Lock()
	defer engineFactoryMu.Unlock()
	engineFactory = f
}

// HasEngineFactory reports whether an engine factory is registered.
func HasEngineFactory() bool {
	engineFactoryMu.RLock()
	defer engineFactoryMu.RUnlock()
	return engineFactory != nil
}

func getEngineFactory() EngineFactory {
	engineFactoryMu.RLock()
	defer engineFactoryMu.RUnlock()
	return engineFactory
}

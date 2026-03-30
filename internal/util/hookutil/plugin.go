package hookutil

import (
	"fmt"
	"plugin"
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
)

var pluginMutex sync.Mutex

// LoadPlugin opens a Go plugin at the given path, looks up the named symbol,
// and type-asserts it to T. All calls are serialized with a mutex because
// Go's plugin.Open() is not safe for concurrent use (causes "empty pluginpath" panic).
func LoadPlugin[T any](path string, symbol string) (T, error) {
	var zero T
	if path == "" {
		return zero, fmt.Errorf("empty plugin path for symbol %q", symbol)
	}

	log.Info("loading plugin", zap.String("path", path), zap.String("symbol", symbol))

	pluginMutex.Lock()
	defer pluginMutex.Unlock()

	p, err := plugin.Open(path)
	if err != nil {
		return zero, fmt.Errorf("fail to open plugin %s: %w", path, err)
	}

	sym, err := p.Lookup(symbol)
	if err != nil {
		return zero, fmt.Errorf("fail to find symbol %q in plugin %s: %w", symbol, path, err)
	}

	val, ok := sym.(T)
	if !ok {
		return zero, fmt.Errorf("symbol %q in plugin %s does not implement expected interface", symbol, path)
	}

	return val, nil
}

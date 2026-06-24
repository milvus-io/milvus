package hookutil

import (
	"context"
	"plugin"
	"sync"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

var pluginMutex sync.Mutex

// LoadPlugin opens a Go plugin at the given path, looks up the named symbol,
// and type-asserts it to T. All calls are serialized with a mutex because
// Go's plugin.Open() is not safe for concurrent use (causes "empty pluginpath" panic).
func LoadPlugin[T any](path string, symbol string) (T, error) {
	var zero T
	if path == "" {
		return zero, merr.WrapErrParameterInvalidMsg("empty plugin path for symbol %q", symbol)
	}

	mlog.Info(context.TODO(), "loading plugin", mlog.String("path", path), mlog.String("symbol", symbol))

	pluginMutex.Lock()
	defer pluginMutex.Unlock()

	p, err := plugin.Open(path)
	if err != nil {
		return zero, merr.Wrapf(err, "fail to open plugin %s", path)
	}

	sym, err := p.Lookup(symbol)
	if err != nil {
		return zero, merr.Wrapf(err, "fail to find symbol %q in plugin %s", symbol, path)
	}

	val, ok := sym.(T)
	if !ok {
		return zero, merr.WrapErrServiceInternalMsg("symbol %q in plugin %s does not implement expected interface", symbol, path)
	}

	return val, nil
}

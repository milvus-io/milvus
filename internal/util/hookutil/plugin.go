package hookutil

import (
	"context"
	"plugin"
	"sync"

	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

var pluginMutex sync.Mutex

// FinishPluginLoading completes every plugin load this process can still
// perform and then permanently disables the sonic JSON gate. It must be called
// after all components have started, at the point where plugin.Open can no
// longer run. This is the process-level lifecycle guarantee that replaces any
// runtime counting: the hook plugin is loaded eagerly by the proxy during
// creation (internal/proxy/proxy.go), and the cipher plugin is forced to
// completion here (a no-op when not configured). After it returns, sonic JIT
// module registration can no longer race with plugin loading, so the JSON hot
// path costs nothing.
func FinishPluginLoading() {
	InitOnceCipher()
	json.DisableGate()
}

// LoadPlugin opens a Go plugin at the given path, looks up the named symbol,
// and type-asserts it to T. All calls are serialized with a mutex because
// Go's plugin.Open() is not safe for concurrent use. The plugin opening is
// additionally guarded by the json gate's exclusive side so that sonic JIT
// module registration never races with plugin loading (which can crash the
// process with "runtime: plugin has empty pluginpath").
func LoadPlugin[T any](path string, symbol string) (T, error) {
	var zero T
	if path == "" {
		return zero, merr.WrapErrParameterInvalidMsg("empty plugin path for symbol %q", symbol)
	}

	mlog.Info(context.TODO(), "loading plugin", mlog.String("path", path), mlog.String("symbol", symbol))

	pluginMutex.Lock()
	defer pluginMutex.Unlock()

	json.BlockForPluginLoad()
	defer json.UnblockForPluginLoad()

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

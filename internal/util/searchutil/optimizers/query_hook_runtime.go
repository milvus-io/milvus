package optimizers

import (
	"context"
	"strings"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/extension"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// ErrQueryHookConflict is an initialization sentinel checked by hosts before
// deciding whether an optional hook loading failure is fatal.
var ErrQueryHookConflict = errors.New("queryNode.soPath and a compiled-in query hook are both configured")

// LoadQueryHook shares the same plugin/compiled-hook policy between the legacy
// QueryNode delegator and Proxy's QueryViewClient.
func LoadQueryHook() (QueryHook, error) {
	path := paramtable.Get().QueryNodeCfg.SoPath.GetValue()
	if compiled := extension.InstalledQueryHook(); compiled != nil {
		if path != "" {
			return nil, merr.Wrapf(ErrQueryHookConflict, "queryNode.soPath is set to %q and a query hook is also compiled in; both tune every search, and only one can", path)
		}
		return compiled, nil
	}
	if path == "" {
		return nil, merr.WrapErrServiceInternalMsg("fail to set the plugin path")
	}
	return hookutil.LoadPlugin[QueryHook](path, "QueryNodePlugin")
}

func InitQueryHook(hook QueryHook) error {
	if err := hook.Init(paramtable.Get().AutoIndexConfig.AutoIndexSearchConfig.GetValue()); err != nil {
		return merr.Wrap(err, "fail to init configs for the hook")
	}
	if err := hook.InitTuningConfig(paramtable.Get().AutoIndexConfig.AutoIndexTuningConfig.GetValue()); err != nil {
		return merr.Wrap(err, "fail to init tuning configs for the hook")
	}
	return nil
}

// WatchQueryHook follows the same dynamic settings in both execution hosts.
// The host owns the returned unregister function and a distinct handler name.
func WatchQueryHook(ctx context.Context, hook QueryHook, name string) func() {
	params := paramtable.Get()
	searchKey := params.AutoIndexConfig.AutoIndexSearchConfig.Key
	tuningPrefix := params.AutoIndexConfig.AutoIndexTuningConfig.KeyPrefix
	search := config.NewHandler(name, func(event *config.Event) {
		if hook != nil {
			if err := hook.Init(event.Value); err != nil {
				mlog.Error(ctx, "failed to refresh hook config", mlog.Err(err))
			}
		}
	})
	tuning := config.NewHandler(name+"2", func(event *config.Event) {
		if hook == nil || !strings.HasPrefix(event.Key, tuningPrefix) {
			return
		}
		key := strings.TrimPrefix(event.Key, tuningPrefix)
		var err error
		switch event.EventType {
		case config.CreateType, config.UpdateType:
			err = hook.InitTuningConfig(map[string]string{key: event.Value})
		case config.DeleteType:
			err = hook.DeleteTuningConfig(key)
		}
		if err != nil {
			mlog.Warn(ctx, "failed to refresh hook tuning config", mlog.Err(err))
		}
	})
	params.Watch(searchKey, search)
	params.WatchKeyPrefix(tuningPrefix, tuning)
	return func() { params.Unwatch(searchKey, search); params.Unwatch(tuningPrefix, tuning) }
}

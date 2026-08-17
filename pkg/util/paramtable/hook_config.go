package paramtable

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

const hookYamlFile = "hook.yaml"

type hookConfig struct {
	hookBase *BaseTable

	SoPath   ParamItem  `refreshable:"false"`
	SoConfig ParamGroup `refreshable:"true"`
}

func (h *hookConfig) init(base *BaseTable) {
	h.hookBase = base

	h.SoPath = ParamItem{
		Key:          "soPath",
		Version:      "2.0.0",
		DefaultValue: "",
	}
	h.SoPath.Init(base.mgr)

	h.SoConfig = ParamGroup{
		KeyPrefix: "",
		Version:   "2.2.0",
	}
	h.SoConfig.Init(base.mgr)

	// Entry count only: hook.yaml is plugin-defined and may carry credentials
	// under names the core cannot classify. Logged after SoConfig.Init so the
	// empty prefix is registered and the projection is not empty.
	mlog.Info(context.TODO(), "hook config loaded", mlog.Int("entries", len(h.SoConfig.GetValue())))
}

func (h *hookConfig) WatchHookWithPrefix(ident string, keyPrefix string, onEvent func(*config.Event)) {
	h.hookBase.mgr.Dispatcher.RegisterForKeyPrefix(keyPrefix, config.NewHandler(ident, onEvent))
}

func (h *hookConfig) GetAll() map[string]string {
	return h.hookBase.mgr.GetConfigs()
}

func (h *hookConfig) Save(key string, value string) error {
	return h.hookBase.Save(key, value)
}

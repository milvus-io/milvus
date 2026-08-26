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
		// The hook table is built by NewBaseTableFromYamlOnly, so hook.yaml is
		// its only source and every key in it really is plugin configuration.
		// The core has no schema for those plugin-owned keys, so it cannot prove
		// which values carry credentials or topology. Fail the whole namespace
		// closed at projection boundaries while keeping its internal reader raw.
		// TestNoEmptyPrefixParamGroup asserts ComponentParam declares no such
		// group, and config.Manager omits environment-only keys whatever prefix
		// matched them, so an empty prefix cannot publish the environment.
		KeyPrefix: "",
		Version:   "2.2.0",
		Sensitive: true,
	}
	h.SoConfig.Init(base.mgr)

	// No values, and no count either: hook.yaml is plugin-defined and may carry
	// credentials under names the core cannot classify, and every nested key is
	// stored under two identities so a count would not mean what it reads like.
	mlog.Info(context.TODO(), "hook config loaded")
}

func (h *hookConfig) WatchHookWithPrefix(ident string, keyPrefix string, onEvent func(*config.Event)) {
	h.hookBase.mgr.Dispatcher.RegisterForKeyPrefix(keyPrefix, config.NewHandler(ident, onEvent))
}

func (h *hookConfig) GetAll() map[string]string {
	return h.hookBase.mgr.ProjectConfigs()
}

func (h *hookConfig) Save(key string, value string) error {
	return h.hookBase.Save(key, value)
}

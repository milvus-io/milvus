package paramtable

import (
	"github.com/milvus-io/milvus/pkg/config"
	"github.com/milvus-io/milvus/pkg/log"
	"go.uber.org/zap"
)

const hookYamlFile = "hook.yaml"

type hookConfig struct {
	hookBase *BaseTable

	SoPath                      ParamItem  `refreshable:"false"`
	SoConfig                    ParamGroup `refreshable:"true"`
	QueryNodePluginConfig       ParamItem  `refreshable:"true"`
	QueryNodePluginTuningConfig ParamGroup `refreshable:"true"`
}

func (h *hookConfig) init(base *BaseTable) {
	hookBase := &BaseTable{YamlFiles: []string{hookYamlFile}}
	hookBase.init(2)
	h.hookBase = hookBase

	log.Info("hook config", zap.Any("hook", hookBase.FileConfigs()))

	h.SoPath = ParamItem{
		Key:          "soPath",
		Version:      "2.0.0",
		DefaultValue: "",
	}
	h.SoPath.Init(hookBase.mgr)

	h.SoConfig = ParamGroup{
		KeyPrefix: "",
		Version:   "2.2.0",
	}
	h.SoConfig.Init(hookBase.mgr)

	h.QueryNodePluginConfig = ParamItem{
		Key:     "autoindex.params.search",
		Version: "2.3.0",
	}
	h.QueryNodePluginConfig.Init(base.mgr)

	h.QueryNodePluginTuningConfig = ParamGroup{
		KeyPrefix: "autoindex.params.tuning.",
		Version:   "2.3.0",
	}
	h.QueryNodePluginTuningConfig.Init(base.mgr)
}

func (h *hookConfig) WatchHookWithPrefix(ident string, keyPrefix string, onEvent func(*config.Event)) {
	h.hookBase.mgr.Dispatcher.RegisterForKeyPrefix(keyPrefix, config.NewHandler(ident, onEvent))
}

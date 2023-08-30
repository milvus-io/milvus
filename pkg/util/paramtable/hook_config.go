package paramtable

import (
	"github.com/milvus-io/milvus/pkg/config"
	"github.com/milvus-io/milvus/pkg/log"
	"go.uber.org/zap"
)

const hookYamlFile = "hook.yaml"

type hookConfig struct {
	hookBase *BaseTable

	SoPath   ParamItem  `refreshable:"false"`
	SoConfig ParamGroup `refreshable:"true"`
}

func (h *hookConfig) init() {
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
}

func (h *hookConfig) WatchHookWithPrefix(ident string, keyPrefix string, onEvent func(*config.Event)) {
	h.hookBase.mgr.Dispatcher.RegisterForKeyPrefix(keyPrefix, config.NewHandler(ident, onEvent))
}

package paramtable

import (
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/config"
	"github.com/milvus-io/milvus/pkg/v2/log"
)

const hookYamlFile = "hook.yaml"

type hookConfig struct {
	hookBase *BaseTable

	SoPath   ParamItem  `refreshable:"false"`
	SoConfig ParamGroup `refreshable:"true"`
}

func (h *hookConfig) init(base *BaseTable) {
	h.hookBase = base
	log.Info("hook config", zap.Any("hook", base.FileConfigs()))

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
}

func (h *hookConfig) WatchHookWithPrefix(ident string, keyPrefix string, onEvent func(*config.Event)) {
	h.hookBase.mgr.Dispatcher.RegisterForKeyPrefix(keyPrefix, config.NewHandler(ident, onEvent))
}

func (h *hookConfig) GetAll() map[string]string {
	return h.hookBase.mgr.GetConfigs()
}

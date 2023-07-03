package paramtable

import (
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
)

const hookYamlFile = "hook.yaml"

type hookConfig struct {
	SoPath                      ParamItem  `refreshable:"false"`
	SoConfig                    ParamGroup `refreshable:"false"`
	QueryNodePluginConfig       ParamItem  `refreshable:"true"`
	QueryNodePluginTuningConfig ParamGroup `refreshable:"true"`
}

func (h *hookConfig) init(base *BaseTable) {
	hookBase := &BaseTable{YamlFiles: []string{hookYamlFile}}
	hookBase.init(0)

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

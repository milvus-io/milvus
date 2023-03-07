package paramtable

import (
	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"
)

const hookYamlFile = "hook.yaml"

type hookConfig struct {
	SoPath   ParamItem  `refreshable:"false"`
	SoConfig ParamGroup `refreshable:"false"`
}

func (h *hookConfig) init() {
	base := &BaseTable{YamlFiles: []string{hookYamlFile}}
	base.init(0)
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

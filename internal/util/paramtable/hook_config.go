package paramtable

import (
	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"
)

const hookYamlFile = "hook.yaml"

type HookConfig struct {
	Base     *BaseTable
	SoPath   string
	SoConfig map[string]string
}

func (h *HookConfig) init() {
	h.Base = &BaseTable{YamlFile: hookYamlFile}
	h.Base.Init()
	log.Info("hook config", zap.Any("hook", h.Base.Configs()))

	h.initSoPath()
	h.initSoConfig()
}

func (h *HookConfig) initSoPath() {
	h.SoPath = h.Base.LoadWithDefault("soPath", "")
}

func (h *HookConfig) initSoConfig() {
	// all keys have been set lower
	h.SoConfig = h.Base.Configs()
}

package paramtable

const hookYamlFile = "hook.yaml"

type HookConfig struct {
	Base     *BaseTable
	SoPath   string
	SoConfig map[string]string
}

func (h *HookConfig) init() {
	h.Base = &BaseTable{YamlFile: hookYamlFile}
	h.Base.Init()

	h.initSoPath()
	h.initSoConfig()
}

func (h *HookConfig) initSoPath() {
	h.SoPath = h.Base.LoadWithDefault("soPath", "")
}

func (h *HookConfig) initSoConfig() {
	// all keys have been set lower
	h.SoConfig = h.Base.GetConfigSubSet("soConfig.")
}

package paramtable

const hookYamlFile = "hook.yaml"

type hookConfig struct {
	SoPath   ParamItem
	SoConfig ParamGroup
}

func (h *hookConfig) init() {
	base := &BaseTable{YamlFile: hookYamlFile}
	base.Init()

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

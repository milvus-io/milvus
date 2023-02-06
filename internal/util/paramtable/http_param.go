package paramtable

type httpConfig struct {
	Enabled   ParamItem `refreshable:"false"`
	DebugMode ParamItem `refreshable:"false"`
}

func (p *httpConfig) init(base *BaseTable) {
	p.Enabled = ParamItem{
		Key:          "proxy.http.enabled",
		DefaultValue: "true",
		Version:      "2.1.0",
		Doc:          "Whether to enable the http server",
		Export:       true,
	}
	p.Enabled.Init(base.mgr)

	p.DebugMode = ParamItem{
		Key:          "proxy.http.debug_mode",
		DefaultValue: "false",
		Version:      "2.1.0",
		Doc:          "Whether to enable http server debug mode",
		Export:       true,
	}
	p.DebugMode.Init(base.mgr)
}

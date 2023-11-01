package paramtable

type httpConfig struct {
	Enabled              ParamItem `refreshable:"false"`
	DebugMode            ParamItem `refreshable:"false"`
	Port                 ParamItem `refreshable:"false"`
	AcceptTypeAllowInt64 ParamItem `refreshable:"false"`
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

	p.Port = ParamItem{
		Key:          "proxy.http.port",
		Version:      "2.3.0",
		Doc:          "high-level restful api",
		PanicIfEmpty: false,
		Export:       true,
	}
	p.Port.Init(base.mgr)

	p.AcceptTypeAllowInt64 = ParamItem{
		Key:          "proxy.http.acceptTypeAllowInt64",
		DefaultValue: "false",
		Version:      "2.3.2",
		Doc:          "high-level restful api, whether http client can deal with int64",
		PanicIfEmpty: false,
		Export:       true,
	}
	p.AcceptTypeAllowInt64.Init(base.mgr)
}

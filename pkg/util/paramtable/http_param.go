// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package paramtable

type httpConfig struct {
	Enabled               ParamItem `refreshable:"false"`
	DebugMode             ParamItem `refreshable:"false"`
	Port                  ParamItem `refreshable:"false"`
	AcceptTypeAllowInt64  ParamItem `refreshable:"true"`
	EnablePprof           ParamItem `refreshable:"false"`
	RequestTimeoutMs      ParamItem `refreshable:"true"`
	HSTSMaxAge            ParamItem `refreshable:"false"`
	HSTSIncludeSubDomains ParamItem `refreshable:"false"`
	EnableHSTS            ParamItem `refreshable:"false"`
	EnableWebUI           ParamItem `refreshable:"false"`
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
		DefaultValue: "true",
		Version:      "2.3.2",
		Doc:          "high-level restful api, whether http client can deal with int64",
		PanicIfEmpty: false,
		Export:       true,
	}
	p.AcceptTypeAllowInt64.Init(base.mgr)

	p.EnablePprof = ParamItem{
		Key:          "proxy.http.enablePprof",
		DefaultValue: "true",
		Version:      "2.3.3",
		Doc:          "Whether to enable pprof middleware on the metrics port",
		Export:       true,
	}
	p.EnablePprof.Init(base.mgr)

	p.RequestTimeoutMs = ParamItem{
		Key:          "proxy.http.requestTimeoutMs",
		DefaultValue: "30000",
		Version:      "2.5.10",
		Doc:          "default restful request timeout duration in milliseconds",
		Export:       false,
	}
	p.RequestTimeoutMs.Init(base.mgr)

	p.HSTSMaxAge = ParamItem{
		Key:          "proxy.http.hstsMaxAge",
		DefaultValue: "31536000", // 1 year
		Version:      "2.6.0",
		Doc:          "Strict-Transport-Security max-age in seconds",
		Export:       true,
	}
	p.HSTSMaxAge.Init(base.mgr)

	p.HSTSIncludeSubDomains = ParamItem{
		Key:          "proxy.http.hstsIncludeSubDomains",
		DefaultValue: "false",
		Version:      "2.6.0",
		Doc:          "Include subdomains in Strict-Transport-Security",
		Export:       true,
	}
	p.HSTSIncludeSubDomains.Init(base.mgr)

	p.EnableHSTS = ParamItem{
		Key:          "proxy.http.enableHSTS",
		DefaultValue: "false",
		Version:      "2.6.0",
		Doc:          "Whether to enable setting the Strict-Transport-Security header",
		Export:       true,
	}
	p.EnableHSTS.Init(base.mgr)

	p.EnableWebUI = ParamItem{
		Key:          "proxy.http.enableWebUI",
		DefaultValue: "true",
		Version:      "v2.5.14",
		Doc:          "Whether to enable setting the WebUI middleware on the metrics port",
		Export:       true,
	}
	p.EnableWebUI.Init(base.mgr)
}

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
	CompatibilityMode     ParamItem `refreshable:"true"`
	MaxExprParamsDepth    ParamItem `refreshable:"true"`
	NativeJSONResponse    ParamItem `refreshable:"true"`
	LegacyArrayResponse   ParamItem `refreshable:"true"`
	EnablePprof           ParamItem `refreshable:"false"`
	RequestTimeoutMs      ParamItem `refreshable:"true"`
	DQLAdmissionEnabled   ParamItem `refreshable:"true"`
	ReadHeaderTimeout     ParamItem `refreshable:"false"`
	ReadTimeout           ParamItem `refreshable:"false"`
	WriteTimeout          ParamItem `refreshable:"false"`
	IdleTimeout           ParamItem `refreshable:"false"`
	MaxHeaderBytes        ParamItem `refreshable:"false"`
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

	p.CompatibilityMode = ParamItem{
		Key:          "proxy.http.compatibilityMode",
		DefaultValue: "false",
		Version:      "3.0.1",
		Doc: `high-level restful api, restore the value handling of releases that predate the REST insert
validation work. When true the server keeps the previous lenient behavior: a missing or null non-nullable field is
stored as an empty value, out-of-range integers wrap instead of being rejected, numbers reach VarChar and JSON fields
through their float64 rendering, and integers too large for the JSON engine become 0. This is a temporary escape hatch
for clients that have not been corrected yet: every one of those behaviors silently changes what is stored.`,
		PanicIfEmpty: false,
		Export:       true,
	}
	p.CompatibilityMode.Init(base.mgr)
	p.MaxExprParamsDepth = ParamItem{
		Key:          "proxy.http.maxExprParamsDepth",
		DefaultValue: "100",
		Version:      "3.0.1",
		Doc: `high-level restful api, the deepest nesting an expression template parameter may use. Converting a
parameter walks its arrays and objects recursively, so the depth a caller may send has to be bounded; requests past the
bound are rejected as invalid rather than served. Values above 1024 are read as 1024, since past that the recursion
itself is the risk the setting exists to remove; values below 1 are read as 1.`,
		PanicIfEmpty: false,
		Export:       true,
	}
	p.MaxExprParamsDepth.Init(base.mgr)
	p.NativeJSONResponse = ParamItem{
		Key:          "proxy.http.nativeJSONResponse",
		DefaultValue: "true",
		Version:      "3.0.1",
		Doc: `high-level restful api, return a JSON field as the document it holds rather than as a string.
Turn this off only to keep clients written against the older shape working while they migrate: there a JSON field read
back as "{\"a\":1}" while the same value in the dynamic field read back as {"a":1}. The insert path follows the same
switch, so either shape can be sent back unchanged: while the field reads back as text, a JSON string is read as the
document it spells; once it reads back as the document itself, a string is stored as the string it is. Rows written
before the insert path stopped storing non-JSON bytes may not hold a document; if any row in a response is such a row,
every JSON field in that response falls back to the string form and a warning is logged, so a caller always sees one
shape or the other and never a mixture.`,
		PanicIfEmpty: false,
		Export:       true,
	}
	p.NativeJSONResponse.Init(base.mgr)
	p.LegacyArrayResponse = ParamItem{
		Key:          "proxy.http.legacyArrayResponse",
		DefaultValue: "false",
		Version:      "3.0.1",
		Doc: `high-level restful api, whether to return Array fields wrapped in the raw protobuf ScalarField shape
({"tags":{"Data":{"StringData":{"data":["a","b"]}}}}) instead of a native JSON array ({"tags":["a","b"]}).
Only enable this to keep clients written against the old, incorrect shape working while they migrate;
it will be removed in a future release. It covers the shape of a top-level Array field and nothing else:
a struct array's sub-fields are unaffected, and so is Accept-Type-Allow-Int64, which renders an Int64 as a
string wherever one appears, including inside either kind of array.`,
		PanicIfEmpty: false,
		Export:       true,
	}
	p.LegacyArrayResponse.Init(base.mgr)

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

	p.DQLAdmissionEnabled = ParamItem{
		Key:          "proxy.http.dqlAdmissionEnabled",
		DefaultValue: "true",
		Version:      "3.0.1",
		Doc: `high-level restful api, reject a search/query request with HTTP 429 while the proxy's DQL task
queue is full, before the request body is decoded. The scheduler rejects such a request with the same
TooManyRequests error anyway, but only after the body has been decoded; admission moves the same verdict before that
cost. Disabling restores the old always-decode behavior.`,
		Export: true,
	}
	p.DQLAdmissionEnabled.Init(base.mgr)

	p.ReadHeaderTimeout = ParamItem{
		Key:          "proxy.http.readHeaderTimeout",
		DefaultValue: "5s",
		Version:      "2.6.0",
		Doc:          "HTTP server timeout for reading request headers",
		Export:       true,
	}
	p.ReadHeaderTimeout.Init(base.mgr)

	p.ReadTimeout = ParamItem{
		Key:          "proxy.http.readTimeout",
		DefaultValue: "30s",
		Version:      "2.6.0",
		Doc: `HTTP server timeout for reading the entire request, including the body. 0 disables this timeout.
Matches proxy.http.requestTimeoutMs: a client that never finishes sending a declared body is disconnected
around the same time an in-budget request would already time out, instead of pinning a goroutine/connection/fd
indefinitely. Raise this only if legitimate requests routinely need longer than requestTimeoutMs to upload their
body on slow networks.`,
		Export: true,
	}
	p.ReadTimeout.Init(base.mgr)

	p.WriteTimeout = ParamItem{
		Key:          "proxy.http.writeTimeout",
		DefaultValue: "60s",
		Version:      "2.6.0",
		Doc: `HTTP server timeout for handling requests and writing responses. 0 disables this timeout.
Kept comfortably above requestTimeoutMs + readTimeout so the graceful in-app timeout response
(see timeoutMiddleware) always has a chance to be written before this raw, connection-level timeout
would otherwise abort the connection.`,
		Export: true,
	}
	p.WriteTimeout.Init(base.mgr)

	p.IdleTimeout = ParamItem{
		Key:          "proxy.http.idleTimeout",
		DefaultValue: "300s",
		Version:      "2.6.0",
		Doc:          "HTTP server keep-alive idle timeout",
		Export:       true,
	}
	p.IdleTimeout.Init(base.mgr)

	p.MaxHeaderBytes = ParamItem{
		Key:          "proxy.http.maxHeaderBytes",
		DefaultValue: "16777216",
		Version:      "2.6.0",
		Doc:          "Maximum number of bytes the HTTP server reads from request headers. Defaults to 16MiB to match grpc-go's max header list size, since in shared-port mode this server also serves external gRPC over HTTP/2",
		Export:       true,
	}
	p.MaxHeaderBytes.Init(base.mgr)

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

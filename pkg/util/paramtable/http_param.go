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

import (
	"errors"
	"strconv"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type httpConfig struct {
	Enabled                   ParamItem `refreshable:"false"`
	EnableV1                  ParamItem `refreshable:"false"`
	DebugMode                 ParamItem `refreshable:"false"`
	Port                      ParamItem `refreshable:"false"`
	AcceptTypeAllowInt64      ParamItem `refreshable:"true"`
	CompatibilityMode         ParamItem `refreshable:"true"`
	MaxExprParamsDepth        ParamItem `refreshable:"true"`
	NativeJSONResponse        ParamItem `refreshable:"true"`
	LegacyArrayResponse       ParamItem `refreshable:"true"`
	EnablePprof               ParamItem `refreshable:"false"`
	RequestTimeoutMs          ParamItem `refreshable:"true"`
	DQLAdmissionEnabled       ParamItem `refreshable:"true"`
	ReadHeaderTimeout         ParamItem `refreshable:"false"`
	OverallTimeoutBudget      ParamItem `refreshable:"false"`
	MaxConnectionIdleInterval ParamItem `refreshable:"false"`
	ReadTimeout               ParamItem `refreshable:"false"`
	WriteTimeout              ParamItem `refreshable:"false"`
	IdleTimeout               ParamItem `refreshable:"false"`
	MaxHeaderBytes            ParamItem `refreshable:"false"`
	HSTSMaxAge                ParamItem `refreshable:"false"`
	HSTSIncludeSubDomains     ParamItem `refreshable:"false"`
	EnableHSTS                ParamItem `refreshable:"false"`
	EnableWebUI               ParamItem `refreshable:"false"`
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

	p.EnableV1 = ParamItem{
		Key:          "proxy.http.enableV1",
		DefaultValue: "true",
		Version:      "3.0.1",
		Doc: `Whether to register /v1/vector/* on the proxy HTTP port and /api/v1/_* on the metrics port.
Disabling requires a restart and leaves the HTTP listeners, /v2/vectordb/*, probes, and metrics available.
The non-underscore /api/v1/* REST API has been removed regardless of this setting.
WebUI data and telemetry commands require these console APIs; disable proxy.http.enableWebUI too to hide the pages.`,
		Export: true,
	}
	p.EnableV1.Init(base.mgr)

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

	p.OverallTimeoutBudget = ParamItem{
		Key:          "proxy.http.overallTimeoutBudget",
		DefaultValue: "120s",
		Doc:          "Server-side REST budget from completed request headers through response write. Clients may shorten but not extend it; header reading has a separate timeout.",
		Export:       true,
	}
	p.OverallTimeoutBudget.Init(base.mgr)

	p.MaxConnectionIdleInterval = ParamItem{
		Key:    "proxy.http.maxConnectionIdleInterval",
		Doc:    "Keep-alive idle interval; if absent, the deprecated idleTimeout setting applies.",
		Export: true,
	}
	p.MaxConnectionIdleInterval.Init(base.mgr)

	p.ReadTimeout = ParamItem{
		Key:          "proxy.http.readTimeout",
		DefaultValue: "0s",
		Version:      "2.6.0",
		Doc:          "HTTP server timeout for reading the entire request, including the body. 0 disables this timeout",
		Export:       true,
	}
	p.ReadTimeout.Init(base.mgr)

	p.WriteTimeout = ParamItem{
		Key:          "proxy.http.writeTimeout",
		DefaultValue: "0s",
		Version:      "2.6.0",
		Doc:          "HTTP server timeout for handling requests and writing responses. 0 disables this timeout",
		Export:       true,
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
		Sensitivity:  Sensitive,
	}
	p.HSTSMaxAge.Init(base.mgr)

	p.HSTSIncludeSubDomains = ParamItem{
		Key:          "proxy.http.hstsIncludeSubDomains",
		DefaultValue: "false",
		Version:      "2.6.0",
		Doc:          "Include subdomains in Strict-Transport-Security",
		Export:       true,
		Sensitivity:  Sensitive,
	}
	p.HSTSIncludeSubDomains.Init(base.mgr)

	p.EnableHSTS = ParamItem{
		Key:          "proxy.http.enableHSTS",
		DefaultValue: "false",
		Version:      "2.6.0",
		Doc:          "Whether to enable setting the Strict-Transport-Security header",
		Export:       true,
		Sensitivity:  Sensitive,
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

// HTTPRequestBudgetPolicy is a native-free snapshot of the REST timeout
// policy. The HTTP server parses it at startup and passes it to the transport
// adapter; parsing alone does not install any request deadlines.
type HTTPRequestBudgetPolicy struct {
	OverallTimeoutBudget      time.Duration
	ReadHeaderTimeout         time.Duration
	MaxConnectionIdleInterval time.Duration
}

// ParseRequestBudgetPolicy validates migration from the old independent
// timeout settings to the REST request budget.
func (p *httpConfig) ParseRequestBudgetPolicy() (HTTPRequestBudgetPolicy, error) {
	var policy HTTPRequestBudgetPolicy
	read := func(item *ParamItem) (time.Duration, bool, error) {
		_, raw, err := item.manager.GetConfig(item.Key)
		present := err == nil
		if err != nil {
			if !errors.Is(err, config.ErrKeyNotFound) {
				return 0, false, merr.WrapErrServiceInternalErr(err, "cannot read %s", item.Key)
			}
			raw = item.DefaultValue
		}
		if raw == "" {
			if present {
				return 0, true, merr.WrapErrServiceInternalMsg("%s must not be empty; configure a duration such as 0s explicitly", item.Key)
			}
			return 0, false, nil
		}
		duration, parseErr := time.ParseDuration(raw)
		if parseErr != nil {
			return 0, present, merr.WrapErrServiceInternalErr(parseErr, "invalid %s duration %q", item.Key, raw)
		}
		return duration, present, nil
	}
	_, legacyRequestRaw, legacyRequestErr := p.RequestTimeoutMs.manager.GetConfig(p.RequestTimeoutMs.Key)
	if legacyRequestErr != nil && !errors.Is(legacyRequestErr, config.ErrKeyNotFound) {
		return policy, merr.WrapErrServiceInternalErr(legacyRequestErr, "cannot read %s", p.RequestTimeoutMs.Key)
	}
	if legacyRequestErr == nil {
		legacyRequestMs, parseErr := strconv.ParseInt(legacyRequestRaw, 10, 64)
		defaultRequestMs, _ := strconv.ParseInt(p.RequestTimeoutMs.DefaultValue, 10, 64)
		if parseErr != nil || legacyRequestMs != defaultRequestMs {
			return policy, merr.WrapErrServiceInternalMsg("%s is deprecated; remove its override and configure proxy.http.overallTimeoutBudget explicitly", p.RequestTimeoutMs.Key)
		}
	}

	for _, legacy := range []*ParamItem{&p.ReadTimeout, &p.WriteTimeout} {
		value, _, err := read(legacy)
		if err != nil {
			return policy, err
		}
		if value != 0 {
			return policy, merr.WrapErrServiceInternalMsg("%s is a nonzero independent budget; clear it and configure proxy.http.overallTimeoutBudget explicitly", legacy.Key)
		}
	}

	var err error
	policy.OverallTimeoutBudget, _, err = read(&p.OverallTimeoutBudget)
	if err != nil {
		return policy, err
	}
	if policy.OverallTimeoutBudget <= 0 {
		return policy, merr.WrapErrServiceInternalMsg("%s must be a positive duration", p.OverallTimeoutBudget.Key)
	}
	policy.ReadHeaderTimeout, _, err = read(&p.ReadHeaderTimeout)
	if err != nil {
		return policy, err
	}
	if policy.ReadHeaderTimeout <= 0 {
		return policy, merr.WrapErrServiceInternalMsg("%s must be positive", p.ReadHeaderTimeout.Key)
	}
	legacyIdle, legacyPresent, err := read(&p.IdleTimeout)
	if err != nil {
		return policy, err
	}
	newIdle, newPresent, err := read(&p.MaxConnectionIdleInterval)
	if err != nil {
		return policy, err
	}
	if legacyIdle < 0 || newIdle < 0 {
		return policy, merr.WrapErrServiceInternalMsg("%s and %s must be nonnegative", p.IdleTimeout.Key, p.MaxConnectionIdleInterval.Key)
	}
	if legacyPresent && newPresent && legacyIdle != newIdle {
		return policy, merr.WrapErrServiceInternalMsg("%s conflicts with %s; remove the old idleTimeout setting or make both durations equivalent", p.IdleTimeout.Key, p.MaxConnectionIdleInterval.Key)
	}
	policy.MaxConnectionIdleInterval = legacyIdle
	if newPresent {
		policy.MaxConnectionIdleInterval = newIdle
	}
	return policy, nil
}

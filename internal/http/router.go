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

package http

// HealthzRouterPath is default path for check health state.
const HealthzRouterPath = "/healthz"

// LogLevelRouterPath is path for Get and Update log level at runtime.
const LogLevelRouterPath = "/log/level"

// EventLogRouterPath is path for eventlog control.
const EventLogRouterPath = "/eventlog"

// ExprPath is path for expression.
const ExprPath = "/expr"

// StaticPath is path for the static view.
const StaticPath = "/static/"

const RootPath = "/"

// Prometheus restful api path
const (
	MetricsPath        = "/metrics"
	MetricsDefaultPath = "/metrics_default"
)

// for every component, register it's own api to trigger stop and check ready
const (
	RouteTriggerStopPath     = "/management/stop"
	RouteCheckComponentReady = "/management/check/ready"
)

// proxy management restful api root path
const (
	RouteGcPause  = "/management/datacoord/garbage_collection/pause"
	RouteGcResume = "/management/datacoord/garbage_collection/resume"

	RouteSuspendQueryCoordBalance = "/management/querycoord/balance/suspend"
	RouteResumeQueryCoordBalance  = "/management/querycoord/balance/resume"
	RouteQueryCoordBalanceStatus  = "/management/querycoord/balance/status"
	RouteTransferSegment          = "/management/querycoord/transfer/segment"
	RouteTransferChannel          = "/management/querycoord/transfer/channel"

	RouteSuspendQueryNode           = "/management/querycoord/node/suspend"
	RouteResumeQueryNode            = "/management/querycoord/node/resume"
	RouteListQueryNode              = "/management/querycoord/node/list"
	RouteGetQueryNodeDistribution   = "/management/querycoord/distribution/get"
	RouteCheckQueryNodeDistribution = "/management/querycoord/distribution/check"
)

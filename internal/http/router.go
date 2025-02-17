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
	RouteWebUI               = "/webui/"
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

// for WebUI restful api root path
const (
	// ClusterInfoPath is the path to get cluster information.
	ClusterInfoPath = "/_cluster/info"
	// ClusterConfigsPath is the path to get cluster configurations.
	ClusterConfigsPath = "/_cluster/configs"
	// ClusterClientsPath is the path to get connected clients.
	ClusterClientsPath = "/_cluster/clients"
	// ClusterDependenciesPath is the path to get cluster dependencies.
	ClusterDependenciesPath = "/_cluster/dependencies"
	// HookConfigsPath is the path to get hook configurations.
	HookConfigsPath = "/_hook/configs"
	// SlowQueryPath is the path to get slow queries metrics
	SlowQueryPath = "/_cluster/slow_query"

	// QCDistPath is the path to get QueryCoord distribution.
	QCDistPath = "/_qc/dist"
	// QCTargetPath is the path to get QueryCoord target.
	QCTargetPath = "/_qc/target"
	// QCReplicaPath is the path to get QueryCoord replica.
	QCReplicaPath = "/_qc/replica"
	// QCResourceGroupPath is the path to get QueryCoord resource group.
	QCResourceGroupPath = "/_qc/resource_group"
	// QCAllTasksPath is the path to get all tasks in QueryCoord.
	QCAllTasksPath = "/_qc/tasks"
	// QCSegmentsPath is the path to get segments in QueryCoord.
	QCSegmentsPath = "/_qc/segments"

	// QNSegmentsPath is the path to get segments in QueryNode.
	QNSegmentsPath = "/_qn/segments"
	// QNChannelsPath is the path to get channels in QueryNode.
	QNChannelsPath = "/_qn/channels"

	// DCDistPath is the path to get all segments and channels distribution in DataCoord.
	DCDistPath = "/_dc/dist"
	// DCImportTasksPath is the path to get import tasks in DataCoord.
	DCImportTasksPath = "/_dc/tasks/import"
	// DCCompactionTasksPath is the path to get compaction tasks in DataCoord.
	DCCompactionTasksPath = "/_dc/tasks/compaction"
	// DCBuildIndexTasksPath is the path to get build index tasks in DataCoord.
	DCBuildIndexTasksPath = "/_dc/tasks/build_index"
	// DCSegmentsPath is the path to get segments in DataCoord.
	DCSegmentsPath = "/_dc/segments"

	// DNSyncTasksPath is the path to get sync tasks in DataNode.
	DNSyncTasksPath = "/_dn/tasks/sync"
	// DNSegmentsPath is the path to get segments in DataNode.
	DNSegmentsPath = "/_dn/segments"
	// DNChannelsPath is the path to get channels in DataNode.
	DNChannelsPath = "/_dn/channels"

	// DatabaseListPath is the path to get all databases.
	DatabaseListPath = "/_db/list"
	//	DatabaseDescPath is the path to get database description.
	DatabaseDescPath = "/_db/desc"

	// CollectionListPath is the path to get all collections.
	CollectionListPath = "/_collection/list"
	// CollectionDescPath is the path to get collection description.
	CollectionDescPath = "/_collection/desc"

	// IndexListPath is the path to get all indexes.
	IndexListPath = "/_index/list"
)

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

package rootcoord

import (
	"time"

	"github.com/milvus-io/milvus/internal/rootcoord/telemetry"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// telemetryConfigFromParams reads the client-telemetry settings out of the param table.
//
// It lives here rather than in the telemetry package so that package stays independent of
// paramtable and can be exercised with plain structs. RootCoord reads the values once, when
// it builds the manager, so a change takes effect on restart -- which is why the params are
// declared non-refreshable.
//
// Values that cannot mean what they say are not corrected here; TelemetryConfig.normalize
// owns that, so the same rules apply no matter how a config was built.
func telemetryConfigFromParams() *telemetry.TelemetryConfig {
	cfg := &paramtable.Get().RootCoordCfg
	return &telemetry.TelemetryConfig{
		CleanupInterval:            cfg.ClientTelemetryCleanupInterval.GetAsDuration(time.Second),
		InactiveClientThreshold:    cfg.ClientTelemetryInactiveClientThreshold.GetAsDuration(time.Second),
		ClientStatusThreshold:      cfg.ClientTelemetryClientStatusThreshold.GetAsDuration(time.Second),
		CommandCleanupTimeout:      cfg.ClientTelemetryCommandCleanupTimeout.GetAsDuration(time.Second),
		MaxMetricsPerClient:        cfg.ClientTelemetryMaxMetricsPerClient.GetAsInt(),
		MaxOperationTypesPerClient: cfg.ClientTelemetryMaxOperationTypesPerClient.GetAsInt(),
		MaxClientsInMemory:         cfg.ClientTelemetryMaxClientsInMemory.GetAsInt(),
		RetainedWindows:            cfg.ClientTelemetryRetainedWindows.GetAsInt(),
	}
}

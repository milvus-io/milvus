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

package coordinator

import (
	"fmt"
	"net/http"
	"sort"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/balance"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

// LoadConfigComplianceState represents the compliance state of replica load configuration
type LoadConfigComplianceState string

const (
	// LoadConfigComplianceStateReady indicates all effective collection targets, cleanup and WAL obligations are satisfied
	LoadConfigComplianceStateReady LoadConfigComplianceState = "Ready"
	// LoadConfigComplianceStateNotReady indicates at least one load-config compliance obligation is unmet
	LoadConfigComplianceStateNotReady LoadConfigComplianceState = "NotReady"
)

// ResourceGroupComplianceState represents the compliance state of a single resource group
type ResourceGroupComplianceState struct {
	ResourceGroup string                    `json:"resourceGroup"`
	State         LoadConfigComplianceState `json:"state"`
	Reason        string                    `json:"reason,omitempty"`
}

// LoadConfigComplianceResponse is the response structure for replica load config compliance check.
// ResourceGroups is a pointer so per-resource-group output can always emit the array (even when
// empty, e.g. no collections loaded) while summary output omits the field entirely.
type LoadConfigComplianceResponse struct {
	State          LoadConfigComplianceState       `json:"state"`
	Reason         string                          `json:"reason,omitempty"`
	ResourceGroups *[]ResourceGroupComplianceState `json:"resourceGroups,omitempty"`
}

// HandleReplicaLoadConfigCompliance checks query and WAL obligations independently.
// Both output modes use the same result; output only controls its presentation.
func (s *mixCoordImpl) HandleReplicaLoadConfigCompliance(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		writeJSONError(w, "Method not allowed, use GET", http.StatusMethodNotAllowed)
		return
	}
	output := req.URL.Query().Get("output")
	if output != "" && output != "summary" && output != "per_resource_group" {
		writeJSONError(w, fmt.Sprintf("invalid output %q, expected one of: summary, per_resource_group", output), http.StatusBadRequest)
		return
	}
	ctx := req.Context()
	b, err := balance.GetWithContext(ctx)
	if err != nil {
		writeJSONError(w, fmt.Sprintf("failed to get streaming balancer: %s", err.Error()), http.StatusInternalServerError)
		return
	}
	groups, err := balancer.CheckWALPlacement(ctx, b, Params.StreamingCfg.PrimaryResourceGroup.GetValue())
	if err != nil {
		writeJSONError(w, fmt.Sprintf("failed to check WAL placement: %s", err.Error()), http.StatusInternalServerError)
		return
	}
	queryGroups, err := s.queryCoordServer.GetLoadConfigCompliance(ctx)
	if err != nil {
		writeJSONError(w, fmt.Sprintf("failed to check collection load config: %s", err.Error()), http.StatusInternalServerError)
		return
	}
	for rg, reason := range queryGroups {
		if groups[rg] == "" {
			groups[rg] = reason
		}
	}
	names := make([]string, 0, len(groups))
	for rg := range groups {
		names = append(names, rg)
	}
	sort.Strings(names)
	response := LoadConfigComplianceResponse{State: LoadConfigComplianceStateReady}
	states := make([]ResourceGroupComplianceState, 0, len(groups))
	for _, rg := range names {
		reason := groups[rg]
		state := LoadConfigComplianceStateReady
		if reason != "" {
			state = LoadConfigComplianceStateNotReady
			response.State = state
			if response.Reason == "" {
				response.Reason = reason
			}
			mlog.Info(ctx, "load config compliance violation", mlog.String("resourceGroup", rg), mlog.String("reason", reason))
		}
		if rg != "" {
			states = append(states, ResourceGroupComplianceState{ResourceGroup: rg, State: state, Reason: reason})
		}
	}
	if output == "per_resource_group" {
		response.ResourceGroups = &states
	}
	writeJSONResponse(w, http.StatusOK, response)
}

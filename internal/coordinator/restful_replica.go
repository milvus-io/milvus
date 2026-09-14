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

// HandleReplicaLoadConfigCompliance serves GET /management/replica/loadconfig/compliance.
//
// A resource group (RG) is Ready only when all of its obligations are satisfied:
//   - Each relevant collection has the expected number of replicas in that RG,
//     according to the collection's effective load target.
//   - Every replica is query-visible and every shard in it is serviceable.
//   - Replicas and segment/channel resources that should be released are gone,
//     including resources on draining RO/RO SQ nodes and resources of collections
//     whose load registration has already been removed.
//   - Its WAL migration obligations, both incoming and outgoing, are complete.
//
// Effective targets are independent of observed replica placement. A valid
// cluster-level load configuration overrides a collection's persisted target only
// when the collection is cluster-managed or forceOverride is enabled. Otherwise,
// the persisted collection target applies; user-specified collections are still
// checked. A single configured RG receives all requested replicas, while repeated
// RG names in a multi-entry configuration specify per-RG replica counts.
//
// When streaming.primaryResourceGroup is configured, every RW pchannel must be
// assigned in that RG. Uninitialized, assigning, unavailable, or misplaced WAL
// channels block the target RG and any identifiable current/historical source RG
// with unfinished migration obligations, even if no collections are loaded.
// Without a primary RG, this endpoint imposes no primary-RG placement requirement;
// RO pchannels are outside that placement check.
//
// Global Ready requires every involved RG to be Ready, all collection/global
// constraints (including total replica counts) to hold, and no unknown effective
// target or unattributable violation. Node quota and node ownership convergence
// are not additional readiness conditions; residual resources on draining nodes
// still block readiness, whereas empty RO node metadata alone does not.
//
// Involved RGs come from effective targets, actual replicas, residual resources,
// and WAL obligations. An entirely drained RG with no remaining obligation may
// disappear from the report. WAL violations and collection count mismatches must
// not skip checks needed to establish other RGs' readiness.
//
// Both summary (the default) and per_resource_group use the same global result;
// output only controls presentation. Per-RG output includes a sorted array (even
// when empty), retaining the first reason per RG. Global reasons take precedence
// over RG reasons; WAL reasons take precedence within an RG. Metadata read errors
// return HTTP 500 rather than a readiness report. Results reflect asynchronously
// observed state, not an atomic snapshot across components or a future guarantee.
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

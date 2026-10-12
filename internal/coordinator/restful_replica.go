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
	"strings"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/balance"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// LoadConfigComplianceState represents the compliance state of replica load configuration
type LoadConfigComplianceState string

const (
	// LoadConfigComplianceStateReady indicates all collections meet the cluster-level replica configuration
	LoadConfigComplianceStateReady LoadConfigComplianceState = "Ready"
	// LoadConfigComplianceStateNotReady indicates some collections do not meet the cluster-level replica configuration
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
// It reports convergence of queryCoord.clusterLevelLoadReplicaNumber,
// queryCoord.clusterLevelLoadResourceGroups, their force-override option, and
// streaming.primaryResourceGroup, using the coordinator's observed state.
//
// An RG is Ready when applicable cluster configuration has the expected replica
// count there, every existing replica is query-visible and serviceable on all
// shards, the existing release check finds no resources left outside current
// replicas, and its incoming/outgoing WAL placement obligations are complete.
// A single configured RG receives all requested replicas; repeated RG entries
// specify replica counts. User-specified collections are exempt from cluster
// count/distribution checks unless force override is enabled, but their existing
// replicas still undergo serviceability, visibility and release checks.
//
// Global Ready requires all involved RGs to be Ready and no global violation.
// A collection count mismatch must not mark healthy RGs as failed or skip later
// checks; WAL violations must not skip collection checks. Unknown attribution
// blocks global Ready. Malformed, partially configured cluster targets are
// NotReady; clearing both cluster settings disables their count/distribution
// requirements. Only RGs involved in applicable targets, replicas, release
// violations or WAL placement are reported.
// Unlike initial collection loading, this endpoint does not fill in missing
// cluster settings: replica count and RGs must be configured together. Count-only
// and RG-only settings remain NotReady even if existing replicas happen to match,
// because they do not define a target accepted by the dynamic load-config watcher.
//
// With a primary RG configured, all RW pchannels must be ASSIGNED there, even
// with no loaded collections. Current and unfinished historical source RGs also
// have migration obligations. RO pchannels are outside this placement check.
//
// This is a configuration-convergence report over loaded collections, not a
// general audit of node quotas, dynamic node/replica RG transfers, collection
// release lifecycles or historical per-collection load intents. Those mechanisms
// retain their existing semantics; this endpoint does not modify them.
//
// Both output modes execute the same checks and return the same global state
// for the same observed configuration and runtime state. per_resource_group
// adds a sorted array (including when empty). When multiple checks fail, the
// reported reason may vary between requests or output modes without affecting
// the Ready verdict. Each RG retains its first observed violation; global
// reasons take precedence. Metadata read failures return HTTP 500. Observations
// and configuration refresh are asynchronous; the report is not a
// cross-component atomic snapshot.
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
	reasons := make(map[string]string)
	record := func(rg, reason string) {
		if reasons[rg] == "" {
			reasons[rg] = reason
		}
	}
	// SQN release attribution needs all streaming nodes, including frozen nodes,
	// even when no primary RG is configured. Reuse this snapshot for WAL checks.
	b, err := balance.GetWithContext(ctx)
	if err != nil {
		writeJSONError(w, fmt.Sprintf("failed to get streaming balancer: %s", err), http.StatusInternalServerError)
		return
	}
	nodes, err := b.GetAllStreamingNodes(ctx)
	if err != nil {
		writeJSONError(w, fmt.Sprintf("failed to get streaming nodes: %s", err), http.StatusInternalServerError)
		return
	}
	streamingNodeRGs := make(map[int64]string, len(nodes))
	for id, node := range nodes {
		streamingNodeRGs[id] = node.ResourceGroup
	}
	// Use the full view: Relations omits uninitialized/assigning/unavailable WALs.
	primaryRG := Params.StreamingCfg.PrimaryResourceGroup.GetValue()
	if primaryRG != "" {
		assignment, err := b.GetLatestChannelAssignment()
		if err != nil {
			writeJSONError(w, fmt.Sprintf("failed to get WAL assignment: %s", err), http.StatusInternalServerError)
			return
		}
		if assignment.PChannelView == nil {
			writeJSONError(w, "WAL channel view is not initialized", http.StatusInternalServerError)
			return
		}
		record(primaryRG, "")
		ids := make([]types.ChannelID, 0, len(assignment.PChannelView.Channels))
		for id := range assignment.PChannelView.Channels {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i].String() < ids[j].String() })
		for _, id := range ids {
			ch := assignment.PChannelView.Channels[id]
			if ch.ChannelInfo().AccessMode != types.AccessModeRW {
				continue
			}
			owner, known := nodes[ch.CurrentServerID()]
			if ch.IsAssigned() && known && owner.ResourceGroup == primaryRG {
				continue
			}
			reason := fmt.Sprintf("WAL placement: pchannel %s is %s, expected assigned in primary rg=%s", ch.Name(), ch.State(), primaryRG)
			record(primaryRG, reason)
			if known {
				record(owner.ResourceGroup, reason)
			} else if ch.CurrentServerID() != 0 {
				record("", reason)
			}
			for _, previous := range ch.AssignHistories() {
				if previous.Channel.AccessMode != types.AccessModeRW {
					continue
				}
				if owner, ok := nodes[previous.Node.ServerID]; ok {
					record(owner.ResourceGroup, reason)
				} else {
					record("", reason)
				}
			}
		}
	}
	count := Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.GetAsInt()
	configuredRGs := Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.GetAsStrings()
	force := Params.QueryCoordCfg.ClusterLevelLoadForceOverrideUserReplicaMode.GetAsBool()
	validConfig := count > 0 && (len(configuredRGs) == 1 || len(configuredRGs) == count)
	if (count != 0 || len(configuredRGs) != 0) && !validConfig {
		record("", "invalid cluster load configuration: positive replica count and one RG or one RG entry per replica are required")
	}
	expectedRGs := configuredRGs
	if validConfig && len(configuredRGs) == 1 {
		expectedRGs = make([]string, count)
		for i := range expectedRGs {
			expectedRGs[i] = configuredRGs[0]
		}
	}
	showResp, err := s.ShowLoadCollections(ctx, &querypb.ShowCollectionsRequest{Base: commonpbutil.NewMsgBase()})
	if err := merr.CheckRPCCall(showResp, err); err != nil {
		writeJSONError(w, fmt.Sprintf("failed to get collections: %s", err), http.StatusInternalServerError)
		return
	}
	collectionIDs := append([]int64(nil), showResp.GetCollectionIDs()...)
	sort.Slice(collectionIDs, func(i, j int) bool { return collectionIDs[i] < collectionIDs[j] })
	for _, id := range collectionIDs {
		replicas := s.queryCoordServer.GetInternalReplicasByCollection(ctx, id)
		actualRGs := make([]string, 0, len(replicas))
		rgByReplica := make(map[int64]string, len(replicas))
		for _, replica := range replicas {
			rg := replica.GetResourceGroup()
			record(rg, "")
			actualRGs = append(actualRGs, rg)
			rgByReplica[replica.GetID()] = rg
		}
		if validConfig && (force || !s.queryCoordServer.IsCollectionUserSpecifiedReplicaMode(ctx, id)) {
			for _, rg := range configuredRGs {
				record(rg, "")
			}
			if len(replicas) != count {
				record("", fmt.Sprintf("collection %d: replica count mismatch (expected %d, actual %d)", id, count, len(replicas)))
			}
			if reason, groups := s.validateRGDistribution(actualRGs, expectedRGs, "resource group", id); reason != "" {
				for _, rg := range groups {
					record(rg, reason)
				}
			}
		}
		if len(replicas) == 0 {
			record("", fmt.Sprintf("collection %d: no replica found", id))
		}
		errors := s.queryCoordServer.CheckReplicasServiceable(ctx, id)
		replicaIDs := make([]int64, 0, len(errors))
		for replicaID := range errors {
			replicaIDs = append(replicaIDs, replicaID)
		}
		sort.Slice(replicaIDs, func(i, j int) bool { return replicaIDs[i] < replicaIDs[j] })
		for _, replicaID := range replicaIDs {
			record(rgByReplica[replicaID], fmt.Sprintf("collection %d: %s", id, errors[replicaID]))
		}
		for _, replica := range replicas {
			if !replica.IsQueryVisible() {
				record(replica.GetResourceGroup(), fmt.Sprintf("collection %d: replica %d (rg=%s) is not query visible", id, replica.GetID(), replica.GetResourceGroup()))
			}
		}
		for rg, leaked := range s.queryCoordServer.GetLeakedResourcesByCollectionPerRG(ctx, id, streamingNodeRGs) {
			if leaked > 0 {
				record(rg, fmt.Sprintf("collection %d: resources not fully released (leaked=%d)", id, leaked))
			}
		}
	}
	names := make([]string, 0, len(reasons))
	for rg := range reasons {
		names = append(names, rg)
	}
	sort.Strings(names)
	response := LoadConfigComplianceResponse{State: LoadConfigComplianceStateReady}
	states := make([]ResourceGroupComplianceState, 0, len(reasons))
	for _, rg := range names {
		reason := reasons[rg]
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

// validateRGDistribution validates that replicas are distributed according to cluster config.
// It returns a non-empty reason string and the set of offending resource groups when validation
// fails (groups whose replica count does not match the expected count); an empty reason means
// the distribution matches. The offending set lets per-resource-group reporting attribute the
// violation to exactly the groups that are wrong, rather than flagging every configured group.
func (s *mixCoordImpl) validateRGDistribution(
	actualRGs []string,
	expectedRGs []string,
	rgType string,
	collectionID int64,
) (string, []string) {
	counts := make(map[string]int, len(actualRGs))
	for _, rg := range actualRGs {
		counts[rg]++
	}
	for _, rg := range expectedRGs {
		counts[rg]--
	}
	var diffs []string
	var offendingRGs []string
	for rg, cnt := range counts {
		if cnt != 0 {
			diffs = append(diffs, fmt.Sprintf("%s:%+d", rg, cnt))
			offendingRGs = append(offendingRGs, rg)
		}
	}
	if len(diffs) > 0 {
		// Sort both so the delta string and the offending set are deterministic across polls.
		sort.Strings(diffs)
		sort.Strings(offendingRGs)
		return fmt.Sprintf("collection %d: %s mismatch (delta: %s)", collectionID, rgType, strings.Join(diffs, ", ")), offendingRGs
	}
	return "", nil
}

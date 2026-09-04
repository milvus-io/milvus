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

// LoadConfigComplianceResponse is the response structure for replica load config compliance check
type LoadConfigComplianceResponse struct {
	State          LoadConfigComplianceState      `json:"state"`
	Reason         string                         `json:"reason,omitempty"`
	ResourceGroups []ResourceGroupComplianceState `json:"resourceGroups,omitempty"`
}

// HandleReplicaLoadConfigCompliance checks if all loaded collections meet the cluster-level replica configuration requirements.
//
// Optional query parameter "per_resource_group=true" switches the check from fail-fast to per-resource-group
// reporting: every collection is still checked fully (no early return on the first violation) and the response
// reports the compliance state of each involved resource group, including the reason for any not-ready one.
func (s *mixCoordImpl) HandleReplicaLoadConfigCompliance(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		writeJSONError(w, "Method not allowed, use GET", http.StatusMethodNotAllowed)
		return
	}

	ctx := req.Context()
	logger := mlog.With(mlog.String("handler", "ReplicaLoadConfigCompliance"))

	// When enabled, keep checking all collections after a violation and report readiness per resource group
	// instead of failing fast on the first violation.
	perResourceGroup := req.URL.Query().Get("per_resource_group") == "true"

	// Cluster-level check: WAL is fully migrated onto the configured primary resource group.
	// Short-circuit before reading config / loading collections — a WAL-layout issue affects
	// every collection and is independent of per-collection replica/RG config.
	if b, err := balance.GetWithContext(ctx); err != nil {
		writeJSONError(w, fmt.Sprintf("failed to get streaming balancer: %s", err.Error()), http.StatusInternalServerError)
		return
	} else if err := b.ConfirmPrimaryResourceGroupReady(ctx); err != nil {
		reason := fmt.Sprintf("WAL placement: %s", err.Error())
		logger.Info(ctx, "WAL not fully placed on primary resource group", mlog.String("reason", reason))
		s.writeComplianceResponse(w, LoadConfigComplianceStateNotReady, reason)
		return
	}

	// Get cluster-level configuration
	clusterReplicaNum := Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.GetAsInt()
	clusterResourceGroups := Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.GetAsStrings()
	forceOverrideUserReplicaMode := Params.QueryCoordCfg.ClusterLevelLoadForceOverrideUserReplicaMode.GetAsBool()

	logger.Info(ctx, "checking replica load config compliance",
		mlog.Int("clusterReplicaNum", clusterReplicaNum),
		mlog.Strings("clusterResourceGroups", clusterResourceGroups),
		mlog.Bool("forceOverrideUserReplicaMode", forceOverrideUserReplicaMode))

	// Use ShowLoadCollections to get all loaded collections
	showResp, err := s.ShowLoadCollections(ctx, &querypb.ShowCollectionsRequest{
		Base: commonpbutil.NewMsgBase(),
	})
	if err := merr.CheckRPCCall(showResp, err); err != nil {
		logger.Warn(ctx, "failed to show collections", mlog.Err(err))
		writeJSONError(w, fmt.Sprintf("failed to get collections: %s", err.Error()), http.StatusInternalServerError)
		return
	}

	// Per-resource-group mode state: the first reason why each resource group is not ready yet, and the set of
	// resource groups to report (configured ones plus any that actually host replicas).
	resourceGroupErrors := make(map[string]string)
	resourceGroups := make(map[string]struct{})
	for _, rg := range clusterResourceGroups {
		resourceGroups[rg] = struct{}{}
	}
	// globalReason keeps the first violation that cannot be attributed to any specific resource group.
	var globalReason string

	// handleFailure records a violation. In fail-fast mode it writes the NotReady response and returns true so
	// the caller stops; in per-resource-group mode it keeps only the first reason per resource group (or as the
	// global reason when no resource group applies) and returns false so checking continues.
	handleFailure := func(rgs []string, reason string) bool {
		if !perResourceGroup {
			s.writeComplianceResponse(w, LoadConfigComplianceStateNotReady, reason)
			return true
		}
		if len(rgs) == 0 {
			if globalReason == "" {
				globalReason = reason
			}
			return false
		}
		for _, rg := range rgs {
			resourceGroups[rg] = struct{}{}
			if _, ok := resourceGroupErrors[rg]; !ok {
				resourceGroupErrors[rg] = reason
			}
		}
		return false
	}

	// Check each collection
	for _, collectionID := range showResp.GetCollectionIDs() {
		skipClusterLevelConfigChecks := !forceOverrideUserReplicaMode && s.queryCoordServer.IsCollectionUserSpecifiedReplicaMode(ctx, collectionID)

		// Get internal replicas from QueryCoord meta which contains StreamingResourceGroup field
		internalReplicas := s.queryCoordServer.GetInternalReplicasByCollection(ctx, collectionID)

		actualRGs := make([]string, 0, len(internalReplicas))
		for _, replica := range internalReplicas {
			actualRGs = append(actualRGs, replica.GetResourceGroup())
			resourceGroups[replica.GetResourceGroup()] = struct{}{}
		}
		// Resource groups this collection's violations are attributed to: the groups actually hosting its
		// replicas, falling back to the cluster-level expected groups when the collection has no replicas yet.
		collectionRGs := actualRGs
		if len(collectionRGs) == 0 {
			collectionRGs = clusterResourceGroups
		}

		// Check replica count matches exactly — the replica meta must already reflect
		// the configured count before we inspect serviceability/leaks.
		if !skipClusterLevelConfigChecks && clusterReplicaNum > 0 && len(internalReplicas) != clusterReplicaNum {
			reason := fmt.Sprintf("collection %d: replica count mismatch (expected %d, actual %d)",
				collectionID, clusterReplicaNum, len(internalReplicas))
			logger.Info(ctx, "collection replica count does not match cluster requirement", mlog.String("reason", reason))
			if handleFailure(collectionRGs, reason) {
				return
			}
		}

		if !skipClusterLevelConfigChecks && len(clusterResourceGroups) > 0 {
			// Check resource groups - collect actual RGs from replicas
			if reason := s.validateRGDistribution(actualRGs, clusterResourceGroups,
				"resource group", collectionID); reason != "" {
				logger.Info(ctx, "collection resource group distribution does not match cluster requirement", mlog.String("reason", reason))
				if handleFailure(clusterResourceGroups, reason) {
					return
				}
			}
		}

		// Now that replica count and RG distribution match, verify every replica actually
		// has a serviceable shard leader for every channel. This live dist check avoids
		// the stale CollectionObserver-persisted LoadPercentage that can falsely report
		// 100% during scale-up/scale-down transitions.
		if err := s.queryCoordServer.CheckAllReplicasServiceable(ctx, collectionID); err != nil {
			reason := fmt.Sprintf("collection %d: %s", collectionID, err.Error())
			logger.Info(ctx, "collection not serviceable", mlog.String("reason", reason))
			if handleFailure(collectionRGs, reason) {
				return
			}
		}

		for _, replica := range internalReplicas {
			if !replica.IsQueryVisible() {
				reason := fmt.Sprintf("collection %d: replica %d (rg=%s) is not query visible",
					collectionID, replica.GetID(), replica.GetResourceGroup())
				logger.Info(ctx, "collection has query-invisible replica", mlog.String("reason", reason))
				if handleFailure([]string{replica.GetResourceGroup()}, reason) {
					return
				}
			}
		}

		// Check that physical resources have been released from querynodes no longer
		// part of any replica. During scale-down a decommissioned replica's querynode may
		// still hold segments/channels while release is in flight; compliance must wait for
		// that to finish before signaling Ready, otherwise callers may terminate nodes while
		// they are still serving or holding state.
		leakedSegments, leakedChannels := s.queryCoordServer.GetLeakedResourcesByCollection(ctx, collectionID)
		if leakedSegments > 0 || leakedChannels > 0 {
			reason := fmt.Sprintf("collection %d: resources not fully released (leaked segments=%d, channels=%d)",
				collectionID, leakedSegments, leakedChannels)
			logger.Info(ctx, "collection has leaked resources on non-replica nodes", mlog.String("reason", reason))
			if handleFailure(collectionRGs, reason) {
				return
			}
		}
	}

	if perResourceGroup {
		s.writePerResourceGroupComplianceResponse(w, resourceGroups, resourceGroupErrors, globalReason)
		return
	}

	// All collections meet the requirements
	logger.Info(ctx, "all collections meet replica load config compliance requirements", mlog.Int("totalCollections", len(showResp.GetCollectionIDs())))
	s.writeComplianceResponse(w, LoadConfigComplianceStateReady, "")
}

// writePerResourceGroupComplianceResponse writes the per-resource-group compliance response: one entry per
// involved resource group with its Ready/NotReady state and, when not ready, the first reason collected for it.
// The top-level reason is likewise the first violation encountered (either the global one or the first
// not-ready resource group), keeping the response compact.
func (s *mixCoordImpl) writePerResourceGroupComplianceResponse(w http.ResponseWriter, resourceGroups map[string]struct{}, resourceGroupErrors map[string]string, globalReason string) {
	rgs := make([]string, 0, len(resourceGroups))
	for rg := range resourceGroups {
		rgs = append(rgs, rg)
	}
	sort.Strings(rgs)

	state := LoadConfigComplianceStateReady
	firstReason := globalReason
	rgStates := make([]ResourceGroupComplianceState, 0, len(rgs))
	for _, rg := range rgs {
		rgState := ResourceGroupComplianceState{ResourceGroup: rg, State: LoadConfigComplianceStateReady}
		if reason, ok := resourceGroupErrors[rg]; ok {
			rgState.State = LoadConfigComplianceStateNotReady
			rgState.Reason = reason
			state = LoadConfigComplianceStateNotReady
			if firstReason == "" {
				firstReason = reason
			}
		}
		rgStates = append(rgStates, rgState)
	}

	resp := LoadConfigComplianceResponse{
		State:          state,
		ResourceGroups: rgStates,
	}
	if firstReason != "" {
		resp.Reason = firstReason
	}
	writeJSONResponse(w, http.StatusOK, resp)
}

// writeComplianceResponse writes the compliance check response
func (s *mixCoordImpl) writeComplianceResponse(w http.ResponseWriter, state LoadConfigComplianceState, reason string) {
	resp := LoadConfigComplianceResponse{
		State: state,
	}
	if reason != "" {
		resp.Reason = reason
	}

	writeJSONResponse(w, http.StatusOK, resp)
}

// validateRGDistribution validates that replicas are distributed according to cluster config
// Returns reason string if validation fails, empty string if validation passes
func (s *mixCoordImpl) validateRGDistribution(
	actualRGs []string,
	expectedRGs []string,
	rgType string,
	collectionID int64,
) string {
	counts := make(map[string]int, len(actualRGs))
	for _, rg := range actualRGs {
		counts[rg]++
	}
	for _, rg := range expectedRGs {
		counts[rg]--
	}
	var diffs []string
	for rg, cnt := range counts {
		if cnt != 0 {
			diffs = append(diffs, fmt.Sprintf("%s:%+d", rg, cnt))
		}
	}
	if len(diffs) > 0 {
		return fmt.Sprintf("collection %d: %s mismatch (delta: %s)", collectionID, rgType, strings.Join(diffs, ", "))
	}
	return ""
}

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
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
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

// complianceViolation records a single compliance violation: the reason and the resource groups it is
// attributed to. An empty rgs means the violation cannot be attributed to any specific resource group.
type complianceViolation struct {
	rgs    []string
	reason string
}

// collectPerResourceGroupErrors aggregates violations into per-resource-group reasons (first reason wins per
// group), the set of resource groups to report (seeded from initialRGs and widened by every group a violation
// is attributed to), and a global reason for violations that cannot be attributed to any group.
func collectPerResourceGroupErrors(violations []complianceViolation, initialRGs map[string]struct{}) (map[string]string, map[string]struct{}, string) {
	resourceGroupErrors := make(map[string]string)
	resourceGroups := make(map[string]struct{}, len(initialRGs))
	for rg := range initialRGs {
		resourceGroups[rg] = struct{}{}
	}
	var globalReason string
	for _, v := range violations {
		if len(v.rgs) == 0 {
			if globalReason == "" {
				globalReason = v.reason
			}
			continue
		}
		for _, rg := range v.rgs {
			if rg == "" {
				if globalReason == "" {
					globalReason = v.reason
				}
				continue
			}
			resourceGroups[rg] = struct{}{}
			if _, ok := resourceGroupErrors[rg]; !ok {
				resourceGroupErrors[rg] = v.reason
			}
		}
	}
	return resourceGroupErrors, resourceGroups, globalReason
}

// HandleReplicaLoadConfigCompliance checks if all loaded collections meet the cluster-level replica configuration requirements.
//
// Optional query parameter "output=per_resource_group" (kubectl "-o" style) switches the check from fail-fast to
// per-resource-group reporting: every collection is still checked fully (no early return on the first violation)
// and the response reports the compliance state of each involved resource group, including the reason for any
// not-ready one. The default output ("summary", or absent) keeps the fail-fast single-reason behavior.
func (s *mixCoordImpl) HandleReplicaLoadConfigCompliance(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		writeJSONError(w, "Method not allowed, use GET", http.StatusMethodNotAllowed)
		return
	}

	ctx := req.Context()
	logger := mlog.With(mlog.String("handler", "ReplicaLoadConfigCompliance"))

	// Validate the output selector up front so a typo is rejected instead of silently degrading
	// to the summary behavior (which could be misread as "no resource groups to worry about").
	output := req.URL.Query().Get("output")
	var perResourceGroup bool
	switch output {
	case "", "summary":
		perResourceGroup = false
	case "per_resource_group":
		perResourceGroup = true
	default:
		writeJSONError(w, fmt.Sprintf("invalid output %q, expected one of: summary, per_resource_group", output), http.StatusBadRequest)
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

	// Per-resource-group mode state: the set of resource groups to report. It is pre-seeded with every
	// resource group known to the cluster (configured ones plus all groups reported by QueryCoord meta),
	// so a group that currently hosts nothing still appears in the report instead of being
	// indistinguishable from "not involved".
	resourceGroups := make(map[string]struct{})
	if perResourceGroup {
		for _, rg := range clusterResourceGroups {
			resourceGroups[rg] = struct{}{}
		}
		if listResp, err := s.queryCoordServer.ListResourceGroups(ctx, &milvuspb.ListResourceGroupsRequest{}); err == nil && merr.Ok(listResp.GetStatus()) {
			for _, rg := range listResp.GetResourceGroups() {
				resourceGroups[rg] = struct{}{}
			}
		} else {
			logger.Warn(ctx, "failed to list resource groups, falling back to configured groups", mlog.Err(err))
		}
	}

	// All checks funnel their violations into one list; the response is written once at the end.
	// In summary mode the fast-fail behavior is preserved by reporting only the first violation;
	// in per-resource-group mode every violation is aggregated per resource group instead.
	var violations []complianceViolation
	record := func(rgs []string, reason string) {
		violations = append(violations, complianceViolation{rgs: rgs, reason: reason})
	}

	// Cluster-level check: WAL is fully migrated onto the configured primary resource group.
	// Short-circuit before loading collections — a WAL-layout issue affects every collection and is
	// independent of per-collection replica/RG config. In per-RG mode every known resource group is
	// reported NotReady with the WAL reason, since the condition is cluster-wide.
	if b, err := balance.GetWithContext(ctx); err != nil {
		writeJSONError(w, fmt.Sprintf("failed to get streaming balancer: %s", err.Error()), http.StatusInternalServerError)
		return
	} else if err := b.ConfirmPrimaryResourceGroupReady(ctx); err != nil {
		reason := fmt.Sprintf("WAL placement: %s", err.Error())
		logger.Info(ctx, "WAL not fully placed on primary resource group", mlog.String("reason", reason))
		if perResourceGroup {
			for rg := range resourceGroups {
				record([]string{rg}, reason)
			}
		}
		record(nil, reason)
		s.writeComplianceResult(ctx, logger, w, perResourceGroup, resourceGroups, violations, 0)
		return
	}

	// Use ShowLoadCollections to get all loaded collections
	showResp, err := s.ShowLoadCollections(ctx, &querypb.ShowCollectionsRequest{
		Base: commonpbutil.NewMsgBase(),
	})
	if err := merr.CheckRPCCall(showResp, err); err != nil {
		logger.Warn(ctx, "failed to show collections", mlog.Err(err))
		writeJSONError(w, fmt.Sprintf("failed to get collections: %s", err.Error()), http.StatusInternalServerError)
		return
	}
	// Sort collection IDs so the reported reasons are deterministic across polls (which collection's
	// reason sticks to a resource group must not depend on ShowLoadCollections iteration order).
	collectionIDs := showResp.GetCollectionIDs()
	sort.Slice(collectionIDs, func(i, j int) bool { return collectionIDs[i] < collectionIDs[j] })

	// Check each collection
	for _, collectionID := range collectionIDs {
		skipClusterLevelConfigChecks := !forceOverrideUserReplicaMode && s.queryCoordServer.IsCollectionUserSpecifiedReplicaMode(ctx, collectionID)

		// Get internal replicas from QueryCoord meta which contains StreamingResourceGroup field
		internalReplicas := s.queryCoordServer.GetInternalReplicasByCollection(ctx, collectionID)

		actualRGs := make([]string, 0, len(internalReplicas))
		rgByReplica := make(map[int64]string, len(internalReplicas))
		for _, replica := range internalReplicas {
			actualRGs = append(actualRGs, replica.GetResourceGroup())
			rgByReplica[replica.GetID()] = replica.GetResourceGroup()
			if perResourceGroup {
				resourceGroups[replica.GetResourceGroup()] = struct{}{}
			}
		}
		// Resource groups this collection's violations are attributed to: the groups actually hosting its
		// replicas, falling back to the cluster-level expected groups when the collection has no replicas yet.
		collectionRGs := actualRGs
		if len(collectionRGs) == 0 {
			collectionRGs = clusterResourceGroups
		}

		// Check replica count matches exactly — the replica meta must already reflect
		// the configured count before we inspect serviceability/leaks. Downstream serviceability
		// and leak checks are only meaningful once the replica meta reflects the configured count,
		// so a count mismatch skips the rest of this collection's checks.
		if !skipClusterLevelConfigChecks && clusterReplicaNum > 0 && len(internalReplicas) != clusterReplicaNum {
			reason := fmt.Sprintf("collection %d: replica count mismatch (expected %d, actual %d)",
				collectionID, clusterReplicaNum, len(internalReplicas))
			logger.Info(ctx, "collection replica count does not match cluster requirement", mlog.String("reason", reason))
			record(collectionRGs, reason)
			continue
		}

		if !skipClusterLevelConfigChecks && len(clusterResourceGroups) > 0 {
			// Check resource groups - collect actual RGs from replicas
			if reason, offendingRGs := s.validateRGDistribution(actualRGs, clusterResourceGroups,
				"resource group", collectionID); reason != "" {
				logger.Info(ctx, "collection resource group distribution does not match cluster requirement", mlog.String("reason", reason))
				record(offendingRGs, reason)
			}
		}

		// Now that replica count and RG distribution match, verify every replica actually
		// has a serviceable shard leader for every channel. This live dist check avoids
		// the stale CollectionObserver-persisted LoadPercentage that can falsely report
		// 100% during scale-up/scale-down transitions.
		if perResourceGroup {
			// Attribute each unserviceable replica to its own resource group instead of the whole
			// collection: a healthy RG must not be blocked by another RG's problem.
			if len(internalReplicas) == 0 {
				reason := fmt.Sprintf("collection %d: no replica found", collectionID)
				logger.Info(ctx, "collection has no replica", mlog.String("reason", reason))
				record(nil, reason)
			}
			for replicaID, replicaErr := range s.queryCoordServer.CheckReplicasServiceable(ctx, collectionID) {
				reason := fmt.Sprintf("collection %d: %s", collectionID, replicaErr.Error())
				logger.Info(ctx, "collection has unserviceable replica", mlog.String("reason", reason))
				record([]string{rgByReplica[replicaID]}, reason)
			}
		} else if err := s.queryCoordServer.CheckAllReplicasServiceable(ctx, collectionID); err != nil {
			reason := fmt.Sprintf("collection %d: %s", collectionID, err.Error())
			logger.Info(ctx, "collection not serviceable", mlog.String("reason", reason))
			record(collectionRGs, reason)
		}

		for _, replica := range internalReplicas {
			if !replica.IsQueryVisible() {
				reason := fmt.Sprintf("collection %d: replica %d (rg=%s) is not query visible",
					collectionID, replica.GetID(), replica.GetResourceGroup())
				logger.Info(ctx, "collection has query-invisible replica", mlog.String("reason", reason))
				record([]string{replica.GetResourceGroup()}, reason)
			}
		}

		// Check that physical resources have been released from querynodes no longer
		// part of any replica. During scale-down a decommissioned replica's querynode may
		// still hold segments/channels while release is in flight; compliance must wait for
		// that to finish before signaling Ready, otherwise callers may terminate nodes while
		// they are still serving or holding state.
		if perResourceGroup {
			// Attribute leaked resources to the resource group of the querynode holding them.
			for rg, leaked := range s.queryCoordServer.GetLeakedResourcesByCollectionPerRG(ctx, collectionID) {
				reason := fmt.Sprintf("collection %d: resources not fully released (leaked=%d)", collectionID, leaked)
				logger.Info(ctx, "collection has leaked resources on non-replica nodes", mlog.String("reason", reason))
				record([]string{rg}, reason)
			}
		} else {
			leakedSegments, leakedChannels := s.queryCoordServer.GetLeakedResourcesByCollection(ctx, collectionID)
			if leakedSegments > 0 || leakedChannels > 0 {
				reason := fmt.Sprintf("collection %d: resources not fully released (leaked segments=%d, channels=%d)",
					collectionID, leakedSegments, leakedChannels)
				logger.Info(ctx, "collection has leaked resources on non-replica nodes", mlog.String("reason", reason))
				record(collectionRGs, reason)
			}
		}
	}

	s.writeComplianceResult(ctx, logger, w, perResourceGroup, resourceGroups, violations, len(collectionIDs))
}

// writeComplianceResult writes the compliance response once all checks have run. In summary mode it
// preserves the fast-fail behavior: only the first violation is reported. In per-resource-group mode
// violations are aggregated per resource group. totalCollections is only used for the summary-mode
// Ready log.
func (s *mixCoordImpl) writeComplianceResult(ctx context.Context, logger *mlog.Logger, w http.ResponseWriter, perResourceGroup bool, resourceGroups map[string]struct{}, violations []complianceViolation, totalCollections int) {
	if perResourceGroup {
		resourceGroupErrors, rgs, globalReason := collectPerResourceGroupErrors(violations, resourceGroups)
		s.writePerResourceGroupComplianceResponse(w, rgs, resourceGroupErrors, globalReason)
		return
	}
	if len(violations) > 0 {
		s.writeComplianceResponse(w, LoadConfigComplianceStateNotReady, violations[0].reason)
		return
	}
	logger.Info(ctx, "all collections meet replica load config compliance requirements", mlog.Int("totalCollections", totalCollections))
	s.writeComplianceResponse(w, LoadConfigComplianceStateReady, "")
}

// writePerResourceGroupComplianceResponse writes the per-resource-group compliance response: one entry per
// involved resource group with its Ready/NotReady state and, when not ready, the first reason collected for
// it. The overall state is NotReady if any resource group is not ready or a global reason exists (a violation
// that cannot be attributed to any specific resource group, such as a collection with no replica at all).
// The top-level reason is the global one when present, otherwise the reason of the alphabetically first
// not-ready resource group, keeping the response compact and deterministic.
func (s *mixCoordImpl) writePerResourceGroupComplianceResponse(w http.ResponseWriter, resourceGroups map[string]struct{}, resourceGroupErrors map[string]string, globalReason string) {
	rgs := make([]string, 0, len(resourceGroups))
	for rg := range resourceGroups {
		rgs = append(rgs, rg)
	}
	sort.Strings(rgs)

	state := LoadConfigComplianceStateReady
	if globalReason != "" {
		state = LoadConfigComplianceStateNotReady
	}
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
		return fmt.Sprintf("collection %d: %s mismatch (delta: %s)", collectionID, rgType, strings.Join(diffs, ", ")), offendingRGs
	}
	return "", nil
}

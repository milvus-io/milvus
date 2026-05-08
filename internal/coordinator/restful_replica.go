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
	"strings"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/balance"
	"github.com/milvus-io/milvus/pkg/v3/log"
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

// LoadConfigComplianceResponse is the response structure for replica load config compliance check
type LoadConfigComplianceResponse struct {
	State  LoadConfigComplianceState `json:"state"`
	Reason string                    `json:"reason,omitempty"`
}

// HandleReplicaLoadConfigCompliance checks if all loaded collections meet the cluster-level replica configuration requirements
func (s *mixCoordImpl) HandleReplicaLoadConfigCompliance(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		writeJSONError(w, "Method not allowed, use GET", http.StatusMethodNotAllowed)
		return
	}

	ctx := req.Context()
	logger := log.Ctx(ctx).With(zap.String("handler", "ReplicaLoadConfigCompliance"))

	// Cluster-level check: WAL is fully migrated onto the configured primary resource group.
	// Short-circuit before reading config / loading collections — a WAL-layout issue affects
	// every collection and is independent of per-collection replica/RG config.
	if b, err := balance.GetWithContext(ctx); err != nil {
		writeJSONError(w, fmt.Sprintf("failed to get streaming balancer: %s", err.Error()), http.StatusInternalServerError)
		return
	} else if err := b.ConfirmPrimaryResourceGroupReady(ctx); err != nil {
		reason := fmt.Sprintf("WAL placement: %s", err.Error())
		logger.Info("WAL not fully placed on primary resource group", zap.String("reason", reason))
		s.writeComplianceResponse(w, LoadConfigComplianceStateNotReady, reason)
		return
	}

	// Get cluster-level configuration
	clusterReplicaNum := Params.QueryCoordCfg.ClusterLevelLoadReplicaNumber.GetAsInt()
	clusterResourceGroups := Params.QueryCoordCfg.ClusterLevelLoadResourceGroups.GetAsStrings()

	logger.Info("checking replica load config compliance",
		zap.Int("clusterReplicaNum", clusterReplicaNum),
		zap.Strings("clusterResourceGroups", clusterResourceGroups))

	// Use ShowLoadCollections to get all loaded collections
	showResp, err := s.ShowLoadCollections(ctx, &querypb.ShowCollectionsRequest{
		Base: commonpbutil.NewMsgBase(),
	})
	if err := merr.CheckRPCCall(showResp, err); err != nil {
		logger.Warn("failed to show collections", zap.Error(err))
		writeJSONError(w, fmt.Sprintf("failed to get collections: %s", err.Error()), http.StatusInternalServerError)
		return
	}

	// Check each collection
	for _, collectionID := range showResp.GetCollectionIDs() {
		// Get internal replicas from QueryCoord meta which contains StreamingResourceGroup field
		internalReplicas := s.queryCoordServer.GetInternalReplicasByCollection(ctx, collectionID)

		// Check replica count matches exactly — the replica meta must already reflect
		// the configured count before we inspect serviceability/leaks.
		if clusterReplicaNum > 0 && len(internalReplicas) != clusterReplicaNum {
			reason := fmt.Sprintf("collection %d: replica count mismatch (expected %d, actual %d)",
				collectionID, clusterReplicaNum, len(internalReplicas))
			logger.Info("collection replica count does not match cluster requirement", zap.String("reason", reason))
			s.writeComplianceResponse(w, LoadConfigComplianceStateNotReady, reason)
			return
		}

		if len(clusterResourceGroups) > 0 {
			// Check resource groups - collect actual RGs from replicas
			actualRGs := []string{}
			for _, replica := range internalReplicas {
				actualRGs = append(actualRGs, replica.GetResourceGroup())
			}
			// Validate resource groups
			if reason := s.validateRGDistribution(actualRGs, clusterResourceGroups,
				"resource group", collectionID); reason != "" {
				s.writeComplianceResponse(w, LoadConfigComplianceStateNotReady, reason)
				return
			}
		}

		// Now that replica count and RG distribution match, verify every replica actually
		// has a serviceable shard leader for every channel. This live dist check avoids
		// the stale CollectionObserver-persisted LoadPercentage that can falsely report
		// 100% during scale-up/scale-down transitions.
		if err := s.queryCoordServer.CheckAllReplicasServiceable(ctx, collectionID); err != nil {
			reason := fmt.Sprintf("collection %d: %s", collectionID, err.Error())
			logger.Info("collection not serviceable", zap.String("reason", reason))
			s.writeComplianceResponse(w, LoadConfigComplianceStateNotReady, reason)
			return
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
			logger.Info("collection has leaked resources on non-replica nodes", zap.String("reason", reason))
			s.writeComplianceResponse(w, LoadConfigComplianceStateNotReady, reason)
			return
		}
	}

	// All collections meet the requirements
	logger.Info("all collections meet replica load config compliance requirements", zap.Int("totalCollections", len(showResp.GetCollectionIDs())))
	s.writeComplianceResponse(w, LoadConfigComplianceStateReady, "")
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

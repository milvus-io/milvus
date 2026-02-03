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
	"encoding/json"
	"fmt"
	"net/http"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
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
	ctx := req.Context()
	logger := log.Ctx(ctx).With(zap.String("handler", "ReplicaLoadConfigCompliance"))

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
		http.Error(w, fmt.Sprintf("failed to get collections: %s", err.Error()), http.StatusInternalServerError)
		return
	}

	// Check each collection
	for idx, collectionID := range showResp.GetCollectionIDs() {
		// Check if collection is fully loaded (LoadPercentage == 100)
		loadPercentage := showResp.GetInMemoryPercentages()[idx]
		if loadPercentage < 100 {
			reason := fmt.Sprintf("collection %d: not fully loaded (%d%%)", collectionID, loadPercentage)
			logger.Info("collection not 100% loaded", zap.String("reason", reason))
			s.writeComplianceResponse(w, LoadConfigComplianceStateNotReady, reason)
			return
		}

		// Get internal replicas from QueryCoord meta which contains StreamingResourceGroup field
		internalReplicas := s.queryCoordServer.GetInternalReplicasByCollection(ctx, collectionID)

		// Check replica count matches exactly
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

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
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
	for _, cnt := range counts {
		if cnt != 0 {
			return fmt.Sprintf("collection %d: %s mismatch (expected [%v], actual [%v])", collectionID, rgType, expectedRGs, actualRGs)
		}
	}
	return ""
}

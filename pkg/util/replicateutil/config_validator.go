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

package replicateutil

import (
	"fmt"
	"net/url"
	"slices"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
)

// ReplicateConfigValidator validates ReplicateConfiguration according to business rules
type ReplicateConfigValidator struct {
	currentClusterID     string
	currentPChannels     []string
	clusterMap           map[string]*commonpb.MilvusCluster
	incomingConfig       *commonpb.ReplicateConfiguration
	currentConfig        *commonpb.ReplicateConfiguration
	isPChannelIncreasing bool // detected during validateConfigComparison
}

// NewReplicateConfigValidator creates a new validator instance with the given configuration
func NewReplicateConfigValidator(incomingConfig, currentConfig *commonpb.ReplicateConfiguration, currentClusterID string, currentPChannels []string) *ReplicateConfigValidator {
	validator := &ReplicateConfigValidator{
		currentClusterID: currentClusterID,
		currentPChannels: currentPChannels,
		clusterMap:       make(map[string]*commonpb.MilvusCluster),
		incomingConfig:   incomingConfig,
		currentConfig:    currentConfig,
	}
	return validator
}

// Validate performs all validation checks on the configuration
func (v *ReplicateConfigValidator) Validate() error {
	if v.incomingConfig == nil {
		return fmt.Errorf("config cannot be nil")
	}
	clusters := v.incomingConfig.GetClusters()
	if len(clusters) == 0 {
		return fmt.Errorf("clusters list cannot be empty")
	}
	// Perform all validation checks
	if err := v.validateClusterBasic(clusters); err != nil {
		return err
	}
	if err := v.validateRelevance(); err != nil {
		return err
	}
	topologies := v.incomingConfig.GetCrossClusterTopology()
	if err := v.validateTopologyEdgeUniqueness(topologies); err != nil {
		return err
	}
	if err := v.validateTopologyTypeConstraint(topologies); err != nil {
		return err
	}
	// If currentConfig is provided, perform comparison validation
	if v.currentConfig != nil {
		if err := v.validateConfigComparison(); err != nil {
			return err
		}
	}
	return nil
}

// validateClusterBasic validates basic format requirements for each MilvusCluster
func (v *ReplicateConfigValidator) validateClusterBasic(clusters []*commonpb.MilvusCluster) error {
	var expectedPchannelCount int
	var firstClusterID string
	uriSet := make(map[string]string)
	for i, cluster := range clusters {
		if cluster == nil {
			return fmt.Errorf("cluster at index %d is nil", i)
		}
		// clusterID validation: non-empty and no whitespace
		clusterID := cluster.GetClusterId()
		if clusterID == "" {
			return fmt.Errorf("cluster at index %d has empty clusterID", i)
		}
		if strings.ContainsAny(clusterID, " \t\n\r") {
			return fmt.Errorf("cluster at index %d has clusterID '%s' containing whitespace characters", i, clusterID)
		}
		// connection_param.uri validation: non-empty and basic URI format
		connParam := cluster.GetConnectionParam()
		if connParam == nil {
			return fmt.Errorf("cluster '%s' has nil connection_param", clusterID)
		}
		uri := connParam.GetUri()
		if uri == "" {
			return fmt.Errorf("cluster '%s' has empty URI", clusterID)
		}
		_, err := url.ParseRequestURI(uri)
		if err != nil {
			return fmt.Errorf("cluster '%s' has invalid URI format: '%s'", clusterID, uri)
		}
		// Check URI uniqueness
		if existingClusterID, exists := uriSet[uri]; exists {
			return fmt.Errorf("duplicate URI found: '%s' is used by both cluster '%s' and cluster '%s'", uri, existingClusterID, clusterID)
		}
		uriSet[uri] = clusterID
		// pchannels validation: non-empty
		pchannels := cluster.GetPchannels()
		if len(pchannels) == 0 {
			return fmt.Errorf("cluster '%s' has empty pchannels", clusterID)
		}
		// pchannels uniqueness within cluster
		pchannelSet := make(map[string]bool)
		for j, pchannel := range pchannels {
			if pchannel == "" {
				return fmt.Errorf("cluster '%s' has empty pchannel at index %d", clusterID, j)
			}
			if pchannelSet[pchannel] {
				return fmt.Errorf("cluster '%s' has duplicate pchannel: '%s'", clusterID, pchannel)
			}
			pchannelSet[pchannel] = true
		}
		// pchannels count consistency across all clusters
		if i == 0 {
			expectedPchannelCount = len(pchannels)
			firstClusterID = clusterID
		} else if len(pchannels) != expectedPchannelCount {
			return fmt.Errorf("cluster '%s' has %d pchannels, but expected %d (same as cluster '%s')",
				clusterID, len(pchannels), expectedPchannelCount, firstClusterID)
		}
		// Build cluster maps
		if _, exists := v.clusterMap[clusterID]; exists {
			return fmt.Errorf("duplicate clusterID found: '%s'", clusterID)
		}
		v.clusterMap[clusterID] = cluster
	}
	return nil
}

// validateRelevance validates that clusters must contain current Milvus cluster
func (v *ReplicateConfigValidator) validateRelevance() error {
	currentCluster, exists := v.clusterMap[v.currentClusterID]
	if !exists {
		return fmt.Errorf("current Milvus cluster '%s' must be included in the clusters list", v.currentClusterID)
	}
	if !equalIgnoreOrder(v.currentPChannels, currentCluster.GetPchannels()) {
		return fmt.Errorf("current pchannels do not match the pchannels in the config, current pchannels: %v, config pchannels: %v", v.currentPChannels, currentCluster.GetPchannels())
	}
	return nil
}

// validateTopologyEdgeUniqueness validates that a given source_clusterID -> target_clusterID pair appears only once
func (v *ReplicateConfigValidator) validateTopologyEdgeUniqueness(topologies []*commonpb.CrossClusterTopology) error {
	if len(topologies) == 0 {
		return nil
	}
	edgeSet := make(map[string]struct{})
	for i, topology := range topologies {
		if topology == nil {
			return fmt.Errorf("topology at index %d is nil", i)
		}
		sourceClusterID := topology.GetSourceClusterId()
		targetClusterID := topology.GetTargetClusterId()
		// Validate edge endpoints exist
		if _, exists := v.clusterMap[sourceClusterID]; !exists {
			return fmt.Errorf("topology at index %d references non-existent source cluster: '%s'", i, sourceClusterID)
		}
		if _, exists := v.clusterMap[targetClusterID]; !exists {
			return fmt.Errorf("topology at index %d references non-existent target cluster: '%s'", i, targetClusterID)
		}
		// Edge uniqueness
		edgeKey := fmt.Sprintf("%s->%s", sourceClusterID, targetClusterID)
		if _, exists := edgeSet[edgeKey]; exists {
			return fmt.Errorf("duplicate topology relationship found: '%s'", edgeKey)
		}
		edgeSet[edgeKey] = struct{}{}
	}
	return nil
}

// validateTopologyTypeConstraint validates that currently only STAR topology is supported
func (v *ReplicateConfigValidator) validateTopologyTypeConstraint(topologies []*commonpb.CrossClusterTopology) error {
	if len(topologies) == 0 {
		return nil
	}
	// Build in-degree and out-degree maps
	inDegree := make(map[string]int)
	outDegree := make(map[string]int)
	// Initialize all clusters with 0 degrees
	for clusterID := range v.clusterMap {
		inDegree[clusterID] = 0
		outDegree[clusterID] = 0
	}
	// Calculate degrees
	for _, topology := range topologies {
		source := topology.GetSourceClusterId()
		target := topology.GetTargetClusterId()
		outDegree[source]++
		inDegree[target]++
	}
	// Find center node (out-degree = clusters-1, in-degree = 0)
	var centerNode string
	clusterCount := len(v.clusterMap)
	for clusterID := range v.clusterMap {
		if outDegree[clusterID] == clusterCount-1 && inDegree[clusterID] == 0 {
			if centerNode != "" {
				// Multiple center nodes found
				return fmt.Errorf("multiple center nodes found, only one center node is allowed in star topology")
			}
			centerNode = clusterID
		}
	}
	if centerNode == "" {
		// No center node found
		return fmt.Errorf("no center node found, star topology must have exactly one center node")
	}
	// Validate other nodes (in-degree = 1, out-degree = 0)
	for clusterID := range v.clusterMap {
		if clusterID == centerNode {
			continue
		}
		if inDegree[clusterID] != 1 || outDegree[clusterID] != 0 {
			return fmt.Errorf("cluster '%s' does not follow star topology pattern (in-degree=%d, out-degree=%d)",
				clusterID, inDegree[clusterID], outDegree[clusterID])
		}
	}
	return nil
}

// validateConfigComparison validates that for clusters with the same ClusterID,
// no cluster attributes can be changed
func (v *ReplicateConfigValidator) validateConfigComparison() error {
	currentClusters := v.currentConfig.GetClusters()
	currentClusterMap := make(map[string]*commonpb.MilvusCluster)

	// Build current cluster map
	for _, cluster := range currentClusters {
		if cluster != nil {
			currentClusterMap[cluster.GetClusterId()] = cluster
		}
	}

	// Compare each incoming cluster with current cluster
	for _, incomingCluster := range v.incomingConfig.GetClusters() {
		clusterID := incomingCluster.GetClusterId()
		currentCluster, exists := currentClusterMap[clusterID]
		if exists {
			// Cluster exists in current config, validate that only ConnectionParam can change
			if err := v.validateClusterConsistency(currentCluster, incomingCluster); err != nil {
				return err
			}
		}
		// If cluster doesn't exist in current config, it's a new cluster, which is allowed
	}

	// When pchannels are increasing, enforce stricter rules
	if v.isPChannelIncreasing {
		if err := v.validatePChannelIncreasingConstraints(currentClusterMap); err != nil {
			return err
		}
	}
	return nil
}

// validatePChannelIncreasingConstraints enforces that when pchannels grow,
// only pchannel lists can change — cluster set and topology must remain identical.
func (v *ReplicateConfigValidator) validatePChannelIncreasingConstraints(currentClusterMap map[string]*commonpb.MilvusCluster) error {
	// Cluster set must be identical (no new or removed clusters)
	if len(currentClusterMap) != len(v.clusterMap) {
		return fmt.Errorf("when pchannels are increasing, cluster set must remain identical: current has %d clusters, incoming has %d",
			len(currentClusterMap), len(v.clusterMap))
	}
	for clusterID := range currentClusterMap {
		if _, ok := v.clusterMap[clusterID]; !ok {
			return fmt.Errorf("when pchannels are increasing, cluster set must remain identical: cluster '%s' missing from incoming config", clusterID)
		}
	}

	// Topology must be identical
	currentTopos := v.currentConfig.GetCrossClusterTopology()
	incomingTopos := v.incomingConfig.GetCrossClusterTopology()
	if len(currentTopos) != len(incomingTopos) {
		return fmt.Errorf("when pchannels are increasing, topology must remain identical: current has %d edges, incoming has %d",
			len(currentTopos), len(incomingTopos))
	}
	currentEdges := make(map[string]struct{})
	for _, topo := range currentTopos {
		currentEdges[topo.GetSourceClusterId()+"->"+topo.GetTargetClusterId()] = struct{}{}
	}
	for _, topo := range incomingTopos {
		edge := topo.GetSourceClusterId() + "->" + topo.GetTargetClusterId()
		if _, ok := currentEdges[edge]; !ok {
			return fmt.Errorf("when pchannels are increasing, topology must remain identical: edge '%s' not in current config", edge)
		}
	}
	return nil
}

// validateClusterConsistency validates that no cluster attributes can be changed between current and incoming cluster
func (v *ReplicateConfigValidator) validateClusterConsistency(current, incoming *commonpb.MilvusCluster) error {
	// Check Pchannels consistency: existing pchannels must be preserved (append-only growth allowed)
	currentPchannels := current.GetPchannels()
	incomingPchannels := incoming.GetPchannels()
	if len(incomingPchannels) < len(currentPchannels) {
		return fmt.Errorf("cluster '%s' pchannels cannot decrease: current=%d, incoming=%d",
			current.GetClusterId(), len(currentPchannels), len(incomingPchannels))
	}
	if !slices.Equal(currentPchannels, incomingPchannels[:len(currentPchannels)]) {
		return fmt.Errorf("cluster '%s' existing pchannels must be preserved at the same positions: current=%v, incoming=%v",
			current.GetClusterId(), currentPchannels, incomingPchannels)
	}
	if len(incomingPchannels) > len(currentPchannels) {
		v.isPChannelIncreasing = true
	}

	// Check ConnectionParam consistency
	currentConn := current.GetConnectionParam()
	incomingConn := incoming.GetConnectionParam()

	if currentConn.GetUri() != incomingConn.GetUri() {
		return fmt.Errorf("cluster '%s' connection_param.uri cannot be changed: current=%s, incoming=%s",
			current.GetClusterId(), currentConn.GetUri(), incomingConn.GetUri())
	}
	if currentConn.GetToken() != incomingConn.GetToken() {
		return fmt.Errorf("cluster '%s' connection_param.token cannot be changed",
			current.GetClusterId())
	}

	return nil
}

// IsPChannelIncreasing returns true if any cluster's pchannel list is growing.
// Must be called after Validate().
func (v *ReplicateConfigValidator) IsPChannelIncreasing() bool {
	return v.isPChannelIncreasing
}

func equalIgnoreOrder(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	counts := make(map[string]int)
	for _, v := range a {
		counts[v]++
	}
	for _, v := range b {
		if counts[v] == 0 {
			return false
		}
		counts[v]--
	}
	return true
}

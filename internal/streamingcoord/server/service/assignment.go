package service

import (
	"context"
	"fmt"
	"strings"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/service/discover"

	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

var _ streamingpb.StreamingCoordAssignmentServiceServer = (*assignmentServiceImpl)(nil)

// NewAssignmentService returns a new assignment service.
func NewAssignmentService(
	balancer *syncutil.Future[balancer.Balancer],
) streamingpb.StreamingCoordAssignmentServiceServer {
	return &assignmentServiceImpl{
		balancer:      balancer,
		listenerTotal: metrics.StreamingCoordAssignmentListenerTotal.WithLabelValues(paramtable.GetStringNodeID()),
	}
}

type AssignmentService interface {
	streamingpb.StreamingCoordAssignmentServiceServer
}

// assignmentServiceImpl is the implementation of the assignment service.
type assignmentServiceImpl struct {
	balancer      *syncutil.Future[balancer.Balancer]
	listenerTotal prometheus.Gauge
}

// AssignmentDiscover watches the state of all log nodes.
func (s *assignmentServiceImpl) AssignmentDiscover(server streamingpb.StreamingCoordAssignmentService_AssignmentDiscoverServer) error {
	s.listenerTotal.Inc()
	defer s.listenerTotal.Dec()

	balancer, err := s.balancer.GetWithContext(server.Context())
	if err != nil {
		return err
	}
	return discover.NewAssignmentDiscoverServer(balancer, server).Execute()
}

// UpdateReplicateConfiguration updates the replicate configuration to the milvus cluster.
func (s *assignmentServiceImpl) UpdateReplicateConfiguration(ctx context.Context, req *milvuspb.UpdateReplicateConfigurationRequest) (*streamingpb.UpdateReplicateConfigurationResponse, error) {
	config := req.GetReplicateConfiguration()
	if err := validateReplicateConfiguration(config); err != nil {
		return nil, err
	}
	err := resource.Resource().StreamingCatalog().SaveReplicateConfiguration(ctx, config)
	if err != nil {
		return nil, err
	}
	return &streamingpb.UpdateReplicateConfigurationResponse{}, nil
}

func validateReplicateConfiguration(config *milvuspb.ReplicateConfiguration) error {
	clusters := config.GetClusters()
	if len(clusters) == 0 {
		return fmt.Errorf("clusters list cannot be empty")
	}
	if err := validateClusters(clusters); err != nil {
		return err
	}
	if err := validateRelevance(clusters); err != nil {
		return err
	}
	if err := validateClusterUniqueness(clusters); err != nil {
		return err
	}
	topologies := config.GetCrossClusterTopology()
	if err := validateTopologyEdgeUniqueness(topologies, clusters); err != nil {
		return err
	}
	if err := validateTopologyTypeConstraint(topologies, clusters); err != nil {
		return err
	}
	return nil
}

// validateClusters validates basic format requirements for each MilvusCluster
func validateClusters(clusters []*milvuspb.MilvusCluster) error {
	var expectedPchannelCount int
	var firstClusterID string
	for i, cluster := range clusters {
		if cluster == nil {
			return fmt.Errorf("cluster at index %d is nil", i)
		}
		// clusterID validation: non-empty and no whitespace
		clusterID := cluster.GetClusterID()
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
		if !isValidURI(uri) {
			return fmt.Errorf("cluster '%s' has invalid URI format: '%s'", clusterID, uri)
		}

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
	}
	return nil
}

// validateRelevance validates that clusters must contain current Milvus cluster
func validateRelevance(clusters []*milvuspb.MilvusCluster) error {
	currentClusterID := paramtable.Get().CommonCfg.ClusterPrefix.GetValue()
	for _, cluster := range clusters {
		if cluster != nil && cluster.GetClusterID() == currentClusterID {
			return nil
		}
	}
	return fmt.Errorf("current Milvus cluster '%s' must be included in the clusters list", currentClusterID)
}

// validateClusterUniqueness validates that clusterID values across all clusters must be globally unique
func validateClusterUniqueness(clusters []*milvuspb.MilvusCluster) error {
	clusterIDs := make(map[string]bool)
	for _, cluster := range clusters {
		clusterID := cluster.GetClusterID()
		if clusterIDs[clusterID] {
			return fmt.Errorf("duplicate clusterID found: '%s'", clusterID)
		}
		clusterIDs[clusterID] = true
	}
	return nil
}

// validateTopologyEdgeUniqueness validates that a given source_clusterID -> target_clusterID pair appears only once
func validateTopologyEdgeUniqueness(topologies []*milvuspb.CrossClusterTopology, clusters []*milvuspb.MilvusCluster) error {
	if len(topologies) == 0 {
		return nil
	}
	// Build cluster ID set for validation
	clusterIDs := make(map[string]bool)
	for _, cluster := range clusters {
		if cluster != nil {
			clusterIDs[cluster.GetClusterID()] = true
		}
	}
	// Validate edge uniqueness and endpoints
	edgeSet := make(map[string]bool)
	for i, topology := range topologies {
		if topology == nil {
			return fmt.Errorf("topology at index %d is nil", i)
		}
		sourceClusterID := topology.GetSourceClusterID()
		targetClusterID := topology.GetTargetClusterID()
		// Validate edge endpoints exist
		if !clusterIDs[sourceClusterID] {
			return fmt.Errorf("topology at index %d references non-existent source cluster: '%s'", i, sourceClusterID)
		}
		if !clusterIDs[targetClusterID] {
			return fmt.Errorf("topology at index %d references non-existent target cluster: '%s'", i, targetClusterID)
		}
		// Edge uniqueness
		edgeKey := fmt.Sprintf("%s->%s", sourceClusterID, targetClusterID)
		if edgeSet[edgeKey] {
			return fmt.Errorf("duplicate topology relationship found: '%s'", edgeKey)
		}
		edgeSet[edgeKey] = true
	}
	return nil
}

// validateTopologyTypeConstraint validates that currently only STAR topology is supported
func validateTopologyTypeConstraint(topologies []*milvuspb.CrossClusterTopology, clusters []*milvuspb.MilvusCluster) error {
	if len(topologies) == 0 {
		return nil
	}
	// Build cluster ID set
	clusterIDs := make(map[string]bool)
	for _, cluster := range clusters {
		if cluster != nil {
			clusterIDs[cluster.GetClusterID()] = true
		}
	}
	// Build in-degree and out-degree maps
	inDegree := make(map[string]int)
	outDegree := make(map[string]int)

	// Initialize all clusters with 0 degrees
	for clusterID := range clusterIDs {
		inDegree[clusterID] = 0
		outDegree[clusterID] = 0
	}
	// Calculate degrees
	for _, topology := range topologies {
		source := topology.GetSourceClusterID()
		target := topology.GetTargetClusterID()
		outDegree[source]++
		inDegree[target]++
	}
	// Find center node (out-degree = clusters-1, in-degree = 0)
	var centerNode string
	clusterCount := len(clusterIDs)
	for clusterID := range clusterIDs {
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
	for clusterID := range clusterIDs {
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

// isValidURI checks if the given string is a valid URI format
func isValidURI(uri string) bool {
	if uri == "" {
		return false
	}
	parts := strings.Split(uri, "://")
	if len(parts) != 2 {
		return false
	}
	scheme := parts[0]
	rest := parts[1]
	if scheme == "" || strings.ContainsAny(scheme, " \t\n\r") {
		return false
	}
	if rest == "" {
		return false
	}
	return true
}

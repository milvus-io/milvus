package balancer

import balancerapi "github.com/milvus-io/milvus/internal/views/coord/balancer/api"

// Shared read types live in api so publishers need not depend on the policy.
type (
	NodeInfo           = balancerapi.NodeInfo
	SegmentDataView    = balancerapi.SegmentDataView
	ShardDataView      = balancerapi.ShardDataView
	PartitionDataView  = balancerapi.PartitionDataView
	CollectionDataView = balancerapi.CollectionDataView
)

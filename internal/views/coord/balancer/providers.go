package balancer

import (
	balancerapi "github.com/milvus-io/milvus/internal/views/coord/balancer/api"
	"github.com/milvus-io/milvus/internal/views/qviews"
)

// Shared read types live in api so publishers need not depend on the policy.
type (
	NodeInfo           = balancerapi.NodeInfo
	SegmentDataView    = balancerapi.SegmentDataView
	ShardDataView      = balancerapi.ShardDataView
	PartitionDataView  = balancerapi.PartitionDataView
	CollectionDataView = balancerapi.CollectionDataView
)

// DataViewProvider publishes current data and pins exact versions during view recovery.
type DataViewProvider interface {
	balancerapi.DataViewPublisher
	qviews.DataViewRefProvider
}

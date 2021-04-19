package queryservice

import (
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/querypb"
)

type queryNode struct {
	client         QueryNodeInterface
	insertChannels string
	nodeID         uint64
	segments       []UniqueID
}

func (qn *queryNode) GetComponentStates() (*internalpb2.ComponentStates, error) {
	return qn.client.GetComponentStates()
}

func (qn *queryNode) LoadSegments(in *querypb.LoadSegmentRequest) (*commonpb.Status, error) {
	return qn.client.LoadSegments(in)
}

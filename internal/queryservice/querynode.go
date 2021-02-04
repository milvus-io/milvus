package queryservice

import (
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/querypb"
)

type queryNodeInfo struct {
	client         QueryNodeInterface
	insertChannels string
	nodeID         uint64
	segments       []UniqueID
	dmChannelNames []string
}

func (qn *queryNodeInfo) GetComponentStates() (*internalpb2.ComponentStates, error) {
	return qn.client.GetComponentStates()
}

func (qn *queryNodeInfo) LoadSegments(in *querypb.LoadSegmentRequest) (*commonpb.Status, error) {
	return qn.client.LoadSegments(in)
}

func (qn *queryNodeInfo) GetSegmentInfo(in *querypb.SegmentInfoRequest) (*querypb.SegmentInfoResponse, error) {
	return qn.client.GetSegmentInfo(in)
}

package queryservice

import (
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/querypb"
)

type queryNodeInfo struct {
	client         QueryNodeInterface
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

func (qn *queryNodeInfo) WatchDmChannels(in *querypb.WatchDmChannelsRequest) (*commonpb.Status, error) {
	return qn.client.WatchDmChannels(in)
}

func (qn *queryNodeInfo) AddDmChannels(channels []string) {
	qn.dmChannelNames = append(qn.dmChannelNames, channels...)
}

func (qn *queryNodeInfo) AddQueryChannel(in *querypb.AddQueryChannelsRequest) (*commonpb.Status, error) {
	return qn.client.AddQueryChannel(in)
}

func (qn *queryNodeInfo) ReleaseCollection(in *querypb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	return qn.client.ReleaseCollection(in)
}

func (qn *queryNodeInfo) ReleasePartitions(in *querypb.ReleasePartitionRequest) (*commonpb.Status, error) {
	return qn.client.ReleasePartitions(in)
}

func newQueryNodeInfo(client QueryNodeInterface) *queryNodeInfo {
	segments := make([]UniqueID, 0)
	dmChannelNames := make([]string, 0)
	return &queryNodeInfo{
		client:         client,
		segments:       segments,
		dmChannelNames: dmChannelNames,
	}
}

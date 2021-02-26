package queryservice

import (
	"context"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/querypb"
)

type queryNodeInfo struct {
	client         QueryNodeInterface
	segments       []UniqueID
	dmChannelNames []string
}

func (qn *queryNodeInfo) GetComponentStates(ctx context.Context) (*internalpb2.ComponentStates, error) {
	return qn.client.GetComponentStates(ctx)
}

func (qn *queryNodeInfo) LoadSegments(ctx context.Context, in *querypb.LoadSegmentRequest) (*commonpb.Status, error) {
	return qn.client.LoadSegments(ctx, in)
}

func (qn *queryNodeInfo) GetSegmentInfo(ctx context.Context, in *querypb.SegmentInfoRequest) (*querypb.SegmentInfoResponse, error) {
	return qn.client.GetSegmentInfo(ctx, in)
}

func (qn *queryNodeInfo) WatchDmChannels(ctx context.Context, in *querypb.WatchDmChannelsRequest) (*commonpb.Status, error) {
	return qn.client.WatchDmChannels(ctx, in)
}

func (qn *queryNodeInfo) AddDmChannels(channels []string) {
	qn.dmChannelNames = append(qn.dmChannelNames, channels...)
}

func (qn *queryNodeInfo) AddQueryChannel(ctx context.Context, in *querypb.AddQueryChannelsRequest) (*commonpb.Status, error) {
	return qn.client.AddQueryChannel(ctx, in)
}

func (qn *queryNodeInfo) ReleaseCollection(ctx context.Context, in *querypb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	return qn.client.ReleaseCollection(ctx, in)
}

func (qn *queryNodeInfo) ReleasePartitions(ctx context.Context, in *querypb.ReleasePartitionRequest) (*commonpb.Status, error) {
	return qn.client.ReleasePartitions(ctx, in)
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

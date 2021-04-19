package queryservice

import (
	"context"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/querypb"
	"github.com/zilliztech/milvus-distributed/internal/types"
)

type queryNodeInfo struct {
	client         types.QueryNode
	segments       []UniqueID
	dmChannelNames []string
}

func (qn *queryNodeInfo) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
	return qn.client.GetComponentStates(ctx)
}

func (qn *queryNodeInfo) LoadSegments(ctx context.Context, in *querypb.LoadSegmentsRequest) (*commonpb.Status, error) {
	return qn.client.LoadSegments(ctx, in)
}

func (qn *queryNodeInfo) GetSegmentInfo(ctx context.Context, in *querypb.GetSegmentInfoRequest) (*querypb.GetSegmentInfoResponse, error) {
	return qn.client.GetSegmentInfo(ctx, in)
}

func (qn *queryNodeInfo) WatchDmChannels(ctx context.Context, in *querypb.WatchDmChannelsRequest) (*commonpb.Status, error) {
	return qn.client.WatchDmChannels(ctx, in)
}

func (qn *queryNodeInfo) AddDmChannels(channels []string) {
	qn.dmChannelNames = append(qn.dmChannelNames, channels...)
}

func (qn *queryNodeInfo) AddQueryChannel(ctx context.Context, in *querypb.AddQueryChannelRequest) (*commonpb.Status, error) {
	return qn.client.AddQueryChannel(ctx, in)
}

func (qn *queryNodeInfo) ReleaseCollection(ctx context.Context, in *querypb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	return qn.client.ReleaseCollection(ctx, in)
}

func (qn *queryNodeInfo) ReleasePartitions(ctx context.Context, in *querypb.ReleasePartitionsRequest) (*commonpb.Status, error) {
	return qn.client.ReleasePartitions(ctx, in)
}

func newQueryNodeInfo(client types.QueryNode) *queryNodeInfo {
	segments := make([]UniqueID, 0)
	dmChannelNames := make([]string, 0)
	return &queryNodeInfo{
		client:         client,
		segments:       segments,
		dmChannelNames: dmChannelNames,
	}
}

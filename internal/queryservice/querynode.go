package queryservice

import (
	"context"
	"sync"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/querypb"
	"github.com/zilliztech/milvus-distributed/internal/types"
)

type queryNodeInfo struct {
	client types.QueryNode

	mu           sync.Mutex // guards segments and channels2Col
	segments     map[UniqueID][]UniqueID
	channels2Col map[UniqueID][]string
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

func (qn *queryNodeInfo) AddDmChannels(channels []string, collectionID UniqueID) {
	qn.mu.Lock()
	defer qn.mu.Unlock()
	if _, ok := qn.channels2Col[collectionID]; !ok {
		chs := make([]string, 0)
		qn.channels2Col[collectionID] = chs
	}
	qn.channels2Col[collectionID] = append(qn.channels2Col[collectionID], channels...)
}

func (qn *queryNodeInfo) getNumChannels() int {
	qn.mu.Lock()
	defer qn.mu.Unlock()
	numChannels := 0
	for _, chs := range qn.channels2Col {
		numChannels += len(chs)
	}
	return numChannels
}

func (qn *queryNodeInfo) AddSegments(segmentIDs []UniqueID, collectionID UniqueID) {
	qn.mu.Lock()
	defer qn.mu.Unlock()
	if _, ok := qn.segments[collectionID]; !ok {
		seg := make([]UniqueID, 0)
		qn.segments[collectionID] = seg
	}
	qn.segments[collectionID] = append(qn.segments[collectionID], segmentIDs...)
}

func (qn *queryNodeInfo) getSegmentsLength() int {
	qn.mu.Lock()
	defer qn.mu.Unlock()
	return len(qn.segments)
}

func (qn *queryNodeInfo) getNumSegments() int {
	qn.mu.Lock()
	defer qn.mu.Unlock()
	numSegments := 0
	for _, ids := range qn.segments {
		numSegments += len(ids)
	}
	return numSegments
}

func (qn *queryNodeInfo) AddQueryChannel(ctx context.Context, in *querypb.AddQueryChannelRequest) (*commonpb.Status, error) {
	return qn.client.AddQueryChannel(ctx, in)
}

func (qn *queryNodeInfo) ReleaseCollection(ctx context.Context, in *querypb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	status, err := qn.client.ReleaseCollection(ctx, in)
	qn.mu.Lock()
	defer qn.mu.Unlock()
	if err != nil {
		return status, err
	}
	delete(qn.segments, in.CollectionID)
	delete(qn.channels2Col, in.CollectionID)
	return status, nil
}

func (qn *queryNodeInfo) ReleasePartitions(ctx context.Context, in *querypb.ReleasePartitionsRequest) (*commonpb.Status, error) {
	return qn.client.ReleasePartitions(ctx, in)
}

func newQueryNodeInfo(client types.QueryNode) *queryNodeInfo {
	segments := make(map[UniqueID][]UniqueID)
	channels := make(map[UniqueID][]string)
	return &queryNodeInfo{
		client:       client,
		segments:     segments,
		channels2Col: channels,
	}
}

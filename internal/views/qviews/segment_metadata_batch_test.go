package qviews

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestSegmentLoadInfoResponseBatchesAreSelfContained(t *testing.T) {
	response := &querypb.WatchQueryViewSegmentLoadInfoResponse{Status: merr.Success(), CollectionIndexes: []*querypb.QueryViewCollectionIndexes{{CollectionID: 1}, {CollectionID: 2}}}
	for i := 0; i < 12; i++ {
		response.Snapshots = append(response.Snapshots, &querypb.QueryViewSegmentLoadInfoSnapshot{CollectionID: int64(i%2 + 1), SegmentID: int64(i + 1), LoadInfo: &querypb.SegmentLoadInfo{ManifestPath: string(make([]byte, 1<<20))}})
	}
	var received []*querypb.QueryViewSegmentLoadInfoSnapshot
	batches := 0
	require.NoError(t, SendSegmentLoadInfoResponses(response, func(batch *querypb.WatchQueryViewSegmentLoadInfoResponse) error {
		batches++
		require.LessOrEqual(t, proto.Size(batch), segmentMetadataBatchBytes)
		collections := make(map[int64]bool)
		for _, indexes := range batch.CollectionIndexes {
			require.False(t, collections[indexes.CollectionID])
			collections[indexes.CollectionID] = true
		}
		for _, snapshot := range batch.Snapshots {
			require.True(t, collections[snapshot.CollectionID])
		}
		received = append(received, batch.Snapshots...)
		return nil
	}))
	require.Greater(t, batches, 1)
	require.Equal(t, response.Snapshots, received)
	// Do not acknowledge or continue sending later revisions after a send failure.
	calls := 0
	err := SendSegmentLoadInfoResponses(response, func(*querypb.WatchQueryViewSegmentLoadInfoResponse) error {
		calls++
		return merr.WrapErrServiceUnavailable("stream closed")
	})
	require.Error(t, err)
	require.Equal(t, 1, calls)
}

func TestSegmentLoadInfoRequestsPreserveReplacementOrder(t *testing.T) {
	request := &querypb.WatchQueryViewSegmentLoadInfoRequest{}
	for i := 0; i < 250000; i++ {
		id := int64(1<<50 + i)
		request.Unsubscribe = append(request.Unsubscribe, &querypb.WatchQueryViewSegmentLoadInfoUnsubscription{CollectionID: id, SegmentID: id})
		request.Subscribe = append(request.Subscribe, &querypb.WatchQueryViewSegmentLoadInfoSubscription{CollectionID: id, SegmentID: id})
	}
	unsubscribed, subscribed, batches := 0, 0, 0
	require.NoError(t, SendSegmentLoadInfoRequests(request, func(batch *querypb.WatchQueryViewSegmentLoadInfoRequest) error {
		batches++
		require.LessOrEqual(t, proto.Size(batch), segmentMetadataBatchBytes)
		unsubscribed += len(batch.Unsubscribe)
		if len(batch.Subscribe) > 0 {
			require.Equal(t, len(request.Unsubscribe), unsubscribed)
		}
		subscribed += len(batch.Subscribe)
		return nil
	}))
	require.Greater(t, batches, 1)
	require.Equal(t, len(request.Subscribe), subscribed)
}

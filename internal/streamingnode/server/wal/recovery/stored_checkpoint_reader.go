package recovery

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// storedCheckpointReader reads back the channel checkpoint DataCoord holds
// for a vchannel, through GetChannelRecoveryInfo. Its seek position is the
// stored checkpoint when there is one; otherwise it falls back to a segment
// DML or collection start position, which lies before any checkpoint the
// vchannel reported.
//
// Once the vchannel's collection is gone, every fallback is gone with it and
// the call answers ErrChannelNotAvailable for good. That one answer is
// followed up with the question DataCoord's garbage collector asks before it
// drops a channel checkpoint -- does the collection still exist from the
// user's perspective (DescribeCollection by id) -- and a definite not-found is
// reported as vchannel.ErrCollectionGone, which the cleanup gate treats as
// nothing left to confirm. Any other answer, an RPC failure included, keeps
// the wait.
type storedCheckpointReader struct {
	coord types.MixCoordClient
}

func (r storedCheckpointReader) StoredChannelCheckpoint(ctx context.Context, channel string) (uint64, error) {
	resp, err := r.coord.GetChannelRecoveryInfo(ctx, &datapb.GetChannelRecoveryInfoRequest{Vchannel: channel})
	if err := merr.CheckRPCCall(resp, err); err != nil {
		if errors.Is(err, merr.ErrChannelNotAvailable) && r.collectionGone(ctx, channel) {
			return 0, errors.Wrapf(vchannel.ErrCollectionGone, "vchannel %s: %s", channel, err.Error())
		}
		return 0, err
	}
	return resp.GetInfo().GetSeekPosition().GetTimestamp(), nil
}

// collectionGone reports whether the vchannel's collection is definitely gone
// from this cluster: DescribeCollection by id answers collection-not-found,
// the same test DataCoord's garbage collector applies (broker.HasCollection)
// before it drops the channel checkpoint. A Dropping collection answers
// not-found too, as it does there; a collection in any live state, an
// unparsable vchannel name and a failed RPC all answer false.
func (r storedCheckpointReader) collectionGone(ctx context.Context, channel string) bool {
	collectionID := funcutil.GetCollectionIDFromVChannel(channel)
	if collectionID == -1 {
		return false
	}
	resp, err := r.coord.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{CollectionID: collectionID})
	if err != nil {
		return false
	}
	return errors.Is(merr.Error(resp.GetStatus()), merr.ErrCollectionNotFound)
}

package recovery

import (
	"context"

	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// storedCheckpointReader reads back the channel checkpoint DataCoord holds
// for a vchannel, through GetChannelRecoveryInfo. Its seek position is the
// stored checkpoint when there is one; otherwise it falls back to a segment
// DML or collection start position, which lies before any checkpoint the
// vchannel reported.
type storedCheckpointReader struct {
	coord types.MixCoordClient
}

func (r storedCheckpointReader) StoredChannelCheckpoint(ctx context.Context, vchannel string) (uint64, error) {
	resp, err := r.coord.GetChannelRecoveryInfo(ctx, &datapb.GetChannelRecoveryInfoRequest{Vchannel: vchannel})
	if err := merr.CheckRPCCall(resp, err); err != nil {
		return 0, err
	}
	return resp.GetInfo().GetSeekPosition().GetTimestamp(), nil
}

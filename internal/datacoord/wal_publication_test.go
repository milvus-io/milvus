package datacoord

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/coordinator/snmanager"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestSaveBinlogPathsDistinguishesWALOwnership(t *testing.T) {
	for _, tc := range []struct {
		name      string
		dropped   bool
		owner     int64
		lookupErr error
		want      error
	}{
		{name: "retired channel", dropped: true, lookupErr: context.DeadlineExceeded, want: merr.ErrChannelNotFound},
		{name: "owner changed", owner: 2, want: merr.ErrChannelMisrouted},
		{name: "assignment unavailable", lookupErr: merr.WrapErrChannelNotAvailable("v1"), want: merr.ErrChannelNotAvailable},
		{name: "lookup timeout", lookupErr: context.DeadlineExceeded, want: context.DeadlineExceeded},
		{name: "current owner reaches segment lookup", owner: 1, want: merr.ErrSegmentNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &Server{meta: &meta{}}
			s.stateCode.Store(commonpb.StateCode_Healthy)
			var checkpoint *msgpb.MsgPosition
			if tc.dropped {
				checkpoint = &msgpb.MsgPosition{Timestamp: funcutil.DroppedChannelCheckpointTimestamp}
			}
			patchCP := mockey.Mock((*meta).GetChannelCheckpoint).Return(checkpoint).Build()
			defer patchCP.UnPatch()
			lookups := 0
			patchOwner := mockey.Mock((*snmanager.StreamingNodeManager).GetLatestWALLocated).To(func(_ *snmanager.StreamingNodeManager, _ context.Context, _ string) (int64, error) {
				lookups++
				return tc.owner, tc.lookupErr
			}).Build()
			defer patchOwner.UnPatch()
			patchSegment := mockey.Mock((*meta).GetSegment).Return(nil).Build()
			defer patchSegment.UnPatch()
			status, err := s.SaveBinlogPaths(context.Background(), &datapb.SaveBinlogPathsRequest{
				Base: &commonpb.MsgBase{SourceID: 1}, Channel: "v1", SegmentID: 10, SegLevel: datapb.SegmentLevel_L1,
			})
			require.NoError(t, err)
			// Compare the encoded result as well: ownership survives the RPC wire.
			require.Equal(t, merr.Code(tc.want), status.GetCode())
			if tc.dropped {
				require.Zero(t, lookups)
			} else {
				require.Equal(t, 1, lookups)
			}
		})
	}
}

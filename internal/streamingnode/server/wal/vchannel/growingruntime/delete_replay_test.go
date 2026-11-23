package growingruntime

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walsummary"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
)

func TestDrainDeleteReplayReadFailure(t *testing.T) {
	patch := mockey.Mock((*walsummary.Manager).ReadTransform).Return(walsummary.TransformBatch{}, walsummary.ErrStoreCorrupted).Build()
	defer patch.UnPatch()
	stream := walsummary.NewStream(walsummary.NewManager(walsummary.ManagerConfig{}))
	defer stream.Close()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	_, err := drainDeleteReplay(ctx, walview.VChannelWALView{VChannel: "v", BaseTransformTimeTick: 10, TransformLogStream: stream})
	require.ErrorIs(t, err, walsummary.ErrStoreCorrupted)
	require.NoError(t, ctx.Err(), "read failure must wake the bootstrap caller without a data event")
}

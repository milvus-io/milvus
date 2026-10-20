package broadcaster

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestEmptyFastAckDoesNotWaitForCallback(t *testing.T) {
	registry.ResetRegistration()
	paramtable.Init()
	task := newBroadcastTaskFromProto(createTruncateBroadcastTask(108, []string{"v1"}), newBroadcasterMetrics(), newAckCallbackScheduler(mlog.With()))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, task.FastAck(ctx, nil))
}

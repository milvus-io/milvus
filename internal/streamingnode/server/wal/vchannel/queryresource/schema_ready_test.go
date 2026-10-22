package queryresource

import (
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/growingruntime"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

func TestReadyWaitsForWALSchemaApplication(t *testing.T) {
	applied := int32(1)
	patch := mockey.Mock((*growingruntime.Runtime).WriteSchemaVersion).To(func(_ *growingruntime.Runtime) int32 { return applied }).Build()
	defer patch.UnPatch()
	runtime := NewQueryRuntime(&growingruntime.Runtime{})
	prepare := mockey.Mock((*QueryRuntime).PrepareQueryView).Return(nil).Build()
	defer prepare.UnPatch()
	key := qviews.QueryViewKey{ShardID: qviews.ShardID{ReplicaID: 1, VChannel: "p_1v0"}}
	manager := &Manager{runtime: runtime, refs: map[qviews.QueryViewKey]queryViewRef{key: {meta: &viewpb.QueryViewMeta{SchemaVersion: 2}}}}
	ready := 0
	callback := func() { ready++ }
	err := manager.prepareReady(t.Context(), key, callback)
	require.True(t, errors.Is(err, nodescheduler.ErrDelay))
	require.Zero(t, ready)
	applied = 2
	require.NoError(t, manager.prepareReady(t.Context(), key, callback))
	require.Equal(t, 1, ready)
}

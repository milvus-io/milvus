package broadcast

import (
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_broadcaster"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestPendingSchemaTargetFacadeRecovery(t *testing.T) {
	mockey.PatchConvey("schema facade waits for recovery and forwards the target", t, func() {
		mockey.Mock((*mock_broadcaster.MockBroadcaster).Close).Return().Build()
		ResetBroadcaster()
		defer ResetBroadcaster()
		target, err := GetPendingSchemaTarget(1)
		require.Nil(t, target)
		require.ErrorIs(t, err, merr.ErrServiceNotReady)
		expected := &broadcaster.SchemaTarget{CollectionID: 1, BroadcastID: 2, Schema: &schemapb.CollectionSchema{Version: 3}, TimeTicks: map[string]uint64{"v1": 42}}
		mockey.Mock((*mock_broadcaster.MockBroadcaster).GetPendingSchemaTarget).To(func(_ *mock_broadcaster.MockBroadcaster, id int64) (*broadcaster.SchemaTarget, error) {
			require.EqualValues(t, 1, id)
			return expected, nil
		}).Build()
		Register(&mock_broadcaster.MockBroadcaster{})
		target, err = GetPendingSchemaTarget(1)
		require.NoError(t, err)
		require.Same(t, expected, target)
	})
}

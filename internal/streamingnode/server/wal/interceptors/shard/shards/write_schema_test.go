package shards

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
)

func TestRecoveredWriteSchemaSnapshot(t *testing.T) {
	manager := &shardManagerImpl{collections: newCollectionInfosFromWritePathRecovery(map[string]moduleapi.VChannelWritePathRecoveryState{
		"p_1v0": {
			CollectionID: 1, VChannel: "p_1v0", Schema: &schemapb.CollectionSchema{Version: 3, Name: "before"}, SchemaTimeTick: 42,
		},
	})}
	snapshot, err := manager.GetWriteSchema(1, "p_1v0")
	require.NoError(t, err)
	require.EqualValues(t, 3, snapshot.Schema.Version)
	require.EqualValues(t, 42, snapshot.CheckpointTimeTick)
	snapshot.Schema.Name = "after"
	again, err := manager.GetWriteSchema(1, "p_1v0")
	require.NoError(t, err)
	require.Equal(t, "before", again.Schema.Name)
	_, err = manager.GetWriteSchema(1, "p_1v1")
	require.True(t, status.AsStreamingError(err).IsUnrecoverable())
	_, err = manager.GetWriteSchema(2, "p_1v0")
	require.True(t, status.AsStreamingError(err).IsUnrecoverable())
}

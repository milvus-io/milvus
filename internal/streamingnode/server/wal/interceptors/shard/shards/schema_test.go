package shards

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

func TestWriteSchemaAdmissionAndRecovery(t *testing.T) {
	m := &shardManagerImpl{mu: sync.RWMutex{}, collections: newCollectionInfosFromWritePathRecovery(map[string]moduleapi.VChannelWritePathRecoveryState{
		"p_1v0": {CollectionID: 1, Schema: &schemapb.CollectionSchema{Version: 0, Name: "original"}, SchemaTimeTick: 10},
	})}
	for _, version := range []int32{0, 2} {
		if version != 0 {
			m.collections[1].setSchema(&streamingpb.CollectionSchemaOfVChannel{Schema: &schemapb.CollectionSchema{Version: version, Name: "original"}, CheckpointTimeTick: 20})
		}
		got, err := m.GetWriteSchema(1, "p_1v0")
		require.NoError(t, err)
		require.Equal(t, version, got.Schema.Version)
		require.Equal(t, uint64(10+version*5), got.CheckpointTimeTick)
		admitted, err := m.CheckIfCollectionSchemaVersionMatch(&message.InsertMessageHeader{CollectionId: 1, SchemaVersion: proto.Int32(version)})
		require.NoError(t, err)
		require.Equal(t, got.Schema.Version, admitted)
		got.Schema.Name = "caller mutation"
		got.CheckpointTimeTick = 99
		again, err := m.GetWriteSchema(1, "p_1v0")
		require.NoError(t, err)
		require.Equal(t, "original", again.Schema.Name)
		require.NotEqualValues(t, 99, again.CheckpointTimeTick)
	}
	for _, identity := range []struct {
		id      int64
		channel string
	}{{2, "p_1v0"}, {1, "p_2v0"}} {
		_, err := m.GetWriteSchema(identity.id, identity.channel)
		require.True(t, status.AsStreamingError(err).IsUnrecoverable())
	}
	m.collections[1].Schema = nil
	_, err := m.GetWriteSchema(1, "p_1v0")
	require.Error(t, err)
	require.False(t, status.AsStreamingError(err).IsUnrecoverable())
	delete(m.collections, 1)
	_, err = m.GetWriteSchema(1, "p_1v0")
	require.True(t, status.AsStreamingError(err).IsUnrecoverable())
}

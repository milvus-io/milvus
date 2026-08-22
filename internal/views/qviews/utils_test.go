package qviews

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestDataVersion(t *testing.T) {
	dvs := []DataVersion{
		FromProtoDataVersion(&viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 0}),
		FromProtoDataVersion(&viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 0, TransformVersion: 1}),
		FromProtoDataVersion(&viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 1}),
		FromProtoDataVersion(&viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 2}),
		FromProtoDataVersion(&viewpb.DataVersion{StreamingVersion: 2, CompactVersion: 0}),
		FromProtoDataVersion(&viewpb.DataVersion{StreamingVersion: 2, CompactVersion: 1}),
		FromProtoDataVersion(&viewpb.DataVersion{StreamingVersion: 3, CompactVersion: 0}),
	}

	for i := 0; i < len(dvs)-1; i++ {
		assert.True(t, dvs[i+1].GT(dvs[i]))
		assert.True(t, dvs[i+1].GTE(dvs[i]))
		assert.False(t, dvs[i+1].EQ(dvs[i]))
		assert.True(t, dvs[i].EQ(dvs[i]))
		assert.NotEmpty(t, dvs[i].String())
	}

	// Test IntoProto round-trip.
	dv := DataVersion{StreamingVersion: 2, CompactVersion: 3, TransformVersion: 4}
	assert.Equal(t, dv, FromProtoDataVersion(dv.IntoProto()))
}

func TestQueryViewVersion(t *testing.T) {
	vss := []QueryViewVersion{
		FromProtoQueryViewVersion(&viewpb.QueryViewVersion{
			DataVersion:  &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 0},
			QueryVersion: 1,
		}),
		FromProtoQueryViewVersion(&viewpb.QueryViewVersion{
			DataVersion:  &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 0},
			QueryVersion: 2,
		}),
		FromProtoQueryViewVersion(&viewpb.QueryViewVersion{
			DataVersion:  &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 0, TransformVersion: 1},
			QueryVersion: 1,
		}),
		FromProtoQueryViewVersion(&viewpb.QueryViewVersion{
			DataVersion:  &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 1},
			QueryVersion: 1,
		}),
		FromProtoQueryViewVersion(&viewpb.QueryViewVersion{
			DataVersion:  &viewpb.DataVersion{StreamingVersion: 2, CompactVersion: 0},
			QueryVersion: 1,
		}),
		FromProtoQueryViewVersion(&viewpb.QueryViewVersion{
			DataVersion:  &viewpb.DataVersion{StreamingVersion: 2, CompactVersion: 0},
			QueryVersion: 3,
		}),
		FromProtoQueryViewVersion(&viewpb.QueryViewVersion{
			DataVersion:  &viewpb.DataVersion{StreamingVersion: 3, CompactVersion: 0},
			QueryVersion: 1,
		}),
	}

	for i := 0; i < len(vss)-1; i++ {
		assert.True(t, vss[i+1].GT(vss[i]))
		assert.True(t, vss[i+1].GTE(vss[i]))
		assert.False(t, vss[i+1].EQ(vss[i]))
		assert.True(t, vss[i].EQ(vss[i]))
		assert.NotEmpty(t, vss[i].String())
	}

	// Test IntoProto round-trip.
	qv := QueryViewVersion{
		DataVersion:  DataVersion{StreamingVersion: 2, CompactVersion: 1, TransformVersion: 2},
		QueryVersion: 3,
	}
	assert.Equal(t, qv, FromProtoQueryViewVersion(qv.IntoProto()))
}

func TestShardID(t *testing.T) {
	sid := NewShardIDFromQVMeta(&viewpb.QueryViewMeta{
		ReplicaId: 1,
		Vchannel:  "v1",
	})
	assert.Equal(t, ShardID{ReplicaID: 1, VChannel: "v1"}, sid)
	assert.NotEmpty(t, sid.String())
	assert.Equal(t, sid, FromProtoShardID(sid.IntoProto()))
}

func TestStateTransition(t *testing.T) {
	st := NewStateTransition(QueryViewStatePreparing)
	st.Done(QueryViewStateReady)
	assert.True(t, st.IsStateTransition())

	st = NewStateTransition(QueryViewStatePreparing)
	st.Done(QueryViewStatePreparing)
	assert.False(t, st.IsStateTransition())

	require.Panics(t, func() {
		NewStateTransition(QueryViewStatePreparing).IsStateTransition()
	})
}

func TestQueryViewIdentifiersString(t *testing.T) {
	state := QueryViewStatePreparing
	assert.Equal(t, "Preparing", state.String())
	assert.Equal(t, "unknown", NodeType(0).String())

	dv := DataVersion{StreamingVersion: 2, CompactVersion: 3, TransformVersion: 4}
	assert.Equal(t, "2/3/4", dv.String())

	qv := QueryViewVersion{
		DataVersion:  dv,
		QueryVersion: 4,
	}
	assert.Equal(t, "2/3/4/4", qv.String())

	key := QueryViewKey{
		ShardID: ShardID{
			ReplicaID: 10,
			VChannel:  "by-dev-rootcoord-dml_0_1v0",
		},
		QueryViewVersion: qv,
	}
	assert.Equal(t, "10-by-dev-rootcoord-dml_0_1v0-2/3/4/4", key.String())
}

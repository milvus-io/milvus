package qviews

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/proto/viewpb"
)

func TestVersions(t *testing.T) {
	vss := []QueryViewVersion{
		FromProtoQueryViewVersion(&viewpb.QueryViewVersion{DataVersion: 1, QueryVersion: 1}),
		FromProtoQueryViewVersion(&viewpb.QueryViewVersion{DataVersion: 1, QueryVersion: 2}),
		FromProtoQueryViewVersion(&viewpb.QueryViewVersion{DataVersion: 2, QueryVersion: 3}),
		FromProtoQueryViewVersion(&viewpb.QueryViewVersion{DataVersion: 3, QueryVersion: 1}),
		FromProtoQueryViewVersion(&viewpb.QueryViewVersion{DataVersion: 5, QueryVersion: 2}),
		FromProtoQueryViewVersion(&viewpb.QueryViewVersion{DataVersion: 6, QueryVersion: 1}),
		FromProtoQueryViewVersion(&viewpb.QueryViewVersion{DataVersion: 7, QueryVersion: 2}),
	}

	for i := 0; i < len(vss)-1; i++ {
		assert.True(t, vss[i+1].GT(vss[i]))
		assert.True(t, vss[i+1].GTE(vss[i]))
		assert.False(t, vss[i+1].EQ(vss[i]))
		assert.True(t, vss[i].EQ(vss[i]))
		assert.NotEmpty(t, vss[i].String())
	}
}

func TestShardID(t *testing.T) {
	sid := NewShardIDFromQVMeta(&viewpb.QueryViewMeta{
		ReplicaId: 1,
		Vchannel:  "v1",
	})
	assert.Equal(t, ShardID{ReplicaID: 1, VChannel: "v1"}, sid)
	assert.NotEmpty(t, sid.String())
}

func TestStateTransition(t *testing.T) {
	st := NewStateTransition(QueryViewStatePreparing)
	st.Done(QueryViewStateReady)
	assert.True(t, st.IsStateTransition())

	st = NewStateTransition(QueryViewStatePreparing)
	st.Done(QueryViewStatePreparing)
	assert.False(t, st.IsStateTransition())
}

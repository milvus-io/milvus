package qviews

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestBuilderPreparationPriorityOwnership(t *testing.T) {
	data := &viewpb.DataViewOfCollection{CollectionId: 1, Shards: []*viewpb.DataViewOfShard{{Vchannel: "v"}}}
	b := NewQueryViewAtCoordBuilder(1, data, "v").SetPreparationPriority(0)
	require.Nil(t, b.Build().Meta.Control)
	for _, priority := range []int32{-20, 0, 20} {
		b.SetPreparationPriority(priority)
		view := b.Build()
		wire, err := proto.Marshal(view)
		require.NoError(t, err)
		restored := &viewpb.QueryViewOfShard{}
		require.NoError(t, proto.Unmarshal(wire, restored))
		require.Equal(t, priority, restored.Meta.GetControl().GetPreparationPriority())
		copyBuilder := NewQueryViewAtCoordBuilderFromView(restored)
		restored.Meta.Control.PreparationPriority = -15
		require.Equal(t, priority, copyBuilder.Build().Meta.Control.PreparationPriority)
		view.Meta.Control.PreparationPriority = 15
		require.Equal(t, priority, b.Build().Meta.Control.PreparationPriority)
	}
}

package streamingnode

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/kv/mocks"
	"github.com/milvus-io/milvus/pkg/streaming/proto/streamingpb"
)

func TestCatalog(t *testing.T) {
	kv := mocks.NewMetaKv(t)
	k := "p1"
	v := streamingpb.SegmentAssignmentMeta{}
	vs, err := proto.Marshal(&v)
	assert.NoError(t, err)

	kv.EXPECT().LoadWithPrefix(mock.Anything).Return([]string{k}, []string{string(vs)}, nil)
	catalog := NewCataLog(kv)
	ctx := context.Background()
	metas, err := catalog.ListSegmentAssignment(ctx, "p1")
	assert.Len(t, metas, 1)
	assert.NoError(t, err)

	kv.EXPECT().MultiRemove(mock.Anything).Return(nil)
	kv.EXPECT().MultiSave(mock.Anything).Return(nil)

	err = catalog.SaveSegmentAssignments(ctx, "p1", []*streamingpb.SegmentAssignmentMeta{
		{
			SegmentId: 1,
			State:     streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED,
		},
		{
			SegmentId: 2,
			State:     streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_PENDING,
		},
	})
	assert.NoError(t, err)
}

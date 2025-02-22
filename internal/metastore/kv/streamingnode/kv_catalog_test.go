package streamingnode

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/kv/mocks"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
)

func TestCatalog(t *testing.T) {
	kv := mocks.NewMetaKv(t)
	k := "p1"
	v := streamingpb.SegmentAssignmentMeta{}
	vs, err := proto.Marshal(&v)
	assert.NoError(t, err)

	kv.EXPECT().LoadWithPrefix(mock.Anything, mock.Anything).Return([]string{k}, []string{string(vs)}, nil)
	catalog := NewCataLog(kv)
	ctx := context.Background()
	metas, err := catalog.ListSegmentAssignment(ctx, "p1")
	assert.Len(t, metas, 1)
	assert.NoError(t, err)

	kv.EXPECT().MultiRemove(mock.Anything, mock.Anything).Return(nil)
	kv.EXPECT().MultiSave(mock.Anything, mock.Anything).Return(nil)

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

func TestBuildDirectory(t *testing.T) {
	assert.Equal(t, "streamingnode-meta/wal/p1/", buildWALDirectory("p1"))
	assert.Equal(t, "streamingnode-meta/wal/p2/", buildWALDirectory("p2"))

	assert.Equal(t, "streamingnode-meta/wal/p1/segment-assign/", buildSegmentAssignmentMetaPath("p1"))
	assert.Equal(t, "streamingnode-meta/wal/p2/segment-assign/", buildSegmentAssignmentMetaPath("p2"))

	assert.Equal(t, "streamingnode-meta/wal/p1/segment-assign/1", buildSegmentAssignmentMetaPathOfSegment("p1", 1))
	assert.Equal(t, "streamingnode-meta/wal/p2/segment-assign/2", buildSegmentAssignmentMetaPathOfSegment("p2", 2))
}

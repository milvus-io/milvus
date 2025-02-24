package streamingnode

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/kv/mocks"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

func TestCatalogConsumeCheckpoint(t *testing.T) {
	kv := mocks.NewMetaKv(t)
	v := streamingpb.WALCheckpoint{}
	vs, err := proto.Marshal(&v)
	assert.NoError(t, err)

	kv.EXPECT().Load(mock.Anything, mock.Anything).Return(string(vs), nil)
	catalog := NewCataLog(kv)
	ctx := context.Background()
	checkpoint, err := catalog.GetConsumeCheckpoint(ctx, "p1")
	assert.NotNil(t, checkpoint)
	assert.NoError(t, err)

	kv.EXPECT().Load(mock.Anything, mock.Anything).Unset()
	kv.EXPECT().Load(mock.Anything, mock.Anything).Return("", errors.New("err"))
	checkpoint, err = catalog.GetConsumeCheckpoint(ctx, "p1")
	assert.Nil(t, checkpoint)
	assert.Error(t, err)

	kv.EXPECT().Load(mock.Anything, mock.Anything).Unset()
	kv.EXPECT().Load(mock.Anything, mock.Anything).Return("", merr.ErrIoKeyNotFound)
	checkpoint, err = catalog.GetConsumeCheckpoint(ctx, "p1")
	assert.Nil(t, checkpoint)
	assert.Nil(t, err)

	kv.EXPECT().Save(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	err = catalog.SaveConsumeCheckpoint(ctx, "p1", &streamingpb.WALCheckpoint{})
	assert.NoError(t, err)

	kv.EXPECT().Save(mock.Anything, mock.Anything, mock.Anything).Unset()
	kv.EXPECT().Save(mock.Anything, mock.Anything, mock.Anything).Return(errors.New("err"))
	err = catalog.SaveConsumeCheckpoint(ctx, "p1", &streamingpb.WALCheckpoint{})
	assert.Error(t, err)
}

func TestCatalogSegmentAssignments(t *testing.T) {
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

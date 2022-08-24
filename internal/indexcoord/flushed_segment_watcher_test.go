package indexcoord

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus/internal/metastore/kv/indexcoord"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
)

func Test_flushSegmentWatcher(t *testing.T) {
	ctx := context.Background()
	segInfo := &datapb.SegmentInfo{
		ID:             1,
		CollectionID:   2,
		PartitionID:    3,
		InsertChannel:  "",
		NumOfRows:      1024,
		State:          commonpb.SegmentState_Flushed,
		MaxRowNum:      1024,
		LastExpireTime: 0,
		StartPosition: &internalpb.MsgPosition{
			Timestamp: createTs,
		},
		DmlPosition:         nil,
		Binlogs:             nil,
		Statslogs:           nil,
		Deltalogs:           nil,
		CreatedByCompaction: false,
		CompactionFrom:      nil,
		DroppedAt:           0,
	}
	value, err := proto.Marshal(segInfo)
	assert.Nil(t, err)

	watcher, err := newFlushSegmentWatcher(ctx, &mockETCDKV{
		loadWithRevision: func(key string) ([]string, []string, int64, error) {
			return []string{"seg1"}, []string{string(value)}, 1, nil
		},
	},
		&metaTable{
			catalog: &indexcoord.Catalog{
				Txn: NewMockEtcdKV(),
			},
		},
		&indexBuilder{}, &IndexCoord{})
	assert.NoError(t, err)
	assert.NotNil(t, watcher)
}

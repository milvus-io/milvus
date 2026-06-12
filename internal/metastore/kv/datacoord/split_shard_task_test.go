package datacoord

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/kv/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func newSplitShardTask() *datapb.SplitShardTask {
	return &datapb.SplitShardTask{
		TaskId:         100,
		CollectionId:   1,
		SourceVchannel: "by-dev-rootcoord-dml_0_1v0",
		Targets: []*datapb.SplitShardTaskTarget{
			{Vchannel: "by-dev-rootcoord-dml_1_1v1", RoutingKeyUpper: []byte{0x80}},
			{Vchannel: "by-dev-rootcoord-dml_2_1v2", RoutingKeyLower: []byte{0x80}},
		},
		State:          datapb.SplitShardTaskState_SplitShardTaskRedistributing,
		SwitchTimeTick: 2000,
	}
}

func TestCatalogSplitShardTask(t *testing.T) {
	paramtable.Init()
	task := newSplitShardTask()

	t.Run("save and key layout", func(t *testing.T) {
		metakv := mocks.NewMetaKv(t)
		var savedKvs map[string]string
		metakv.EXPECT().MultiSave(mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, kvs map[string]string) error {
				savedKvs = kvs
				return nil
			}).Once()

		catalog := NewCatalog(metakv, rootPath, "")
		assert.NoError(t, catalog.SaveSplitShardTask(context.TODO(), task))
		assert.Len(t, savedKvs, 1)
		value, ok := savedKvs[SplitShardTaskPrefix+"/1/100"]
		assert.True(t, ok)
		saved := &datapb.SplitShardTask{}
		assert.NoError(t, proto.Unmarshal([]byte(value), saved))
		assert.Equal(t, uint64(2000), saved.GetSwitchTimeTick())
		assert.Equal(t, datapb.SplitShardTaskState_SplitShardTaskRedistributing, saved.GetState())
		assert.Len(t, saved.GetTargets(), 2)

		// nil task is a no-op.
		assert.NoError(t, catalog.SaveSplitShardTask(context.TODO(), nil))
	})

	t.Run("save failure", func(t *testing.T) {
		metakv := mocks.NewMetaKv(t)
		metakv.EXPECT().MultiSave(mock.Anything, mock.Anything).Return(errors.New("mock save error")).Once()
		catalog := NewCatalog(metakv, rootPath, "")
		assert.Error(t, catalog.SaveSplitShardTask(context.TODO(), task))
	})

	t.Run("list", func(t *testing.T) {
		taskBytes, err := proto.Marshal(task)
		assert.NoError(t, err)
		metakv := mocks.NewMetaKv(t)
		metakv.EXPECT().WalkWithPrefix(mock.Anything, SplitShardTaskPrefix+"/", mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, prefix string, paginationSize int, fn func([]byte, []byte) error) error {
				return fn([]byte(SplitShardTaskPrefix+"/1/100"), taskBytes)
			}).Once()

		catalog := NewCatalog(metakv, rootPath, "")
		tasks, err := catalog.ListSplitShardTask(context.TODO())
		assert.NoError(t, err)
		assert.Len(t, tasks, 1)
		assert.Equal(t, int64(100), tasks[0].GetTaskId())
		assert.Equal(t, "by-dev-rootcoord-dml_0_1v0", tasks[0].GetSourceVchannel())
	})

	t.Run("list failures", func(t *testing.T) {
		// walk failure.
		metakv := mocks.NewMetaKv(t)
		metakv.EXPECT().WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("mock walk error")).Once()
		catalog := NewCatalog(metakv, rootPath, "")
		tasks, err := catalog.ListSplitShardTask(context.TODO())
		assert.Nil(t, tasks)
		assert.Error(t, err)

		// corrupted value.
		metakv = mocks.NewMetaKv(t)
		metakv.EXPECT().WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, prefix string, paginationSize int, fn func([]byte, []byte) error) error {
				return fn([]byte(SplitShardTaskPrefix+"/1/100"), []byte("not-a-proto"))
			}).Once()
		catalog = NewCatalog(metakv, rootPath, "")
		_, err = catalog.ListSplitShardTask(context.TODO())
		assert.Error(t, err)
	})

	t.Run("drop", func(t *testing.T) {
		metakv := mocks.NewMetaKv(t)
		metakv.EXPECT().Remove(mock.Anything, SplitShardTaskPrefix+"/1/100").Return(nil).Once()
		catalog := NewCatalog(metakv, rootPath, "")
		assert.NoError(t, catalog.DropSplitShardTask(context.TODO(), task))
	})
}

package rootcoord

import (
	"context"
	"errors"
	"testing"

	"github.com/milvus-io/milvus/internal/metastore/model"

	"github.com/milvus-io/milvus/internal/common"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
)

func Test_alterCollectionTask_Prepare(t *testing.T) {
	t.Run("invalid collectionID", func(t *testing.T) {
		task := &alterCollectionTask{Req: &milvuspb.AlterCollectionRequest{Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_AlterCollection}}}
		err := task.Prepare(context.Background())
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		task := &alterCollectionTask{
			Req: &milvuspb.AlterCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_AlterCollection},
				CollectionName: "cn",
			},
		}
		err := task.Prepare(context.Background())
		assert.NoError(t, err)
	})
}

func Test_alterCollectionTask_Execute(t *testing.T) {
	properties := []*commonpb.KeyValuePair{
		{
			Key:   common.CollectionTTLConfigKey,
			Value: "3600",
		},
	}

	t.Run("properties is empty", func(t *testing.T) {
		task := &alterCollectionTask{Req: &milvuspb.AlterCollectionRequest{Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_AlterCollection}}}
		err := task.Execute(context.Background())
		assert.Error(t, err)
	})

	t.Run("failed to create alias", func(t *testing.T) {
		core := newTestCore(withInvalidMeta())
		task := &alterCollectionTask{
			baseTask: baseTask{core: core},
			Req: &milvuspb.AlterCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_AlterCollection},
				CollectionName: "cn",
				Properties:     properties,
			},
		}
		err := task.Execute(context.Background())
		assert.Error(t, err)
	})

	t.Run("alter step failed", func(t *testing.T) {
		meta := newMockMetaTable()
		meta.GetCollectionByNameFunc = func(ctx context.Context, collectionName string, ts Timestamp) (*model.Collection, error) {
			return &model.Collection{CollectionID: int64(1)}, nil
		}
		meta.AlterCollectionFunc = func(ctx context.Context, oldColl *model.Collection, newColl *model.Collection, ts Timestamp) error {
			return errors.New("err")
		}

		core := newTestCore(withMeta(meta))
		task := &alterCollectionTask{
			baseTask: baseTask{core: core},
			Req: &milvuspb.AlterCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_AlterCollection},
				CollectionName: "cn",
				Properties:     properties,
			},
		}

		err := task.Execute(context.Background())
		assert.Error(t, err)
	})

	t.Run("broadcast step failed", func(t *testing.T) {
		meta := newMockMetaTable()
		meta.GetCollectionByNameFunc = func(ctx context.Context, collectionName string, ts Timestamp) (*model.Collection, error) {
			return &model.Collection{CollectionID: int64(1)}, nil
		}
		meta.AlterCollectionFunc = func(ctx context.Context, oldColl *model.Collection, newColl *model.Collection, ts Timestamp) error {
			return nil
		}

		broker := newMockBroker()
		broker.BroadcastAlteredCollectionFunc = func(ctx context.Context, req *milvuspb.AlterCollectionRequest) error {
			return errors.New("err")
		}

		core := newTestCore(withValidProxyManager(), withMeta(meta), withBroker(broker))
		task := &alterCollectionTask{
			baseTask: baseTask{core: core},
			Req: &milvuspb.AlterCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_AlterCollection},
				CollectionName: "cn",
				Properties:     properties,
			},
		}

		err := task.Execute(context.Background())
		assert.Error(t, err)
	})

	t.Run("alter successfully", func(t *testing.T) {
		meta := newMockMetaTable()
		meta.GetCollectionByNameFunc = func(ctx context.Context, collectionName string, ts Timestamp) (*model.Collection, error) {
			return &model.Collection{CollectionID: int64(1)}, nil
		}
		meta.AlterCollectionFunc = func(ctx context.Context, oldColl *model.Collection, newColl *model.Collection, ts Timestamp) error {
			return nil
		}

		broker := newMockBroker()
		broker.BroadcastAlteredCollectionFunc = func(ctx context.Context, req *milvuspb.AlterCollectionRequest) error {
			return nil
		}

		core := newTestCore(withValidProxyManager(), withMeta(meta), withBroker(broker))
		task := &alterCollectionTask{
			baseTask: baseTask{core: core},
			Req: &milvuspb.AlterCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_AlterCollection},
				CollectionName: "cn",
				Properties:     properties,
			},
		}

		err := task.Execute(context.Background())
		assert.NoError(t, err)
	})
}

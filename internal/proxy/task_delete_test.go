package proxy

import (
	"context"
	"fmt"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"

	"github.com/cockroachdb/errors"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
)

func Test_getPrimaryKeysFromExpr(t *testing.T) {
	t.Run("delete on non-pk field", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Name:        "test_delete",
			Description: "",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      common.StartOfUserFieldID,
					Name:         "pk",
					IsPrimaryKey: true,
					DataType:     schemapb.DataType_Int64,
				},
				{
					FieldID:      common.StartOfUserFieldID + 1,
					Name:         "non_pk",
					IsPrimaryKey: false,
					DataType:     schemapb.DataType_Int64,
				},
			},
		}

		expr := "non_pk in [1, 2, 3]"

		_, _, err := getPrimaryKeysFromExpr(schema, expr)
		assert.Error(t, err)
	})
}

func TestDeleteTask(t *testing.T) {
	t.Run("test getChannels", func(t *testing.T) {
		collectionID := UniqueID(0)
		collectionName := "col-0"
		channels := []pChan{"mock-chan-0", "mock-chan-1"}
		cache := NewMockCache(t)
		cache.On("GetCollectionID",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(collectionID, nil)
		globalMetaCache = cache
		chMgr := newMockChannelsMgr()
		chMgr.getChannelsFunc = func(collectionID UniqueID) ([]pChan, error) {
			return channels, nil
		}
		dt := deleteTask{
			ctx: context.Background(),
			deleteMsg: &msgstream.DeleteMsg{
				DeleteRequest: msgpb.DeleteRequest{
					CollectionName: collectionName,
				},
			},
			chMgr: chMgr,
		}
		err := dt.setChannels()
		assert.NoError(t, err)
		resChannels := dt.getChannels()
		assert.ElementsMatch(t, channels, resChannels)
		assert.ElementsMatch(t, channels, dt.pChannels)

		chMgr.getChannelsFunc = func(collectionID UniqueID) ([]pChan, error) {
			return nil, fmt.Errorf("mock err")
		}
		// get channels again, should return task's pChannels, so getChannelsFunc should not invoke again
		resChannels = dt.getChannels()
		assert.ElementsMatch(t, channels, resChannels)
	})

	t.Run("empty collection name", func(t *testing.T) {
		dt := deleteTask{
			deleteMsg: &BaseDeleteTask{
				DeleteRequest: msgpb.DeleteRequest{
					Base: &commonpb.MsgBase{},
				},
			},
		}
		assert.Error(t, dt.PreExecute(context.Background()))
	})

	t.Run("fail to get collection id", func(t *testing.T) {
		dt := deleteTask{deleteMsg: &BaseDeleteTask{
			DeleteRequest: msgpb.DeleteRequest{
				Base:           &commonpb.MsgBase{},
				CollectionName: "foo",
			},
		}}
		cache := NewMockCache(t)
		cache.On("GetCollectionID",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(int64(0), errors.New("mock GetCollectionID err"))
		globalMetaCache = cache
		assert.Error(t, dt.PreExecute(context.Background()))
	})

	t.Run("fail partition key mode", func(t *testing.T) {
		dt := deleteTask{deleteMsg: &BaseDeleteTask{
			DeleteRequest: msgpb.DeleteRequest{
				Base:           &commonpb.MsgBase{},
				CollectionName: "foo",
				DbName:         "db_1",
			},
		}}
		cache := NewMockCache(t)
		cache.On("GetCollectionID",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(int64(10000), nil)
		cache.On("GetCollectionSchema",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(nil, errors.New("mock GetCollectionSchema err"))

		globalMetaCache = cache
		assert.Error(t, dt.PreExecute(context.Background()))
	})

	t.Run("invalid partition name", func(t *testing.T) {
		dt := deleteTask{deleteMsg: &BaseDeleteTask{
			DeleteRequest: msgpb.DeleteRequest{
				Base:           &commonpb.MsgBase{},
				CollectionName: "foo",
				DbName:         "db_1",
				PartitionName:  "aaa",
			},
		}}
		cache := NewMockCache(t)
		cache.On("GetCollectionID",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(int64(10000), nil)
		cache.On("GetCollectionSchema",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(&schemapb.CollectionSchema{
			Name:        "test_delete",
			Description: "",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:        common.StartOfUserFieldID,
					Name:           "pk",
					IsPrimaryKey:   true,
					DataType:       schemapb.DataType_Int64,
					IsPartitionKey: true,
				},
			},
		}, nil)

		globalMetaCache = cache
		assert.Error(t, dt.PreExecute(context.Background()))
	})

	schema := &schemapb.CollectionSchema{
		Name:        "test_delete",
		Description: "",
		AutoID:      false,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      common.StartOfUserFieldID,
				Name:         "pk",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:      common.StartOfUserFieldID + 1,
				Name:         "non_pk",
				IsPrimaryKey: false,
				DataType:     schemapb.DataType_Int64,
			},
		},
	}

	t.Run("invalie partition", func(t *testing.T) {
		dt := deleteTask{
			deleteMsg: &BaseDeleteTask{
				DeleteRequest: msgpb.DeleteRequest{
					Base:           &commonpb.MsgBase{},
					CollectionName: "foo",
					DbName:         "db_1",
					PartitionName:  "_aaa",
				},
			},
			deleteExpr: "non_pk in [1, 2, 3]",
		}
		cache := NewMockCache(t)
		cache.On("GetCollectionID",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(int64(10000), nil)
		cache.On("GetCollectionSchema",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(schema, nil)
		cache.On("GetPartitionID",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(int64(0), errors.New("mock GetPartitionID err"))

		globalMetaCache = cache
		assert.Error(t, dt.PreExecute(context.Background()))

		dt.deleteMsg.PartitionName = "aaa"
		assert.Error(t, dt.PreExecute(context.Background()))

		cache.On("GetPartitionID",
			mock.Anything, // context.Context
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
		).Return(int64(100001), nil)
		assert.Error(t, dt.PreExecute(context.Background()))
	})
}

package proxy

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/mock"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
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
			BaseDeleteTask: msgstream.DeleteMsg{
				DeleteRequest: internalpb.DeleteRequest{
					CollectionName: collectionName,
				},
			},
			chMgr: chMgr,
		}
		resChannels, err := dt.getChannels()
		assert.NoError(t, err)
		assert.ElementsMatch(t, channels, resChannels)
		assert.ElementsMatch(t, channels, dt.pChannels)

		chMgr.getChannelsFunc = func(collectionID UniqueID) ([]pChan, error) {
			return nil, fmt.Errorf("mock err")
		}
		// get channels again, should return task's pChannels, so getChannelsFunc should not invoke again
		resChannels, err = dt.getChannels()
		assert.NoError(t, err)
		assert.ElementsMatch(t, channels, resChannels)
	})
}

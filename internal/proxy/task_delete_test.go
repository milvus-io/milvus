package proxy

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
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
		cache := newMockCache()
		cache.setGetIDFunc(func(ctx context.Context, collectionName string) (typeutil.UniqueID, error) {
			return collectionID, nil
		})
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
}

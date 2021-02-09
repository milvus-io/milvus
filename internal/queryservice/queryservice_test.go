package queryservice

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/milvus-distributed/internal/msgstream/pulsarms"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/querypb"
)

func TestQueryService_Init(t *testing.T) {
	msFactory := pulsarms.NewFactory()
	service, err := NewQueryService(context.Background(), msFactory)
	assert.Nil(t, err)
	service.Init()
	service.Start()

	t.Run("Test create channel", func(t *testing.T) {
		response, err := service.CreateQueryChannel()
		assert.Nil(t, err)
		assert.Equal(t, response.RequestChannel, "query-0")
		assert.Equal(t, response.ResultChannel, "queryResult-0")
	})

	t.Run("Test Get statistics channel", func(t *testing.T) {
		response, err := service.GetStatisticsChannel()
		assert.Nil(t, err)
		assert.Equal(t, response, "query-node-stats")
	})

	t.Run("Test Get timeTick channel", func(t *testing.T) {
		response, err := service.GetTimeTickChannel()
		assert.Nil(t, err)
		assert.Equal(t, response, "queryTimeTick")
	})

	service.Stop()
}

func TestQueryService_load(t *testing.T) {
	msFactory := pulsarms.NewFactory()
	service, err := NewQueryService(context.Background(), msFactory)
	assert.Nil(t, err)
	service.Init()
	service.Start()
	service.SetMasterService(NewMasterMock())
	service.SetDataService(NewDataMock())
	registerNodeRequest := &querypb.RegisterNodeRequest{
		Address: &commonpb.Address{},
	}
	service.RegisterNode(registerNodeRequest)

	t.Run("Test LoadSegment", func(t *testing.T) {
		loadCollectionRequest := &querypb.LoadCollectionRequest{
			CollectionID: 1,
		}
		response, err := service.LoadCollection(loadCollectionRequest)
		assert.Nil(t, err)
		assert.Equal(t, response.ErrorCode, commonpb.ErrorCode_SUCCESS)
	})

	t.Run("Test LoadPartition", func(t *testing.T) {
		loadPartitionRequest := &querypb.LoadPartitionRequest{
			CollectionID: 1,
			PartitionIDs: []UniqueID{1},
		}
		response, err := service.LoadPartitions(loadPartitionRequest)
		assert.Nil(t, err)
		assert.Equal(t, response.ErrorCode, commonpb.ErrorCode_SUCCESS)
	})
}

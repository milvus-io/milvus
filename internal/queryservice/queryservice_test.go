package queryservice

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQueryService_Init(t *testing.T) {
	service, err := NewQueryService(context.Background())
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

//func TestQueryService_Load(t *testing.T) {
//
//}

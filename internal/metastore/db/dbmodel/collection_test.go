package dbmodel

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/common"
)

var (
	ts = time.Now()
)

func TestUnmarshalCollectionModel(t *testing.T) {
	t.Run("Unmarshal start position fail", func(t *testing.T) {
		collection := &Collection{
			StartPosition: "{\"error json\":}",
		}

		ret, err := UnmarshalCollectionModel(collection)
		assert.Nil(t, ret)
		assert.Error(t, err)
	})

	t.Run("Unmarshal properties fail", func(t *testing.T) {
		collection := &Collection{
			Properties: "{\"error json\":}",
		}

		ret, err := UnmarshalCollectionModel(collection)
		assert.Nil(t, ret)
		assert.Error(t, err)
	})

	t.Run("Unmarshal collection successfully", func(t *testing.T) {
		collection := &Collection{
			TenantID:         "",
			CollectionID:     1,
			CollectionName:   "cn",
			Description:      "",
			AutoID:           false,
			ShardsNum:        common.DefaultShardsNum,
			StartPosition:    "",
			ConsistencyLevel: int32(commonpb.ConsistencyLevel_Eventually),
			Properties:       "",
			Ts:               1,
			IsDeleted:        false,
			CreatedAt:        ts,
			UpdatedAt:        ts,
		}

		ret, err := UnmarshalCollectionModel(collection)
		assert.NotNil(t, ret)
		assert.NoError(t, err)

		assert.Equal(t, "", ret.TenantID)
		assert.Equal(t, int64(1), ret.CollectionID)
		assert.Equal(t, "cn", ret.Name)
		assert.Equal(t, "", ret.Description)
		assert.Equal(t, false, ret.AutoID)
		assert.Equal(t, common.DefaultShardsNum, ret.ShardsNum)
		assert.Equal(t, 0, len(ret.StartPositions))
		assert.Equal(t, commonpb.ConsistencyLevel(3), ret.ConsistencyLevel)
		assert.Nil(t, ret.Properties)
		assert.Equal(t, uint64(1), ret.CreateTime)
	})
}

func TestUnmarshalAndMarshalProperties(t *testing.T) {
	t.Run("Unmarshal and Marshal empty", func(t *testing.T) {

		ret, err := UnmarshalProperties("")
		assert.Nil(t, ret)
		assert.NoError(t, err)

		ret2, err := MarshalProperties(nil)
		assert.Empty(t, ret2)
		assert.NoError(t, err)
	})

	t.Run("Unmarshal and Marshal fail", func(t *testing.T) {
		ret, err := UnmarshalProperties("{\"error json\":}")
		assert.Nil(t, ret)
		assert.Error(t, err)
	})

	t.Run("Unmarshal collection successfully", func(t *testing.T) {
		properties := []*commonpb.KeyValuePair{
			{
				Key:   common.CollectionTTLConfigKey,
				Value: "3600",
			},
		}
		propertiesStr, err := MarshalProperties(properties)
		assert.NotEmpty(t, propertiesStr)
		assert.NoError(t, err)

		ret2, err := UnmarshalProperties(propertiesStr)
		assert.NotNil(t, ret2)
		assert.NoError(t, err)
		assert.Equal(t, ret2, properties)
	})
}

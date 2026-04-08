package shallowcopy

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
)

func TestShallowCopySearchRequest(t *testing.T) {
	t.Run("nil input", func(t *testing.T) {
		assert.Nil(t, ShallowCopySearchRequest(nil, 1))
	})

	t.Run("copies all fields and sets targetID", func(t *testing.T) {
		src := &internalpb.SearchRequest{
			Base:                    &commonpb.MsgBase{TargetID: 100, SourceID: 200},
			ReqID:                   1,
			DbID:                    2,
			CollectionID:            3,
			PartitionIDs:            []int64{10, 20},
			Dsl:                     "dsl",
			PlaceholderGroup:        []byte("placeholder"),
			SerializedExprPlan:      []byte("plan"),
			OutputFieldsId:          []int64{1, 2},
			MvccTimestamp:           100,
			Nq:                      5,
			Topk:                    10,
			MetricType:              "L2",
			IgnoreGrowing:           true,
			Username:                "user",
			IsAdvanced:              true,
			Offset:                  5,
			GroupByFieldId:          7,
			GroupSize:               3,
			FieldId:                 8,
			IsTopkReduce:            true,
			IsRecallEvaluation:      true,
			IsIterator:              true,
			AnalyzerName:            "analyzer",
			CollectionTtlTimestamps: 999,
			EntityTtlPhysicalTime:   888,
		}

		dst := ShallowCopySearchRequest(src, 42)

		// Base is new with correct TargetID
		assert.Equal(t, int64(42), dst.Base.TargetID)
		// Original Base not modified
		assert.Equal(t, int64(100), src.Base.TargetID)

		// Scalar fields copied
		assert.Equal(t, src.ReqID, dst.ReqID)
		assert.Equal(t, src.CollectionID, dst.CollectionID)
		assert.Equal(t, src.Nq, dst.Nq)
		assert.Equal(t, src.Topk, dst.Topk)
		assert.Equal(t, src.IsIterator, dst.IsIterator)
		assert.Equal(t, src.AnalyzerName, dst.AnalyzerName)
		assert.Equal(t, src.EntityTtlPhysicalTime, dst.EntityTtlPhysicalTime)

		// Slices share underlying array (shallow copy)
		assert.Equal(t, src.PartitionIDs, dst.PartitionIDs)
		assert.Equal(t, src.PlaceholderGroup, dst.PlaceholderGroup)
		assert.Equal(t, src.SerializedExprPlan, dst.SerializedExprPlan)
	})
}

func TestShallowCopyRetrieveRequest(t *testing.T) {
	t.Run("nil input", func(t *testing.T) {
		assert.Nil(t, ShallowCopyRetrieveRequest(nil, 1))
	})

	t.Run("copies all fields and sets targetID", func(t *testing.T) {
		src := &internalpb.RetrieveRequest{
			Base:                    &commonpb.MsgBase{TargetID: 100},
			ReqID:                   1,
			CollectionID:            3,
			PartitionIDs:            []int64{10, 20},
			SerializedExprPlan:      []byte("plan"),
			OutputFieldsId:          []int64{1, 2},
			MvccTimestamp:           100,
			Limit:                   50,
			IgnoreGrowing:           true,
			IsCount:                 true,
			Username:                "user",
			IsIterator:              true,
			CollectionTtlTimestamps: 999,
			EntityTtlPhysicalTime:   888,
			QueryLabel:              "query",
		}

		dst := ShallowCopyRetrieveRequest(src, 42)

		assert.Equal(t, int64(42), dst.Base.TargetID)
		assert.Equal(t, int64(100), src.Base.TargetID)
		assert.Equal(t, src.ReqID, dst.ReqID)
		assert.Equal(t, src.CollectionID, dst.CollectionID)
		assert.Equal(t, src.Limit, dst.Limit)
		assert.Equal(t, src.IsIterator, dst.IsIterator)
		assert.Equal(t, src.EntityTtlPhysicalTime, dst.EntityTtlPhysicalTime)
		assert.Equal(t, src.QueryLabel, dst.QueryLabel)
		assert.Equal(t, src.PartitionIDs, dst.PartitionIDs)
	})
}

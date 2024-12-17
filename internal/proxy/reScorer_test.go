package proxy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/json"
)

func TestRescorer(t *testing.T) {
	t.Run("default scorer", func(t *testing.T) {
		rescorers, err := NewReScorers(context.TODO(), 2, nil)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(rescorers))
		assert.Equal(t, rrfRankType, rescorers[0].scorerType())
	})

	t.Run("rrf without param", func(t *testing.T) {
		params := make(map[string]float64)
		b, err := json.Marshal(params)
		assert.NoError(t, err)
		rankParams := []*commonpb.KeyValuePair{
			{Key: RankTypeKey, Value: "rrf"},
			{Key: RankParamsKey, Value: string(b)},
		}

		_, err = NewReScorers(context.TODO(), 2, rankParams)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "k not found in rank_params")
	})

	t.Run("rrf param out of range", func(t *testing.T) {
		params := make(map[string]float64)
		params[RRFParamsKey] = -1
		b, err := json.Marshal(params)
		assert.NoError(t, err)
		rankParams := []*commonpb.KeyValuePair{
			{Key: RankTypeKey, Value: "rrf"},
			{Key: RankParamsKey, Value: string(b)},
		}

		_, err = NewReScorers(context.TODO(), 2, rankParams)
		assert.Error(t, err)

		params[RRFParamsKey] = maxRRFParamsValue + 1
		b, err = json.Marshal(params)
		assert.NoError(t, err)
		rankParams = []*commonpb.KeyValuePair{
			{Key: RankTypeKey, Value: "rrf"},
			{Key: RankParamsKey, Value: string(b)},
		}

		_, err = NewReScorers(context.TODO(), 2, rankParams)
		assert.Error(t, err)
	})

	t.Run("rrf", func(t *testing.T) {
		params := make(map[string]float64)
		params[RRFParamsKey] = 61
		b, err := json.Marshal(params)
		assert.NoError(t, err)
		rankParams := []*commonpb.KeyValuePair{
			{Key: RankTypeKey, Value: "rrf"},
			{Key: RankParamsKey, Value: string(b)},
		}

		rescorers, err := NewReScorers(context.TODO(), 2, rankParams)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(rescorers))
		assert.Equal(t, rrfRankType, rescorers[0].scorerType())
		assert.Equal(t, float32(61), rescorers[0].(*rrfScorer).k)
	})

	t.Run("weights without param", func(t *testing.T) {
		params := make(map[string][]float64)
		b, err := json.Marshal(params)
		assert.NoError(t, err)
		rankParams := []*commonpb.KeyValuePair{
			{Key: RankTypeKey, Value: "weighted"},
			{Key: RankParamsKey, Value: string(b)},
		}

		_, err = NewReScorers(context.TODO(), 2, rankParams)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found in rank_params")
	})

	t.Run("weights out of range", func(t *testing.T) {
		weights := []float64{1.2, 2.3}
		params := make(map[string][]float64)
		params[WeightsParamsKey] = weights
		b, err := json.Marshal(params)
		assert.NoError(t, err)
		rankParams := []*commonpb.KeyValuePair{
			{Key: RankTypeKey, Value: "weighted"},
			{Key: RankParamsKey, Value: string(b)},
		}

		_, err = NewReScorers(context.TODO(), 2, rankParams)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "rank param weight should be in range [0, 1]")
	})

	t.Run("weights", func(t *testing.T) {
		weights := []float64{0.5, 0.2}
		params := make(map[string][]float64)
		params[WeightsParamsKey] = weights
		b, err := json.Marshal(params)
		assert.NoError(t, err)
		rankParams := []*commonpb.KeyValuePair{
			{Key: RankTypeKey, Value: "weighted"},
			{Key: RankParamsKey, Value: string(b)},
		}

		rescorers, err := NewReScorers(context.TODO(), 2, rankParams)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(rescorers))
		assert.Equal(t, weightedRankType, rescorers[0].scorerType())
		assert.Equal(t, float32(weights[0]), rescorers[0].(*weightedScorer).weight)
	})
}

package testcases

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/client/v3/column"
	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/index"
	client "github.com/milvus-io/milvus/client/v3/milvusclient"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

const (
	searchAggPKField       = "id"
	searchAggVectorField   = "vector"
	searchAggBrandField    = "brand"
	searchAggColorField    = "color"
	searchAggCategoryField = "category"
	searchAggPriceField    = "price"
	searchAggRatingField   = "rating"
	searchAggStockField    = "in_stock"
	searchAggDim           = 4
)

type searchAggregationFixture struct {
	collectionName string
	vectors        []entity.FloatVector
}

func prepareSearchAggregationFixture(t *testing.T) searchAggregationFixture {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	collName := common.GenRandomString("search_agg", 6)
	schema := entity.NewSchema().WithName(collName).
		WithField(entity.NewField().WithName(searchAggPKField).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName(searchAggVectorField).WithDataType(entity.FieldTypeFloatVector).WithDim(searchAggDim)).
		WithField(entity.NewField().WithName(searchAggBrandField).WithDataType(entity.FieldTypeVarChar).WithMaxLength(32)).
		WithField(entity.NewField().WithName(searchAggColorField).WithDataType(entity.FieldTypeVarChar).WithMaxLength(32)).
		WithField(entity.NewField().WithName(searchAggCategoryField).WithDataType(entity.FieldTypeVarChar).WithMaxLength(32)).
		WithField(entity.NewField().WithName(searchAggPriceField).WithDataType(entity.FieldTypeInt64)).
		WithField(entity.NewField().WithName(searchAggRatingField).WithDataType(entity.FieldTypeDouble)).
		WithField(entity.NewField().WithName(searchAggStockField).WithDataType(entity.FieldTypeBool))
	err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(collName, schema).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	t.Cleanup(func() {
		cleanupCtx := hp.CreateContext(t, time.Second*30)
		_ = mc.DropCollection(cleanupCtx, client.NewDropCollectionOption(collName))
		_ = mc.Close(cleanupCtx)
	})

	pks := make([]int64, 0, 12)
	vectors := make([][]float32, 0, 12)
	brands := make([]string, 0, 12)
	colors := make([]string, 0, 12)
	categories := make([]string, 0, 12)
	prices := make([]int64, 0, 12)
	ratings := make([]float64, 0, 12)
	stocks := make([]bool, 0, 12)

	brandValues := []string{"brand_a", "brand_b", "brand_c"}
	colorValues := []string{"red", "blue"}
	categoryValues := []string{"phone", "tablet"}
	for i := 0; i < 12; i++ {
		pks = append(pks, int64(i))
		vectors = append(vectors, []float32{float32(i), 0, 0, 0})
		brands = append(brands, brandValues[i%len(brandValues)])
		colors = append(colors, colorValues[(i/3)%len(colorValues)])
		categories = append(categories, categoryValues[(i/6)%len(categoryValues)])
		prices = append(prices, int64(100+i*10))
		ratings = append(ratings, 3.0+float64(i%5)*0.25)
		stocks = append(stocks, i%2 == 0)
	}

	_, err = mc.Insert(ctx, client.NewColumnBasedInsertOption(collName).WithColumns(
		column.NewColumnInt64(searchAggPKField, pks),
		column.NewColumnFloatVector(searchAggVectorField, searchAggDim, vectors),
		column.NewColumnVarChar(searchAggBrandField, brands),
		column.NewColumnVarChar(searchAggColorField, colors),
		column.NewColumnVarChar(searchAggCategoryField, categories),
		column.NewColumnInt64(searchAggPriceField, prices),
		column.NewColumnDouble(searchAggRatingField, ratings),
		column.NewColumnBool(searchAggStockField, stocks),
	))
	common.CheckErr(t, err, true)

	flushTask, err := mc.Flush(ctx, client.NewFlushOption(collName))
	common.CheckErr(t, err, true)
	common.CheckErr(t, flushTask.Await(ctx), true)

	indexTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(collName, searchAggVectorField, index.NewFlatIndex(entity.L2)))
	common.CheckErr(t, err, true)
	common.CheckErr(t, indexTask.Await(ctx), true)

	loadTask, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(collName))
	common.CheckErr(t, err, true)
	common.CheckErr(t, loadTask.Await(ctx), true)

	queryVectors := make([]entity.FloatVector, 0, len(vectors))
	for _, vector := range vectors {
		queryVectors = append(queryVectors, entity.FloatVector(vector))
	}
	return searchAggregationFixture{collectionName: collName, vectors: queryVectors}
}

func TestSearchAggregationSingleFieldTopHits(t *testing.T) {
	t.Parallel()

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	fixture := prepareSearchAggregationFixture(t)

	res, err := mc.Search(ctx, client.NewSearchOption(fixture.collectionName, 10, []entity.Vector{fixture.vectors[0], fixture.vectors[6]}).
		WithANNSField(searchAggVectorField).
		WithOutputFields(searchAggBrandField, searchAggPriceField).
		WithSearchAggregation(
			client.NewSearchAggregation([]string{searchAggBrandField}, 3).
				WithMetric("doc_count", "count", "*").
				WithMetric("total_price", "sum", searchAggPriceField).
				WithOrder("_key", "asc").
				WithTopHits(client.NewTopHits(2).WithSort("_score", "asc")),
		))
	common.CheckErr(t, err, true)
	require.Len(t, res, 2)

	for _, result := range res {
		require.Len(t, result.AggregationBuckets, 3)
		for _, bucket := range result.AggregationBuckets {
			brand := requireSearchAggregationKey[string](t, bucket, searchAggBrandField)
			require.EqualValues(t, bucket.Count, bucket.Metrics["doc_count"])
			require.Len(t, bucket.Hits, 2)
			requireSearchAggregationScoresAsc(t, bucket.Hits)

			var priceSum int64
			for _, hit := range bucket.Hits {
				require.Equal(t, brand, hit.Fields[searchAggBrandField])
				priceSum += hit.Fields[searchAggPriceField].(int64)
			}
			require.EqualValues(t, priceSum, bucket.Metrics["total_price"])
		}
	}
}

func TestSearchAggregationCompositeKeyMetricsOrder(t *testing.T) {
	t.Parallel()

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	fixture := prepareSearchAggregationFixture(t)

	res, err := mc.Search(ctx, client.NewSearchOption(fixture.collectionName, 10, []entity.Vector{fixture.vectors[0], fixture.vectors[6]}).
		WithANNSField(searchAggVectorField).
		WithFilter(fmt.Sprintf("%s == true", searchAggStockField)).
		WithOutputFields(searchAggBrandField, searchAggColorField, searchAggPriceField, searchAggStockField).
		WithSearchAggregation(
			client.NewSearchAggregation([]string{searchAggBrandField, searchAggColorField}, 3).
				WithMetric("avg_price", "avg", searchAggPriceField).
				WithMetric("doc_count", "count", "*").
				WithOrder("avg_price", "desc").
				WithTopHits(client.NewTopHits(2).WithSort(searchAggPriceField, "asc")),
		))
	common.CheckErr(t, err, true)
	require.Len(t, res, 2)

	for _, result := range res {
		require.Len(t, result.AggregationBuckets, 3)
		var lastAvg float64
		for i, bucket := range result.AggregationBuckets {
			avgPrice := bucket.Metrics["avg_price"].(float64)
			if i > 0 {
				require.LessOrEqual(t, avgPrice, lastAvg)
			}
			lastAvg = avgPrice

			brand := requireSearchAggregationKey[string](t, bucket, searchAggBrandField)
			color := requireSearchAggregationKey[string](t, bucket, searchAggColorField)
			require.EqualValues(t, bucket.Count, bucket.Metrics["doc_count"])
			require.NotEmpty(t, bucket.Hits)
			requireSearchAggregationHitPricesAsc(t, bucket.Hits)

			var priceSum int64
			for _, hit := range bucket.Hits {
				require.Equal(t, brand, hit.Fields[searchAggBrandField])
				require.Equal(t, color, hit.Fields[searchAggColorField])
				require.True(t, hit.Fields[searchAggStockField].(bool))
				priceSum += hit.Fields[searchAggPriceField].(int64)
			}
			require.InEpsilon(t, float64(priceSum)/float64(len(bucket.Hits)), avgPrice, 0.000001)
		}
	}
}

func TestSearchAggregationNestedWithFilter(t *testing.T) {
	t.Parallel()

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	fixture := prepareSearchAggregationFixture(t)

	res, err := mc.Search(ctx, client.NewSearchOption(fixture.collectionName, 10, []entity.Vector{fixture.vectors[0]}).
		WithANNSField(searchAggVectorField).
		WithFilter(fmt.Sprintf("%s == true", searchAggStockField)).
		WithOutputFields(searchAggCategoryField, searchAggBrandField, searchAggRatingField, searchAggStockField).
		WithSearchAggregation(
			client.NewSearchAggregation([]string{searchAggCategoryField}, 2).
				WithMetric("item_count", "count", "*").
				WithOrder("_key", "asc").
				WithTopHits(client.NewTopHits(1).WithSort("_score", "asc")).
				WithSubAggregation(
					client.NewSearchAggregation([]string{searchAggBrandField}, 2).
						WithMetric("avg_rating", "avg", searchAggRatingField).
						WithOrder("avg_rating", "desc").
						WithTopHits(client.NewTopHits(1).WithSort(searchAggRatingField, "desc")),
				),
		))
	common.CheckErr(t, err, true)
	require.Len(t, res, 1)
	require.Len(t, res[0].AggregationBuckets, 2)

	for _, bucket := range res[0].AggregationBuckets {
		category := requireSearchAggregationKey[string](t, bucket, searchAggCategoryField)
		require.EqualValues(t, bucket.Count, bucket.Metrics["item_count"])
		require.NotEmpty(t, bucket.Hits)
		for _, hit := range bucket.Hits {
			require.Equal(t, category, hit.Fields[searchAggCategoryField])
			require.True(t, hit.Fields[searchAggStockField].(bool))
		}

		require.NotEmpty(t, bucket.SubGroups)
		require.LessOrEqual(t, len(bucket.SubGroups), 2)
		for _, subBucket := range bucket.SubGroups {
			brand := requireSearchAggregationKey[string](t, subBucket, searchAggBrandField)
			require.NotEmpty(t, subBucket.Hits)
			for _, hit := range subBucket.Hits {
				require.Equal(t, category, hit.Fields[searchAggCategoryField])
				require.Equal(t, brand, hit.Fields[searchAggBrandField])
				require.True(t, hit.Fields[searchAggStockField].(bool))
				require.InEpsilon(t, hit.Fields[searchAggRatingField].(float64), subBucket.Metrics["avg_rating"].(float64), 0.000001)
			}
		}
	}
}

func TestSearchAggregationRejectMutuallyExclusiveParams(t *testing.T) {
	t.Parallel()

	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	fixture := prepareSearchAggregationFixture(t)
	aggregation := client.NewSearchAggregation([]string{searchAggBrandField}, 2)

	_, err := mc.Search(ctx, client.NewSearchOption(fixture.collectionName, 10, []entity.Vector{fixture.vectors[0]}).
		WithANNSField(searchAggVectorField).
		WithGroupByField(searchAggBrandField).
		WithSearchAggregation(aggregation))
	common.CheckErr(t, err, false, "search_aggregation and group_by_field/group_size are mutually exclusive")

	_, err = mc.Search(ctx, client.NewSearchOption(fixture.collectionName, 10, []entity.Vector{fixture.vectors[0]}).
		WithANNSField(searchAggVectorField).
		WithSearchParam("offset", "1").
		WithSearchAggregation(aggregation))
	common.CheckErr(t, err, false, "offset is not supported with search_aggregation")

	iteratorOpt := client.NewSearchIteratorOption(fixture.collectionName, fixture.vectors[0]).
		WithANNSField(searchAggVectorField)
	iteratorOpt.WithSearchAggregation(aggregation)
	_, err = mc.SearchIterator(ctx, iteratorOpt)
	common.CheckErr(t, err, false, "search_aggregation is not supported with search iterator")
}

func requireSearchAggregationKey[T comparable](t *testing.T, bucket client.AggregationBucket, fieldName string) T {
	t.Helper()
	for _, entry := range bucket.Key {
		if entry.FieldName == fieldName {
			value, ok := entry.Value.(T)
			require.Truef(t, ok, "bucket key %s has unexpected type %T", fieldName, entry.Value)
			return value
		}
	}
	require.FailNowf(t, "bucket key not found", "field %s not found in %+v", fieldName, bucket.Key)
	var zero T
	return zero
}

func requireSearchAggregationScoresAsc(t *testing.T, hits []client.AggregationHit) {
	t.Helper()
	for i := 1; i < len(hits); i++ {
		require.LessOrEqual(t, hits[i-1].Score, hits[i].Score)
	}
}

func requireSearchAggregationHitPricesAsc(t *testing.T, hits []client.AggregationHit) {
	t.Helper()
	lastPrice := int64(math.MinInt64)
	for _, hit := range hits {
		price := hit.Fields[searchAggPriceField].(int64)
		require.GreaterOrEqual(t, price, lastPrice)
		lastPrice = price
	}
}

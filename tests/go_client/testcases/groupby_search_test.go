package testcases

import (
	"context"
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/client/v2/column"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	client "github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/milvus-io/milvus/tests/go_client/base"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

// Generate groupBy-supported vector indexes
func genGroupByVectorIndex(metricType entity.MetricType) []index.Index {
	nlist := 128
	idxFlat := index.NewFlatIndex(metricType)
	idxIvfFlat := index.NewIvfFlatIndex(metricType, nlist)
	idxHnsw := index.NewHNSWIndex(metricType, 8, 96)
	idxIvfSq8 := index.NewIvfSQ8Index(metricType, 128)

	allFloatIndex := []index.Index{
		idxFlat,
		idxIvfFlat,
		idxHnsw,
		idxIvfSq8,
	}
	return allFloatIndex
}

// Generate groupBy-supported vector indexes
func genGroupByBinaryIndex(metricType entity.MetricType) []index.Index {
	nlist := 128
	idxBinFlat := index.NewBinFlatIndex(metricType)
	idxBinIvfFlat := index.NewBinIvfFlatIndex(metricType, nlist)

	allFloatIndex := []index.Index{
		idxBinFlat,
		idxBinIvfFlat,
	}
	return allFloatIndex
}

func genUnsupportedFloatGroupByIndex() []index.Index {
	idxIvfPq := index.NewIvfPQIndex(entity.L2, 128, 16, 8)
	return []index.Index{
		idxIvfPq,
	}
}

func prepareDataForGroupBySearch(t *testing.T, loopInsert int, insertNi int, idx index.Index, withGrowing bool) (*base.MilvusClient, context.Context, string) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout*5)
	mc := createDefaultMilvusClient(ctx, t)

	// create collection with all datatype
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.AllFields), hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(true))
	for i := 0; i < loopInsert; i++ {
		prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithNb(insertNi))
	}

	if !withGrowing {
		prepare.FlushData(ctx, t, mc, schema.CollectionName)
	}
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema).TWithFieldIndex(map[string]index.Index{common.DefaultFloatVecFieldName: idx}))

	// create scalar index
	supportedGroupByFields := []string{
		common.DefaultInt64FieldName, common.DefaultInt8FieldName, common.DefaultInt16FieldName,
		common.DefaultInt32FieldName, common.DefaultVarcharFieldName, common.DefaultBoolFieldName,
	}
	for _, groupByField := range supportedGroupByFields {
		idxTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, groupByField, index.NewAutoIndex(entity.L2)))
		common.CheckErr(t, err, true)
		err = idxTask.Await(ctx)
		common.CheckErr(t, err, true)
	}
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))
	return mc, ctx, schema.CollectionName
}

// create coll with all datatype -> build all supported index
// -> search with WithGroupByField (int* + varchar + bool
// -> verify every top passage is the top of whole group
// output_fields: pk + groupBy
func TestSearchGroupByFloatDefault(t *testing.T) {
	t.Parallel()
	concurrency := 10
	ch := make(chan struct{}, concurrency)
	wg := sync.WaitGroup{}

	testFunc := func(idx index.Index) {
		defer func() {
			wg.Done()
			<-ch
		}()
		// prepare data
		mc, ctx, collName := prepareDataForGroupBySearch(t, 100, 200, idx, false)

		// search params
		queryVec := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeFloatVector)

		// search with groupBy field
		supportedGroupByFields := []string{
			common.DefaultInt64FieldName, common.DefaultInt8FieldName,
			common.DefaultInt16FieldName, common.DefaultInt32FieldName, common.DefaultVarcharFieldName, common.DefaultBoolFieldName,
		}
		for _, groupByField := range supportedGroupByFields {
			resGroupBy, _ := mc.Search(ctx, client.NewSearchOption(collName, common.DefaultLimit, queryVec).WithANNSField(common.DefaultFloatVecFieldName).
				WithGroupByField(groupByField).WithOutputFields(common.DefaultInt64FieldName, groupByField))

			// verify each topK entity is the top1 of the whole group
			hitsNum := 0
			total := 0
			for i := 0; i < common.DefaultNq; i++ {
				for j := 0; j < resGroupBy[i].ResultCount; j++ {
					groupByValue, _ := resGroupBy[i].GroupByValue.Get(j)
					pkValue, _ := resGroupBy[i].IDs.GetAsInt64(j)
					var expr string
					if groupByField == "varchar" {
						expr = fmt.Sprintf("%s == '%v' ", groupByField, groupByValue)
					} else {
						expr = fmt.Sprintf("%s == %v", groupByField, groupByValue)
					}

					// 	search filter with groupByValue is the top1
					resFilter, _ := mc.Search(ctx, client.NewSearchOption(collName, 1, queryVec[:1]).WithANNSField(common.DefaultFloatVecFieldName).
						WithGroupByField(groupByField).WithFilter(expr).WithOutputFields(common.DefaultInt64FieldName, groupByField))

					filterTop1Pk, _ := resFilter[0].IDs.GetAsInt64(0)
					if filterTop1Pk == pkValue {
						hitsNum += 1
					}
					total += 1
				}
			}

			// verify hits rate
			hitsRate := float32(hitsNum) / float32(total)
			_str := fmt.Sprintf("GroupBy search with field %s, nq=%d and limit=%d , then hitsNum= %d, hitsRate=%v\n",
				groupByField, common.DefaultNq, common.DefaultLimit, hitsNum, hitsRate)
			log.Println(_str)
			if groupByField != "bool" {
				// waiting for fix https://github.com/milvus-io/milvus/issues/32630
				require.GreaterOrEqualf(t, hitsRate, float32(0.1), _str)
			}
		}
	}
	for _, idx := range genGroupByVectorIndex(entity.L2) {
		ch <- struct{}{}
		wg.Add(1)
		go testFunc(idx)
	}
	wg.Wait()
}

func TestSearchGroupByFloatDefaultCosine(t *testing.T) {
	t.Parallel()
	concurrency := 10
	ch := make(chan struct{}, concurrency)
	wg := sync.WaitGroup{}

	testFunc := func(idx index.Index) {
		defer func() {
			wg.Done()
			<-ch
		}()
		// prepare data
		mc, ctx, collName := prepareDataForGroupBySearch(t, 100, 200, idx, false)

		// search params
		queryVec := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeFloatVector)

		// search with groupBy field without varchar
		supportedGroupByFields := []string{
			common.DefaultInt64FieldName, common.DefaultInt8FieldName,
			common.DefaultInt16FieldName, common.DefaultInt32FieldName, common.DefaultBoolFieldName,
		}
		for _, groupByField := range supportedGroupByFields {
			resGroupBy, _ := mc.Search(ctx, client.NewSearchOption(collName, common.DefaultLimit, queryVec).WithANNSField(common.DefaultFloatVecFieldName).
				WithGroupByField(groupByField).WithOutputFields(common.DefaultInt64FieldName, groupByField))

			// verify each topK entity is the top1 of the whole group
			hitsNum := 0
			total := 0
			for i := 0; i < common.DefaultNq; i++ {
				for j := 0; j < resGroupBy[i].ResultCount; j++ {
					groupByValue, _ := resGroupBy[i].GroupByValue.Get(j)
					pkValue, _ := resGroupBy[i].IDs.GetAsInt64(j)
					expr := fmt.Sprintf("%s == %v", groupByField, groupByValue)

					// 	search filter with groupByValue is the top1
					resFilter, _ := mc.Search(ctx, client.NewSearchOption(collName, 1, queryVec[:1]).WithANNSField(common.DefaultFloatVecFieldName).
						WithGroupByField(groupByField).WithFilter(expr).WithOutputFields(common.DefaultInt64FieldName, groupByField))

					filterTop1Pk, _ := resFilter[0].IDs.GetAsInt64(0)
					if filterTop1Pk == pkValue {
						hitsNum += 1
					}
					total += 1
				}
			}

			// verify hits rate
			hitsRate := float32(hitsNum) / float32(total)
			_str := fmt.Sprintf("GroupBy search with field %s, nq=%d and limit=%d , then hitsNum= %d, hitsRate=%v\n",
				groupByField, common.DefaultNq, common.DefaultLimit, hitsNum, hitsRate)
			log.Println(_str)
			if groupByField != "bool" {
				// waiting for fix https://github.com/milvus-io/milvus/issues/32630
				require.GreaterOrEqualf(t, hitsRate, float32(0.1), _str)
			}
		}
	}
	for _, idx := range genGroupByVectorIndex(entity.COSINE) {
		ch <- struct{}{}
		wg.Add(1)
		go testFunc(idx)
	}
	wg.Wait()
}

// test groupBy search sparse vector
func TestGroupBySearchSparseVector(t *testing.T) {
	t.Parallel()
	idxInverted := index.NewSparseInvertedIndex(entity.IP, 0.3)
	idxWand := index.NewSparseWANDIndex(entity.IP, 0.2)
	for _, idx := range []index.Index{idxInverted, idxWand} {
		ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
		mc := createDefaultMilvusClient(ctx, t)

		prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64VarcharSparseVec), hp.TNewFieldsOption().TWithMaxLen(common.TestMaxLen),
			hp.TNewSchemaOption().TWithEnableDynamicField(true))
		for i := 0; i < 100; i++ {
			prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithNb(200))
		}
		prepare.FlushData(ctx, t, mc, schema.CollectionName)
		prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema).TWithFieldIndex(map[string]index.Index{common.DefaultSparseVecFieldName: idx}))
		prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

		// groupBy search
		queryVec := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeSparseVector)

		resGroupBy, _ := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, queryVec).WithANNSField(common.DefaultSparseVecFieldName).
			WithGroupByField(common.DefaultVarcharFieldName).WithOutputFields(common.DefaultInt64FieldName, common.DefaultVarcharFieldName))

		// verify each topK entity is the top1 of the whole group
		hitsNum := 0
		total := 0
		for i := 0; i < common.DefaultNq; i++ {
			if resGroupBy[i].ResultCount > 0 {
				for j := 0; j < resGroupBy[i].ResultCount; j++ {
					groupByValue, _ := resGroupBy[i].GroupByValue.Get(j)
					pkValue, _ := resGroupBy[i].IDs.GetAsInt64(j)
					expr := fmt.Sprintf("%s == '%v' ", common.DefaultVarcharFieldName, groupByValue)
					// 	search filter with groupByValue is the top1
					resFilter, _ := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, 1, []entity.Vector{queryVec[i]}).
						WithANNSField(common.DefaultSparseVecFieldName).
						WithGroupByField(common.DefaultVarcharFieldName).
						WithFilter(expr).
						WithOutputFields(common.DefaultInt64FieldName, common.DefaultVarcharFieldName))

					filterTop1Pk, _ := resFilter[0].IDs.GetAsInt64(0)
					log.Printf("Search top1 with %s: groupByValue: %v, pkValue: %d. The returned pk by filter search is: %d",
						common.DefaultVarcharFieldName, groupByValue, pkValue, filterTop1Pk)
					if filterTop1Pk == pkValue {
						hitsNum += 1
					}
					total += 1
				}
			}
		}

		// verify hits rate
		hitsRate := float32(hitsNum) / float32(total)
		_str := fmt.Sprintf("GroupBy search with field %s, nq=%d and limit=%d , then hitsNum= %d, hitsRate=%v\n",
			common.DefaultVarcharFieldName, common.DefaultNq, common.DefaultLimit, hitsNum, hitsRate)
		log.Println(_str)
		require.GreaterOrEqualf(t, hitsRate, float32(0.8), _str)
	}
}

// binary vector -> not supported
func TestSearchGroupByBinaryDefault(t *testing.T) {
	t.Parallel()
	for _, metricType := range hp.SupportBinIvfFlatMetricType {
		for _, idx := range genGroupByBinaryIndex(metricType) {
			ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
			mc := createDefaultMilvusClient(ctx, t)

			prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.VarcharBinary), hp.TNewFieldsOption(),
				hp.TNewSchemaOption().TWithEnableDynamicField(true))
			for i := 0; i < 2; i++ {
				prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithNb(1000))
			}
			prepare.FlushData(ctx, t, mc, schema.CollectionName)
			prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema).TWithFieldIndex(map[string]index.Index{common.DefaultBinaryVecFieldName: idx}))
			prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

			// search params
			queryVec := hp.GenSearchVectors(2, common.DefaultDim, entity.FieldTypeBinaryVector)
			t.Log("Waiting for support for specifying search parameters")
			// sp, _ := index.NewBinIvfFlatIndexSearchParam(32)
			supportedGroupByFields := []string{common.DefaultVarcharFieldName, common.DefaultBinaryVecFieldName}

			// search with groupBy field
			for _, groupByField := range supportedGroupByFields {
				_, err := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, queryVec).WithGroupByField(groupByField).
					WithOutputFields(common.DefaultVarcharFieldName, groupByField))
				common.CheckErr(t, err, false, "not support search_group_by operation based on binary vector column")
			}
		}
	}
}

// binary vector -> growing segments, maybe brute force
// default Bounded ConsistencyLevel -> succ ??
// strong ConsistencyLevel -> error
func TestSearchGroupByBinaryGrowing(t *testing.T) {
	t.Parallel()
	for _, metricType := range hp.SupportBinIvfFlatMetricType {
		idxBinIvfFlat := index.NewBinIvfFlatIndex(metricType, 128)
		ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
		mc := createDefaultMilvusClient(ctx, t)

		prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.VarcharBinary), hp.TNewFieldsOption(),
			hp.TNewSchemaOption().TWithEnableDynamicField(true))
		prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema).TWithFieldIndex(map[string]index.Index{common.DefaultBinaryVecFieldName: idxBinIvfFlat}))
		prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))
		for i := 0; i < 2; i++ {
			prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithNb(1000))
		}

		// search params
		queryVec := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeBinaryVector)
		t.Log("Waiting for support for specifying search parameters")
		// sp, _ := index.NewBinIvfFlatIndexSearchParam(64)
		supportedGroupByFields := []string{common.DefaultVarcharFieldName}

		// search with groupBy field
		for _, groupByField := range supportedGroupByFields {
			_, err := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, queryVec).WithGroupByField(groupByField).
				WithOutputFields(common.DefaultVarcharFieldName, groupByField).WithConsistencyLevel(entity.ClStrong))
			common.CheckErr(t, err, false, "not support search_group_by operation based on binary vector column")
		}
	}
}

// groupBy in growing segments, maybe growing index or brute force
func TestSearchGroupByFloatGrowing(t *testing.T) {
	t.Parallel()
	concurrency := 10
	ch := make(chan struct{}, concurrency)
	wg := sync.WaitGroup{}

	testFunc := func(metricType entity.MetricType) {
		defer func() {
			wg.Done()
			<-ch
		}()

		idxHnsw := index.NewHNSWIndex(metricType, 8, 96)
		mc, ctx, collName := prepareDataForGroupBySearch(t, 100, 200, idxHnsw, true)

		// search params
		queryVec := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeFloatVector)
		supportedGroupByFields := []string{common.DefaultInt64FieldName, "int8", "int16", "int32", "varchar", "bool"}

		// search with groupBy field
		hitsNum := 0
		total := 0
		for _, groupByField := range supportedGroupByFields {
			resGroupBy, _ := mc.Search(ctx, client.NewSearchOption(collName, common.DefaultLimit, queryVec).WithANNSField(common.DefaultFloatVecFieldName).
				WithOutputFields(common.DefaultInt64FieldName, groupByField).WithGroupByField(groupByField).WithConsistencyLevel(entity.ClStrong))

			// verify each topK entity is the top1 in the group
			for i := 0; i < common.DefaultNq; i++ {
				for j := 0; j < resGroupBy[i].ResultCount; j++ {
					groupByValue, _ := resGroupBy[i].GroupByValue.Get(j)
					pkValue, _ := resGroupBy[i].IDs.GetAsInt64(j)
					var expr string
					if groupByField == "varchar" {
						expr = fmt.Sprintf("%s == '%v' ", groupByField, groupByValue)
					} else {
						expr = fmt.Sprintf("%s == %v", groupByField, groupByValue)
					}
					resFilter, _ := mc.Search(ctx, client.NewSearchOption(collName, 1, queryVec).WithANNSField(common.DefaultFloatVecFieldName).
						WithOutputFields(common.DefaultInt64FieldName, groupByField).WithGroupByField(groupByField).WithFilter(expr).WithConsistencyLevel(entity.ClStrong))

					// search filter with groupByValue is the top1
					filterTop1Pk, _ := resFilter[0].IDs.GetAsInt64(0)
					log.Printf("Search top1 with %s: groupByValue: %v, pkValue: %d. The returned pk by filter search is: %d",
						groupByField, groupByValue, pkValue, filterTop1Pk)
					if filterTop1Pk == pkValue {
						hitsNum += 1
					}
					total += 1
				}
			}
			// verify hits rate
			hitsRate := float32(hitsNum) / float32(total)
			_str := fmt.Sprintf("GroupBy search with field %s, nq=%d and limit=%d , then hitsNum= %d, hitsRate=%v\n",
				groupByField, common.DefaultNq, common.DefaultLimit, hitsNum, hitsRate)
			log.Println(_str)
			if groupByField != "bool" {
				require.GreaterOrEqualf(t, hitsRate, float32(0.8), _str)
			}
		}
	}

	for _, metricType := range hp.SupportFloatMetricType {
		ch <- struct{}{}
		wg.Add(1)
		go testFunc(metricType)
	}
	wg.Wait()
}

// groupBy + pagination
func TestSearchGroupByPagination(t *testing.T) {
	t.Parallel()
	// create index and load
	idx := index.NewHNSWIndex(entity.COSINE, 8, 96)
	mc, ctx, collName := prepareDataForGroupBySearch(t, 10, 1000, idx, false)

	// search params
	queryVec := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeFloatVector)
	offset := 10

	// search pagination & groupBy
	resGroupByPagination, _ := mc.Search(ctx, client.NewSearchOption(collName, common.DefaultLimit, queryVec).WithGroupByField(common.DefaultVarcharFieldName).WithOffset(offset).
		WithOutputFields(common.DefaultInt64FieldName, common.DefaultVarcharFieldName).WithANNSField(common.DefaultFloatVecFieldName))

	common.CheckSearchResult(t, resGroupByPagination, common.DefaultNq, common.DefaultLimit)

	// search limit=origin limit + offset
	resGroupByDefault, _ := mc.Search(ctx, client.NewSearchOption(collName, offset+common.DefaultLimit, queryVec).WithGroupByField(common.DefaultVarcharFieldName).
		WithOutputFields(common.DefaultInt64FieldName, common.DefaultVarcharFieldName).WithANNSField(common.DefaultFloatVecFieldName))

	for i := 0; i < common.DefaultNq; i++ {
		require.Equal(t, resGroupByDefault[i].IDs.(*column.ColumnInt64).Data()[10:], resGroupByPagination[i].IDs.(*column.ColumnInt64).Data())
	}
}

// only support: "FLAT", "IVF_FLAT", "HNSW"
func TestSearchGroupByUnsupportedIndex(t *testing.T) {
	t.Parallel()
	for _, idx := range genUnsupportedFloatGroupByIndex() {
		t.Run(string(idx.IndexType()), func(t *testing.T) {
			mc, ctx, collName := prepareDataForGroupBySearch(t, 3, 1000, idx, false)
			// groupBy search
			queryVec := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeFloatVector)
			_, err := mc.Search(ctx, client.NewSearchOption(collName, common.DefaultLimit, queryVec).WithGroupByField(common.DefaultVarcharFieldName).WithANNSField(common.DefaultFloatVecFieldName))
			common.CheckErr(t, err, false, "doesn't support")
		})
	}
}

// FLOAT, DOUBLE, JSON, ARRAY
func TestSearchGroupByUnsupportedDataType(t *testing.T) {
	idxHnsw := index.NewHNSWIndex(entity.L2, 8, 96)
	mc, ctx, collName := prepareDataForGroupBySearch(t, 1, 1000, idxHnsw, true)

	// groupBy search with unsupported field type
	queryVec := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeFloatVector)
	for _, unsupportedField := range []string{
		common.DefaultFloatFieldName, common.DefaultDoubleFieldName,
		common.DefaultJSONFieldName, common.DefaultFloatVecFieldName, common.DefaultInt8ArrayField, common.DefaultFloatArrayField,
	} {
		_, err := mc.Search(ctx, client.NewSearchOption(collName, common.DefaultLimit, queryVec).WithGroupByField(unsupportedField).WithANNSField(common.DefaultFloatVecFieldName))
		common.CheckErr(t, err, false, "unsupported data type")
	}
}

// groupBy + iterator -> not supported
func TestSearchGroupByIterator(t *testing.T) {
	// TODO: sdk support
}

// groupBy + range search -> not supported
func TestSearchGroupByRangeSearch(t *testing.T) {
	t.Skipf("https://github.com/milvus-io/milvus/issues/38846")
	idxHnsw := index.NewHNSWIndex(entity.COSINE, 8, 96)
	mc, ctx, collName := prepareDataForGroupBySearch(t, 1, 1000, idxHnsw, true)

	// groupBy search with range
	queryVec := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeFloatVector)

	// range search
	_, err := mc.Search(ctx, client.NewSearchOption(collName, common.DefaultLimit, queryVec).WithGroupByField(common.DefaultVarcharFieldName).
		WithANNSField(common.DefaultFloatVecFieldName).WithSearchParam("radius", "0").WithSearchParam("range_filter", "0.8"))
	common.CheckErr(t, err, false, "Not allowed to do range-search when doing search-group-by")
}

// groupBy + advanced search
func TestSearchGroupByHybridSearch(t *testing.T) {
	t.Skipf("Waiting for HybridSearch implemention")
}

package testcases

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	client "github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

func TestIndexVectorDefault(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout*2)
	mc := createDefaultMilvusClient(ctx, t)

	cp := hp.NewCreateCollectionParams(hp.Int64MultiVec)
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption())

	// insert
	ip := hp.NewInsertParams(schema)
	prepare.InsertData(ctx, t, mc, ip, hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)

	// index
	for _, idx := range hp.GenAllFloatIndex(entity.L2) {
		log.Debug("index", zap.String("name", idx.Name()), zap.Any("indexType", idx.IndexType()), zap.Any("params", idx.Params()))
		for _, fieldName := range []string{common.DefaultFloat16VecFieldName, common.DefaultBFloat16VecFieldName, common.DefaultFloatVecFieldName} {
			indexTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, fieldName, idx))
			common.CheckErr(t, err, true)
			err = indexTask.Await(ctx)
			common.CheckErr(t, err, true)

			descIdx, err := mc.DescribeIndex(ctx, client.NewDescribeIndexOption(schema.CollectionName, fieldName))
			common.CheckErr(t, err, true)
			common.CheckIndex(t, descIdx, index.NewGenericIndex(fieldName, idx.Params()), common.TNewCheckIndexOpt(common.DefaultNb))

			// drop index
			err = mc.DropIndex(ctx, client.NewDropIndexOption(schema.CollectionName, descIdx.Name()))
			common.CheckErr(t, err, true)
		}
	}
}

func TestIndexVectorIP(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout*2)
	mc := createDefaultMilvusClient(ctx, t)

	cp := hp.NewCreateCollectionParams(hp.Int64MultiVec)
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption())

	// insert
	ip := hp.NewInsertParams(schema)
	prepare.InsertData(ctx, t, mc, ip, hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)

	// index
	for _, idx := range hp.GenAllFloatIndex(entity.IP) {
		log.Debug("index", zap.String("name", idx.Name()), zap.Any("indexType", idx.IndexType()), zap.Any("params", idx.Params()))
		for _, fieldName := range []string{common.DefaultFloat16VecFieldName, common.DefaultBFloat16VecFieldName, common.DefaultFloatVecFieldName} {
			indexTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, fieldName, idx))
			common.CheckErr(t, err, true)
			err = indexTask.Await(ctx)
			common.CheckErr(t, err, true)

			expIdx := index.NewGenericIndex(fieldName, idx.Params())
			descIdx, err := mc.DescribeIndex(ctx, client.NewDescribeIndexOption(schema.CollectionName, fieldName))
			common.CheckErr(t, err, true)
			common.CheckIndex(t, descIdx, expIdx, common.TNewCheckIndexOpt(common.DefaultNb))

			// drop index
			err = mc.DropIndex(ctx, client.NewDropIndexOption(schema.CollectionName, expIdx.Name()))
			common.CheckErr(t, err, true)
		}
	}
}

func TestIndexVectorCosine(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout*2)
	mc := createDefaultMilvusClient(ctx, t)

	cp := hp.NewCreateCollectionParams(hp.Int64MultiVec)
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption())

	// insert
	ip := hp.NewInsertParams(schema)
	prepare.InsertData(ctx, t, mc, ip, hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)

	// index
	for _, idx := range hp.GenAllFloatIndex(entity.COSINE) {
		log.Debug("index", zap.String("name", idx.Name()), zap.Any("indexType", idx.IndexType()), zap.Any("params", idx.Params()))
		for _, fieldName := range []string{common.DefaultFloat16VecFieldName, common.DefaultBFloat16VecFieldName, common.DefaultFloatVecFieldName} {
			indexTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, fieldName, idx))
			common.CheckErr(t, err, true)
			err = indexTask.Await(ctx)
			common.CheckErr(t, err, true)

			expIdx := index.NewGenericIndex(fieldName, idx.Params())
			descIdx, err := mc.DescribeIndex(ctx, client.NewDescribeIndexOption(schema.CollectionName, fieldName))
			common.CheckErr(t, err, true)
			common.CheckIndex(t, descIdx, expIdx, common.TNewCheckIndexOpt(common.DefaultNb))

			// drop index
			err = mc.DropIndex(ctx, client.NewDropIndexOption(schema.CollectionName, expIdx.Name()))
			common.CheckErr(t, err, true)
		}
	}
}

func TestIndexAutoFloatVector(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	cp := hp.NewCreateCollectionParams(hp.Int64Vec)
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption())

	// insert
	ip := hp.NewInsertParams(schema)
	prepare.InsertData(ctx, t, mc, ip, hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)

	for _, invalidMt := range hp.SupportBinFlatMetricType {
		idx := index.NewAutoIndex(invalidMt)
		_, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, common.DefaultFloatVecFieldName, idx))
		common.CheckErr(t, err, false, fmt.Sprintf("float vector index does not support metric type: %s", invalidMt))
	}
	// auto index with different metric type on float vec
	for _, mt := range hp.SupportFloatMetricType {
		idx := index.NewAutoIndex(mt)
		indexTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, common.DefaultFloatVecFieldName, idx))
		common.CheckErr(t, err, true)
		err = indexTask.Await(ctx)
		common.CheckErr(t, err, true)

		expIdx := index.NewGenericIndex(common.DefaultFloatVecFieldName, idx.Params())
		descIdx, err := mc.DescribeIndex(ctx, client.NewDescribeIndexOption(schema.CollectionName, common.DefaultFloatVecFieldName))
		common.CheckErr(t, err, true)
		common.CheckIndex(t, descIdx, expIdx, common.TNewCheckIndexOpt(common.DefaultNb))

		// drop index
		err = mc.DropIndex(ctx, client.NewDropIndexOption(schema.CollectionName, expIdx.Name()))
		common.CheckErr(t, err, true)
	}
}

func TestIndexAutoBinaryVector(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	cp := hp.NewCreateCollectionParams(hp.VarcharBinary)
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption())

	// insert
	ip := hp.NewInsertParams(schema)
	prepare.InsertData(ctx, t, mc, ip, hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)

	// auto index with different metric type on float vec
	for _, unsupportedMt := range []entity.MetricType{entity.L2, entity.COSINE, entity.IP, entity.TANIMOTO, entity.SUPERSTRUCTURE, entity.SUBSTRUCTURE} {
		idx := index.NewAutoIndex(unsupportedMt)
		_, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, common.DefaultBinaryVecFieldName, idx))
		common.CheckErr(t, err, false, fmt.Sprintf("binary vector index does not support metric type: %s", unsupportedMt),
			"metric type SUPERSTRUCTURE not found or not supported, supported: [HAMMING JACCARD]",
			"metric type SUBSTRUCTURE not found or not supported, supported: [HAMMING JACCARD]")
	}

	// auto index with different metric type on binary vec
	for _, mt := range hp.SupportBinIvfFlatMetricType {
		idx := index.NewAutoIndex(mt)
		indexTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, common.DefaultBinaryVecFieldName, idx))
		common.CheckErr(t, err, true)
		err = indexTask.Await(ctx)
		common.CheckErr(t, err, true)

		expIdx := index.NewGenericIndex(common.DefaultBinaryVecFieldName, idx.Params())
		descIdx, err := mc.DescribeIndex(ctx, client.NewDescribeIndexOption(schema.CollectionName, common.DefaultBinaryVecFieldName))
		common.CheckErr(t, err, true)
		common.CheckIndex(t, descIdx, expIdx, common.TNewCheckIndexOpt(common.DefaultNb))

		// drop index
		err = mc.DropIndex(ctx, client.NewDropIndexOption(schema.CollectionName, expIdx.Name()))
		common.CheckErr(t, err, true)
	}
}

func TestIndexAutoSparseVector(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	cp := hp.NewCreateCollectionParams(hp.Int64VarcharSparseVec)
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption())

	// insert
	ip := hp.NewInsertParams(schema)
	prepare.InsertData(ctx, t, mc, ip, hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)

	// auto index with different metric type on float vec
	for _, unsupportedMt := range hp.UnsupportedSparseVecMetricsType {
		idx := index.NewAutoIndex(unsupportedMt)
		_, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, common.DefaultSparseVecFieldName, idx))
		common.CheckErr(t, err, false, "only IP&BM25 is the supported metric type for sparse index")
	}

	// auto index with different metric type on sparse vec
	idx := index.NewAutoIndex(entity.IP)
	indexTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, common.DefaultSparseVecFieldName, idx))
	common.CheckErr(t, err, true)
	err = indexTask.Await(ctx)
	common.CheckErr(t, err, true)

	expIdx := index.NewGenericIndex(common.DefaultSparseVecFieldName, idx.Params())
	descIdx, err := mc.DescribeIndex(ctx, client.NewDescribeIndexOption(schema.CollectionName, common.DefaultSparseVecFieldName))
	common.CheckErr(t, err, true)
	common.CheckIndex(t, descIdx, expIdx, common.TNewCheckIndexOpt(common.DefaultNb))

	// drop index
	err = mc.DropIndex(ctx, client.NewDropIndexOption(schema.CollectionName, expIdx.Name()))
	common.CheckErr(t, err, true)
}

// test create auto index on all vector and scalar index
func TestCreateAutoIndexAllFields(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	cp := hp.NewCreateCollectionParams(hp.AllFields)
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption())

	// insert
	ip := hp.NewInsertParams(schema)
	prepare.InsertData(ctx, t, mc, ip, hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)

	var expFields []string
	var idx index.Index
	for _, field := range schema.Fields {
		if field.DataType == entity.FieldTypeJSON {
			idx = index.NewAutoIndex(entity.IP)
			_, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, field.Name, idx))
			common.CheckErr(t, err, false, fmt.Sprintf("create auto index on type:%s is not supported", field.DataType))
		} else {
			if field.DataType == entity.FieldTypeBinaryVector {
				idx = index.NewAutoIndex(entity.JACCARD)
			} else {
				idx = index.NewAutoIndex(entity.IP)
			}
			idxTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, field.Name, idx))
			common.CheckErr(t, err, true)
			err = idxTask.Await(ctx)
			common.CheckErr(t, err, true)

			// describe index
			descIdx, descErr := mc.DescribeIndex(ctx, client.NewDescribeIndexOption(schema.CollectionName, field.Name))
			common.CheckErr(t, descErr, true)
			common.CheckIndex(t, descIdx, index.NewGenericIndex(field.Name, idx.Params()), common.TNewCheckIndexOpt(common.DefaultNb))
		}
		expFields = append(expFields, field.Name)
	}

	// load -> search and output all vector fields
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))
	queryVec := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeFloatVector)
	searchRes, err := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, queryVec).WithANNSField(common.DefaultFloatVecFieldName).WithOutputFields("*"))
	common.CheckErr(t, err, true)
	common.CheckOutputFields(t, expFields, searchRes[0].Fields)
}

func TestIndexBinaryFlat(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	cp := hp.NewCreateCollectionParams(hp.VarcharBinary)
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption())

	// insert
	ip := hp.NewInsertParams(schema)
	prepare.InsertData(ctx, t, mc, ip, hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)

	// create index for all binary
	for _, metricType := range hp.SupportBinFlatMetricType {
		idx := index.NewBinFlatIndex(metricType)
		indexTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, common.DefaultBinaryVecFieldName, idx))
		common.CheckErr(t, err, true)
		err = indexTask.Await(ctx)
		common.CheckErr(t, err, true)

		expIdx := index.NewGenericIndex(common.DefaultBinaryVecFieldName, idx.Params())
		descIdx, err := mc.DescribeIndex(ctx, client.NewDescribeIndexOption(schema.CollectionName, common.DefaultBinaryVecFieldName))
		common.CheckErr(t, err, true)
		common.CheckIndex(t, descIdx, expIdx, common.TNewCheckIndexOpt(common.DefaultNb))

		// drop index
		err = mc.DropIndex(ctx, client.NewDropIndexOption(schema.CollectionName, expIdx.Name()))
		common.CheckErr(t, err, true)
	}
}

func TestIndexBinaryIvfFlat(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	cp := hp.NewCreateCollectionParams(hp.VarcharBinary)
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption())

	// insert
	ip := hp.NewInsertParams(schema)
	prepare.InsertData(ctx, t, mc, ip, hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)

	// create index for all binary
	for _, metricType := range hp.SupportBinIvfFlatMetricType {
		idx := index.NewBinIvfFlatIndex(metricType, 32)
		indexTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, common.DefaultBinaryVecFieldName, idx))
		common.CheckErr(t, err, true)
		err = indexTask.Await(ctx)
		common.CheckErr(t, err, true)

		expIdx := index.NewGenericIndex(common.DefaultBinaryVecFieldName, idx.Params())
		descIdx, err := mc.DescribeIndex(ctx, client.NewDescribeIndexOption(schema.CollectionName, common.DefaultBinaryVecFieldName))
		common.CheckErr(t, err, true)
		common.CheckIndex(t, descIdx, expIdx, common.TNewCheckIndexOpt(common.DefaultNb))

		// drop index
		err = mc.DropIndex(ctx, client.NewDropIndexOption(schema.CollectionName, expIdx.Name()))
		common.CheckErr(t, err, true)
	}
}

// test create binary index with unsupported metrics type
func TestCreateBinaryIndexNotSupportedMetricType(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	cp := hp.NewCreateCollectionParams(hp.VarcharBinary)
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption())

	// insert
	ip := hp.NewInsertParams(schema)
	prepare.InsertData(ctx, t, mc, ip, hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)

	// create BinIvfFlat, BinFlat index with not supported metric type
	invalidMetricTypes := []entity.MetricType{
		entity.L2,
		entity.COSINE,
		entity.IP,
		entity.TANIMOTO,
	}
	for _, metricType := range invalidMetricTypes {
		// create BinFlat
		idxBinFlat := index.NewBinFlatIndex(metricType)
		_, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, common.DefaultBinaryVecFieldName, idxBinFlat))
		common.CheckErr(t, err, false, fmt.Sprintf("binary vector index does not support metric type: %v", metricType))
	}

	invalidMetricTypes2 := []entity.MetricType{
		entity.L2,
		entity.COSINE,
		entity.IP,
		entity.TANIMOTO,
		entity.SUBSTRUCTURE,
		entity.SUPERSTRUCTURE,
	}

	for _, metricType := range invalidMetricTypes2 {
		// create BinIvfFlat index
		idxBinIvfFlat := index.NewBinIvfFlatIndex(metricType, 64)
		_, errIvf := mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, common.DefaultBinaryVecFieldName, idxBinIvfFlat))
		common.CheckErr(t, errIvf, false, fmt.Sprintf("binary vector index does not support metric type: %s", metricType),
			"supported: [HAMMING JACCARD]")
	}
}

func TestIndexInvalidMetricType(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	cp := hp.NewCreateCollectionParams(hp.Int64Vec)
	_, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(true))

	for _, mt := range []entity.MetricType{entity.HAMMING, entity.JACCARD, entity.TANIMOTO, entity.SUBSTRUCTURE, entity.SUPERSTRUCTURE} {
		idxScann := index.NewSCANNIndex(mt, 64, true)
		_, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, common.DefaultFloatVecFieldName, idxScann))
		common.CheckErr(t, err, false,
			fmt.Sprintf("float vector index does not support metric type: %s", mt))

		idxFlat := index.NewFlatIndex(mt)
		_, err1 := mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, common.DefaultFloatVecFieldName, idxFlat))
		common.CheckErr(t, err1, false,
			fmt.Sprintf("float vector index does not support metric type: %s", mt))
	}
}

// Trie scalar Trie index only supported on varchar
func TestCreateTrieScalarIndex(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	cp := hp.NewCreateCollectionParams(hp.Int64VecAllScalar)
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption())

	// insert
	ip := hp.NewInsertParams(schema)
	prepare.InsertData(ctx, t, mc, ip, hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)

	// create Trie scalar index on varchar field
	idx := index.NewTrieIndex()
	for _, field := range schema.Fields {
		if hp.SupportScalarIndexFieldType(field.DataType) {
			if field.DataType == entity.FieldTypeVarChar {
				idxTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, field.Name, idx))
				common.CheckErr(t, err, true)
				err = idxTask.Await(ctx)
				common.CheckErr(t, err, true)

				// describe index
				expIndex := index.NewGenericIndex(field.Name, idx.Params())
				descIdx, err := mc.DescribeIndex(ctx, client.NewDescribeIndexOption(schema.CollectionName, field.Name))
				common.CheckErr(t, err, true)
				common.CheckIndex(t, descIdx, expIndex, common.TNewCheckIndexOpt(common.DefaultNb))
			} else {
				_, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, field.Name, idx))
				common.CheckErr(t, err, false, "TRIE are only supported on varchar field")
			}
		}
	}
}

// Sort scalar index only supported on numeric field
func TestCreateSortedScalarIndex(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	cp := hp.NewCreateCollectionParams(hp.Int64VecAllScalar)
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption())

	// insert
	ip := hp.NewInsertParams(schema)
	prepare.InsertData(ctx, t, mc, ip, hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))

	// create Trie scalar index on varchar field
	idx := index.NewSortedIndex()
	for _, field := range schema.Fields {
		if hp.SupportScalarIndexFieldType(field.DataType) {
			if field.DataType == entity.FieldTypeVarChar || field.DataType == entity.FieldTypeBool ||
				field.DataType == entity.FieldTypeJSON || field.DataType == entity.FieldTypeArray {
				_, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, field.Name, idx))
				common.CheckErr(t, err, false, "STL_SORT are only supported on numeric field")
			} else {
				idxTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, field.Name, idx))
				common.CheckErr(t, err, true)
				err = idxTask.Await(ctx)
				common.CheckErr(t, err, true)

				// describe index
				expIndex := index.NewGenericIndex(field.Name, idx.Params())
				descIdx, err := mc.DescribeIndex(ctx, client.NewDescribeIndexOption(schema.CollectionName, field.Name))
				common.CheckErr(t, err, true)
				common.CheckIndex(t, descIdx, expIndex, common.TNewCheckIndexOpt(common.DefaultNb))
			}
		}
	}
	// load -> search and output all fields
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	queryVec := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeFloatVector)
	expr := fmt.Sprintf("%s > 10", common.DefaultInt64FieldName)
	searchRes, err := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, queryVec).WithFilter(expr).WithOutputFields("*"))
	common.CheckErr(t, err, true)
	expFields := make([]string, 0, len(schema.Fields))
	for _, field := range schema.Fields {
		expFields = append(expFields, field.Name)
	}
	common.CheckOutputFields(t, expFields, searchRes[0].Fields)
}

// create Inverted index for all scalar fields
func TestCreateInvertedScalarIndex(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	cp := hp.NewCreateCollectionParams(hp.Int64VecAllScalar)
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption())

	// insert
	ip := hp.NewInsertParams(schema)
	prepare.InsertData(ctx, t, mc, ip, hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))

	// create Trie scalar index on varchar field
	idx := index.NewInvertedIndex()
	for _, field := range schema.Fields {
		if hp.SupportScalarIndexFieldType(field.DataType) {
			idxTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, field.Name, idx))
			common.CheckErr(t, err, true)
			err = idxTask.Await(ctx)
			common.CheckErr(t, err, true)

			// describe index
			expIndex := index.NewGenericIndex(field.Name, idx.Params())
			_index, _ := mc.DescribeIndex(ctx, client.NewDescribeIndexOption(schema.CollectionName, field.Name))
			common.CheckIndex(t, _index, expIndex, common.TNewCheckIndexOpt(common.DefaultNb))
		}
	}
	// load -> search and output all fields
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	queryVec := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeFloatVector)
	expr := fmt.Sprintf("%s > 10", common.DefaultInt64FieldName)
	searchRes, err := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, queryVec).WithFilter(expr).WithOutputFields("*"))
	common.CheckErr(t, err, true)
	expFields := make([]string, 0, len(schema.Fields))
	for _, field := range schema.Fields {
		expFields = append(expFields, field.Name)
	}
	common.CheckOutputFields(t, expFields, searchRes[0].Fields)
}

// test create index on vector field -> error
func TestCreateScalarIndexVectorField(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	cp := hp.NewCreateCollectionParams(hp.Int64MultiVec)
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption())

	// insert
	ip := hp.NewInsertParams(schema)
	prepare.InsertData(ctx, t, mc, ip, hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)

	for _, idx := range []index.Index{index.NewInvertedIndex(), index.NewSortedIndex(), index.NewTrieIndex()} {
		for _, fieldName := range []string{
			common.DefaultFloatVecFieldName, common.DefaultBinaryVecFieldName,
			common.DefaultBFloat16VecFieldName, common.DefaultFloat16VecFieldName,
		} {
			_, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, fieldName, idx))
			common.CheckErr(t, err, false, "metric type not set for vector index")
		}
	}
}

// test create scalar index with vector field name
func TestCreateIndexWithOtherFieldName(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	cp := hp.NewCreateCollectionParams(hp.VarcharBinary)
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption())

	// insert
	ip := hp.NewInsertParams(schema)
	prepare.InsertData(ctx, t, mc, ip, hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)

	// create index with vector field name as index name (vector field name is the vector default index name)
	idx := index.NewInvertedIndex()
	idxTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, common.DefaultVarcharFieldName, idx).WithIndexName(common.DefaultBinaryVecFieldName))
	common.CheckErr(t, err, true)
	err = idxTask.Await(ctx)
	common.CheckErr(t, err, true)

	// describe index
	expIndex := index.NewGenericIndex(common.DefaultBinaryVecFieldName, idx.Params())
	descIdx, err := mc.DescribeIndex(ctx, client.NewDescribeIndexOption(schema.CollectionName, common.DefaultBinaryVecFieldName))
	common.CheckErr(t, err, true)
	common.CheckIndex(t, descIdx, expIndex, common.TNewCheckIndexOpt(common.DefaultNb))

	// create index in binary field with default name
	idxBinary := index.NewBinFlatIndex(entity.JACCARD)
	_, err = mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, common.DefaultBinaryVecFieldName, idxBinary))
	common.CheckErr(t, err, false, "CreateIndex failed: at most one distinct index is allowed per field")
}

// create all scalar index on json field -> error
func TestCreateIndexJsonField(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	cp := hp.NewCreateCollectionParams(hp.Int64VecJSON)
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption())

	// insert
	ip := hp.NewInsertParams(schema)
	prepare.InsertData(ctx, t, mc, ip, hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)

	// create vector index on json field
	idx := index.NewSCANNIndex(entity.L2, 8, false)
	_, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, common.DefaultJSONFieldName, idx).WithIndexName("json_index"))
	common.CheckErr(t, err, false, "index SCANN only supports vector data type")

	// create scalar index on json field
	type scalarIndexError struct {
		idx    index.Index
		errMsg string
	}
	inxError := []scalarIndexError{
		{index.NewInvertedIndex(), "INVERTED are not supported on JSON field"},
		{index.NewSortedIndex(), "STL_SORT are only supported on numeric field"},
		{index.NewTrieIndex(), "TRIE are only supported on varchar field"},
	}
	for _, idxErr := range inxError {
		_, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, common.DefaultJSONFieldName, idxErr.idx).WithIndexName("json_index"))
		common.CheckErr(t, err, false, idxErr.errMsg)
	}
}

// array field on supported array field
func TestCreateUnsupportedIndexArrayField(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	cp := hp.NewCreateCollectionParams(hp.Int64VecArray)
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption())

	// insert
	ip := hp.NewInsertParams(schema)
	prepare.InsertData(ctx, t, mc, ip, hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	type scalarIndexError struct {
		idx    index.Index
		errMsg string
	}
	inxError := []scalarIndexError{
		{index.NewSortedIndex(), "STL_SORT are only supported on numeric field"},
		{index.NewTrieIndex(), "TRIE are only supported on varchar field"},
	}

	// create scalar and vector index on array field
	vectorIdx := index.NewSCANNIndex(entity.L2, 10, false)
	for _, idxErr := range inxError {
		for _, field := range schema.Fields {
			if field.DataType == entity.FieldTypeArray {
				// create vector index
				_, err1 := mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, field.Name, vectorIdx).WithIndexName("vector_index"))
				common.CheckErr(t, err1, false, "index SCANN only supports vector data type")

				// create scalar index
				_, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, field.Name, idxErr.idx))
				common.CheckErr(t, err, false, idxErr.errMsg)
			}
		}
	}
}

// create inverted index on array field
func TestCreateInvertedIndexArrayField(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	cp := hp.NewCreateCollectionParams(hp.Int64VecArray)
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption())

	// insert
	ip := hp.NewInsertParams(schema)
	prepare.InsertData(ctx, t, mc, ip, hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))

	// create scalar and vector index on array field
	for _, field := range schema.Fields {
		if field.DataType == entity.FieldTypeArray {
			log.Debug("array field", zap.String("name", field.Name), zap.Any("element type", field.ElementType))

			// create scalar index
			_, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, field.Name, index.NewInvertedIndex()))
			common.CheckErr(t, err, true)
		}
	}

	// load -> search and output all fields
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))
	queryVec := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeFloatVector)
	searchRes, errSearch := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, queryVec).WithConsistencyLevel(entity.ClStrong).WithOutputFields("*"))
	common.CheckErr(t, errSearch, true)
	var expFields []string
	for _, field := range schema.Fields {
		expFields = append(expFields, field.Name)
	}
	common.CheckSearchResult(t, searchRes, common.DefaultNq, common.DefaultLimit)
	common.CheckOutputFields(t, expFields, searchRes[0].Fields)
}

// test create index without specify index name: default index name is field name
func TestCreateIndexWithoutName(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	cp := hp.NewCreateCollectionParams(hp.Int64Vec)
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption())

	// insert
	ip := hp.NewInsertParams(schema)
	prepare.InsertData(ctx, t, mc, ip, hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)

	// create index
	idx := index.NewHNSWIndex(entity.L2, 8, 96)
	idxTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, common.DefaultFloatVecFieldName, idx))
	common.CheckErr(t, err, true)
	err = idxTask.Await(ctx)
	common.CheckErr(t, err, true)

	// describe index return index with default name
	idxDesc, _ := mc.DescribeIndex(ctx, client.NewDescribeIndexOption(schema.CollectionName, common.DefaultFloatVecFieldName))
	expIndex := index.NewGenericIndex(common.DefaultFloatVecFieldName, idx.Params())
	require.Equal(t, common.DefaultFloatVecFieldName, idxDesc.Name())
	common.CheckIndex(t, idxDesc, expIndex, common.TNewCheckIndexOpt(common.DefaultNb))
}

// test create index on same field twice
func TestCreateIndexDup(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	cp := hp.NewCreateCollectionParams(hp.Int64Vec)
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption())

	// insert
	ip := hp.NewInsertParams(schema)
	prepare.InsertData(ctx, t, mc, ip, hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)

	// index dup
	idxHnsw := index.NewHNSWIndex(entity.L2, 8, 96)
	idxIvfSq8 := index.NewIvfSQ8Index(entity.L2, 128)
	idxTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, common.DefaultFloatVecFieldName, idxHnsw))
	common.CheckErr(t, err, true)
	idxTask.Await(ctx)

	// describe index
	_index, _ := mc.DescribeIndex(ctx, client.NewDescribeIndexOption(schema.CollectionName, common.DefaultFloatVecFieldName))
	expIndex := index.NewGenericIndex(common.DefaultFloatVecFieldName, idxHnsw.Params())
	common.CheckIndex(t, _index, expIndex, common.TNewCheckIndexOpt(common.DefaultNb))

	_, err = mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, common.DefaultFloatVecFieldName, idxIvfSq8))
	common.CheckErr(t, err, false, "CreateIndex failed: at most one distinct index is allowed per field")
}

func TestCreateIndexSparseVectorGeneric(t *testing.T) {
	idxInverted := index.NewGenericIndex(common.DefaultSparseVecFieldName, map[string]string{"drop_ratio_build": "0.2", index.MetricTypeKey: "IP", index.IndexTypeKey: "SPARSE_INVERTED_INDEX"})
	idxWand := index.NewGenericIndex(common.DefaultSparseVecFieldName, map[string]string{"drop_ratio_build": "0.3", index.MetricTypeKey: "IP", index.IndexTypeKey: "SPARSE_WAND"})

	for _, idx := range []index.Index{idxInverted, idxWand} {
		ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
		mc := createDefaultMilvusClient(ctx, t)

		cp := hp.NewCreateCollectionParams(hp.Int64VarcharSparseVec)
		prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(true))

		// insert
		ip := hp.NewInsertParams(schema)
		prepare.InsertData(ctx, t, mc, ip, hp.TNewDataOption().TWithSparseMaxLen(100))
		prepare.FlushData(ctx, t, mc, schema.CollectionName)

		// create index
		idxTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, common.DefaultSparseVecFieldName, idx))
		common.CheckErr(t, err, true)
		err = idxTask.Await(ctx)
		common.CheckErr(t, err, true)

		descIdx, err := mc.DescribeIndex(ctx, client.NewDescribeIndexOption(schema.CollectionName, common.DefaultSparseVecFieldName))
		common.CheckErr(t, err, true)
		common.CheckIndex(t, descIdx, index.NewGenericIndex(common.DefaultSparseVecFieldName, idx.Params()), common.TNewCheckIndexOpt(common.DefaultNb))
	}
}

func TestCreateIndexSparseVector(t *testing.T) {
	idxInverted1 := index.NewSparseInvertedIndex(entity.IP, 0.2)
	idxWand1 := index.NewSparseWANDIndex(entity.IP, 0.3)
	for _, idx := range []index.Index{idxInverted1, idxWand1} {
		ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
		mc := createDefaultMilvusClient(ctx, t)

		cp := hp.NewCreateCollectionParams(hp.Int64VarcharSparseVec)
		prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(true))

		// insert
		ip := hp.NewInsertParams(schema)
		prepare.InsertData(ctx, t, mc, ip, hp.TNewDataOption().TWithSparseMaxLen(100))
		prepare.FlushData(ctx, t, mc, schema.CollectionName)

		// describe index
		idxTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, common.DefaultSparseVecFieldName, idx))
		common.CheckErr(t, err, true)
		err = idxTask.Await(ctx)
		common.CheckErr(t, err, true)
		descIdx, err := mc.DescribeIndex(ctx, client.NewDescribeIndexOption(schema.CollectionName, common.DefaultSparseVecFieldName))
		common.CheckErr(t, err, true)
		common.CheckIndex(t, descIdx, index.NewGenericIndex(common.DefaultSparseVecFieldName, idx.Params()), common.TNewCheckIndexOpt(common.DefaultNb))
	}
}

func TestCreateSparseIndexInvalidParams(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	cp := hp.NewCreateCollectionParams(hp.Int64VarcharSparseVec)
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(true))

	// insert
	ip := hp.NewInsertParams(schema)
	prepare.InsertData(ctx, t, mc, ip, hp.TNewDataOption().TWithSparseMaxLen(100))
	prepare.FlushData(ctx, t, mc, schema.CollectionName)

	// create index with invalid metric type
	for _, mt := range hp.UnsupportedSparseVecMetricsType {
		idxInverted := index.NewSparseInvertedIndex(mt, 0.2)
		_, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, common.DefaultSparseVecFieldName, idxInverted))
		common.CheckErr(t, err, false, "only IP&BM25 is the supported metric type for sparse index")

		idxWand := index.NewSparseWANDIndex(mt, 0.2)
		_, err = mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, common.DefaultSparseVecFieldName, idxWand))
		common.CheckErr(t, err, false, "only IP&BM25 is the supported metric type for sparse index")
	}

	// create index with invalid drop_ratio_build
	for _, drb := range []float64{-0.3, 1.3} {
		idxInverted := index.NewSparseInvertedIndex(entity.IP, drb)
		_, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, common.DefaultSparseVecFieldName, idxInverted))
		common.CheckErr(t, err, false, "Out of range in json: param 'drop_ratio_build'")

		idxWand := index.NewSparseWANDIndex(entity.IP, drb)
		_, err1 := mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, common.DefaultSparseVecFieldName, idxWand))
		common.CheckErr(t, err1, false, "Out of range in json: param 'drop_ratio_build'")
	}
}

// create sparse unsupported index: other vector index and scalar index and auto index
func TestCreateSparseUnsupportedIndex(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	cp := hp.NewCreateCollectionParams(hp.Int64VarcharSparseVec)
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(true))

	// insert
	ip := hp.NewInsertParams(schema)
	prepare.InsertData(ctx, t, mc, ip, hp.TNewDataOption().TWithSparseMaxLen(100))
	prepare.FlushData(ctx, t, mc, schema.CollectionName)

	// create unsupported vector index on sparse field
	for _, idx := range hp.GenAllFloatIndex(entity.IP) {
		_, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, common.DefaultSparseVecFieldName, idx))
		common.CheckErr(t, err, false, fmt.Sprintf("data type SparseFloatVector can't build with this index %v", idx.IndexType()))
	}

	// create scalar index on sparse vector
	for _, idx := range []index.Index{
		index.NewTrieIndex(),
		index.NewSortedIndex(),
		index.NewInvertedIndex(),
	} {
		_, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, common.DefaultSparseVecFieldName, idx))
		common.CheckErr(t, err, false, "metric type not set for vector index")
	}
}

// test new index by Generic index
func TestCreateIndexGeneric(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	cp := hp.NewCreateCollectionParams(hp.Int64Vec)
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(true))

	// insert
	ip := hp.NewInsertParams(schema)
	prepare.InsertData(ctx, t, mc, ip, hp.TNewDataOption().TWithSparseMaxLen(100))
	prepare.FlushData(ctx, t, mc, schema.CollectionName)

	// create index
	for _, field := range schema.Fields {
		idx := index.NewGenericIndex(field.Name, map[string]string{index.IndexTypeKey: string(index.AUTOINDEX), index.MetricTypeKey: string(entity.COSINE)})
		idxTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, field.Name, idx))
		common.CheckErr(t, err, true)
		err = idxTask.Await(ctx)
		common.CheckErr(t, err, true)

		descIdx, err := mc.DescribeIndex(ctx, client.NewDescribeIndexOption(schema.CollectionName, field.Name))
		common.CheckErr(t, err, true)
		common.CheckIndex(t, descIdx, index.NewGenericIndex(field.Name, idx.Params()), common.TNewCheckIndexOpt(common.DefaultNb))
	}
}

// test create index with not exist index name and not exist field name
func TestIndexNotExistName(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	// create index with not exist collection
	idx := index.NewHNSWIndex(entity.L2, 8, 96)
	_, err := mc.CreateIndex(ctx, client.NewCreateIndexOption("haha", common.DefaultFloatVecFieldName, idx))
	common.CheckErr(t, err, false, "collection not found")

	// create index with not exist field name
	cp := hp.NewCreateCollectionParams(hp.Int64Vec)
	_, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(true))
	_, err1 := mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, "aaa", idx))
	common.CheckErr(t, err1, false, "cannot create index on non-exist field: aaa")

	// describe index with not exist field name
	_, errDesc := mc.DescribeIndex(ctx, client.NewDescribeIndexOption(schema.CollectionName, "aaa"))
	common.CheckErr(t, errDesc, false, "index not found[indexName=aaa]")

	// drop index with not exist field name
	errDrop := mc.DropIndex(ctx, client.NewDropIndexOption(schema.CollectionName, "aaa"))
	common.CheckErr(t, errDrop, true)
}

// test create float / binary / sparse vector index on non-vector field
func TestCreateVectorIndexScalarField(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	cp := hp.NewCreateCollectionParams(hp.Int64VecAllScalar)
	_, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(true))

	// create index
	for _, field := range schema.Fields {
		if field.DataType < 100 {
			// create float vector index on scalar field
			for _, idx := range hp.GenAllFloatIndex(entity.COSINE) {
				_, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, field.Name, idx))
				expErrorMsg := fmt.Sprintf("index %s only supports vector data type", idx.IndexType())
				common.CheckErr(t, err, false, expErrorMsg)
			}

			// create binary vector index on scalar field
			for _, idxBinary := range []index.Index{index.NewBinFlatIndex(entity.IP), index.NewBinIvfFlatIndex(entity.COSINE, 64)} {
				_, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, field.Name, idxBinary))
				expErrorMsg := fmt.Sprintf("index %s only supports vector data type", idxBinary.IndexType())
				common.CheckErr(t, err, false, expErrorMsg)
			}

			// create sparse vector index on scalar field
			for _, idxSparse := range []index.Index{index.NewSparseInvertedIndex(entity.IP, 0.2), index.NewSparseWANDIndex(entity.IP, 0.3)} {
				_, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, field.Name, idxSparse))
				expErrorMsg := fmt.Sprintf("index %s only supports vector data type", idxSparse.IndexType())
				common.CheckErr(t, err, false, expErrorMsg)
			}
		}
	}
}

// test create index with invalid params
func TestCreateIndexInvalidParams(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	cp := hp.NewCreateCollectionParams(hp.Int64Vec)
	_, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(true))

	// invalid IvfFlat nlist [1, 65536]
	errMsg := "Out of range in json: param 'nlist'"
	for _, invalidNlist := range []int{0, -1, 65536 + 1} {
		// IvfFlat
		idxIvfFlat := index.NewIvfFlatIndex(entity.L2, invalidNlist)
		_, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, common.DefaultFloatVecFieldName, idxIvfFlat))
		common.CheckErr(t, err, false, errMsg)
		// IvfSq8
		idxIvfSq8 := index.NewIvfSQ8Index(entity.L2, invalidNlist)
		_, err = mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, common.DefaultFloatVecFieldName, idxIvfSq8))
		common.CheckErr(t, err, false, errMsg)
		// IvfPq
		idxIvfPq := index.NewIvfPQIndex(entity.L2, invalidNlist, 16, 8)
		_, err = mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, common.DefaultFloatVecFieldName, idxIvfPq))
		common.CheckErr(t, err, false, errMsg)
		// scann
		idxScann := index.NewSCANNIndex(entity.L2, invalidNlist, true)
		_, err = mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, common.DefaultFloatVecFieldName, idxScann))
		common.CheckErr(t, err, false, errMsg)
	}

	// invalid IvfPq params m dim ≡ 0 (mod m), nbits [1, 16]
	for _, invalidNBits := range []int{0, 65} {
		// IvfFlat
		idxIvfPq := index.NewIvfPQIndex(entity.L2, 128, 8, invalidNBits)
		_, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, common.DefaultFloatVecFieldName, idxIvfPq))
		common.CheckErr(t, err, false, "Out of range in json: param 'nbits'")
	}

	idxIvfPq := index.NewIvfPQIndex(entity.L2, 128, 7, 8)
	_, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, common.DefaultFloatVecFieldName, idxIvfPq))
	common.CheckErr(t, err, false, "The dimension of a vector (dim) should be a multiple of the number of subquantizers (m)")

	// invalid Hnsw M [1, 2048], efConstruction [1, 2147483647]
	for _, invalidM := range []int{0, 2049} {
		// IvfFlat
		idxHnsw := index.NewHNSWIndex(entity.L2, invalidM, 96)
		_, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, common.DefaultFloatVecFieldName, idxHnsw))
		common.CheckErr(t, err, false, "Out of range in json: param 'M'")
	}
	for _, invalidEfConstruction := range []int{0, 2147483647 + 1} {
		// IvfFlat
		idxHnsw := index.NewHNSWIndex(entity.L2, 8, invalidEfConstruction)
		_, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, common.DefaultFloatVecFieldName, idxHnsw))
		common.CheckErr(t, err, false, "Out of range in json: param 'efConstruction'", "integer value out of range, key: 'efConstruction'")
	}
}

// test create index with nil index
func TestCreateIndexNil(t *testing.T) {
	t.Skip("Issue: https://github.com/milvus-io/milvus-sdk-go/issues/358")
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	cp := hp.NewCreateCollectionParams(hp.Int64Vec)
	_, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(true))

	_, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, common.DefaultFloatVecFieldName, nil))
	common.CheckErr(t, err, false, "invalid index")
}

// test create index async true
func TestCreateIndexAsync(t *testing.T) {
	t.Log("wait GetIndexState")
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	cp := hp.NewCreateCollectionParams(hp.Int64Vec)
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(true))

	// insert
	ip := hp.NewInsertParams(schema)
	prepare.InsertData(ctx, t, mc, ip, hp.TNewDataOption().TWithSparseMaxLen(100))
	prepare.FlushData(ctx, t, mc, schema.CollectionName)

	// create index
	_, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, common.DefaultFloatVecFieldName, index.NewHNSWIndex(entity.L2, 8, 96)))
	common.CheckErr(t, err, true)

	idx, err := mc.DescribeIndex(ctx, client.NewDescribeIndexOption(schema.CollectionName, common.DefaultFloatVecFieldName))
	common.CheckErr(t, err, true)
	log.Debug("describe index", zap.Any("descIdx", idx))
}

// create same index name on different vector field
func TestIndexMultiVectorDupName(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	cp := hp.NewCreateCollectionParams(hp.Int64MultiVec)
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(true))

	// insert
	ip := hp.NewInsertParams(schema)
	prepare.InsertData(ctx, t, mc, ip, hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)

	// create index with same indexName on different fields
	idx := index.NewHNSWIndex(entity.COSINE, 8, 96)
	idxTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, common.DefaultFloatVecFieldName, idx).WithIndexName("index_1"))
	common.CheckErr(t, err, true)
	err = idxTask.Await(ctx)
	common.CheckErr(t, err, true)

	_, err = mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, common.DefaultFloat16VecFieldName, idx).WithIndexName("index_1"))
	common.CheckErr(t, err, false, "CreateIndex failed: at most one distinct index is allowed per field")

	// create different index on same field
	idxRe := index.NewIvfSQ8Index(entity.COSINE, 32)
	_, errRe := mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, common.DefaultFloatVecFieldName, idxRe).WithIndexName("index_2"))
	common.CheckErr(t, errRe, false, "CreateIndex failed: creating multiple indexes on same field is not supported")
}

func TestDropIndex(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	cp := hp.NewCreateCollectionParams(hp.Int64MultiVec)
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(true))

	// insert
	ip := hp.NewInsertParams(schema)
	prepare.InsertData(ctx, t, mc, ip, hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)

	// create index with indexName
	idxName := "index_1"
	idx := index.NewHNSWIndex(entity.COSINE, 8, 96)
	idxTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, common.DefaultFloatVecFieldName, idx).WithIndexName(idxName))
	common.CheckErr(t, err, true)
	err = idxTask.Await(ctx)
	common.CheckErr(t, err, true)

	// describe index with fieldName -> not found
	_, errNotFound := mc.DescribeIndex(ctx, client.NewDescribeIndexOption(schema.CollectionName, common.DefaultFloatVecFieldName))
	common.CheckErr(t, errNotFound, false, "index not found")

	// describe index with index name -> ok
	descIdx, err := mc.DescribeIndex(ctx, client.NewDescribeIndexOption(schema.CollectionName, idxName))
	common.CheckErr(t, err, true)
	common.CheckIndex(t, descIdx, index.NewGenericIndex(idxName, idx.Params()), common.TNewCheckIndexOpt(common.DefaultNb))

	// drop index with field name
	errDrop := mc.DropIndex(ctx, client.NewDropIndexOption(schema.CollectionName, common.DefaultFloatVecFieldName))
	common.CheckErr(t, errDrop, true)
	descIdx, err = mc.DescribeIndex(ctx, client.NewDescribeIndexOption(schema.CollectionName, idxName))
	common.CheckErr(t, err, true)
	common.CheckIndex(t, descIdx, index.NewGenericIndex(idxName, idx.Params()), common.TNewCheckIndexOpt(common.DefaultNb))

	// drop index with index name
	errDrop = mc.DropIndex(ctx, client.NewDropIndexOption(schema.CollectionName, idxName))
	common.CheckErr(t, errDrop, true)
	_idx, errDescribe := mc.DescribeIndex(ctx, client.NewDescribeIndexOption(schema.CollectionName, idxName))
	common.CheckErr(t, errDescribe, false, "index not found")
	common.CheckIndex(t, _idx, nil, nil)
}

func TestDropIndexCreateIndexWithIndexName(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	cp := hp.NewCreateCollectionParams(hp.Int64MultiVec)
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, cp, hp.TNewFieldsOption(), hp.TNewSchemaOption().TWithEnableDynamicField(true))

	// insert
	ip := hp.NewInsertParams(schema)
	prepare.InsertData(ctx, t, mc, ip, hp.TNewDataOption())
	prepare.FlushData(ctx, t, mc, schema.CollectionName)

	// create index with same indexName on different fields
	// create index: index_1 on vector
	idxName := "index_1"
	idx := index.NewHNSWIndex(entity.COSINE, 8, 96)
	idxTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, common.DefaultFloatVecFieldName, idx).WithIndexName(idxName))
	common.CheckErr(t, err, true)
	err = idxTask.Await(ctx)
	common.CheckErr(t, err, true)
	descIdx, err := mc.DescribeIndex(ctx, client.NewDescribeIndexOption(schema.CollectionName, idxName))
	common.CheckErr(t, err, true)
	common.CheckIndex(t, descIdx, index.NewGenericIndex(idxName, idx.Params()), common.TNewCheckIndexOpt(common.DefaultNb))

	// drop index
	errDrop := mc.DropIndex(ctx, client.NewDropIndexOption(schema.CollectionName, idxName))
	common.CheckErr(t, errDrop, true)
	_idx, errDescribe := mc.DescribeIndex(ctx, client.NewDescribeIndexOption(schema.CollectionName, idxName))
	common.CheckErr(t, errDescribe, false, "index not found")
	common.CheckIndex(t, _idx, nil, common.TNewCheckIndexOpt(0).TWithIndexRows(0, 0, 0).
		TWithIndexState(common.IndexStateIndexStateNone))

	// create new IP index
	ipIdx := index.NewHNSWIndex(entity.IP, 8, 96)
	idxTask, err2 := mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, common.DefaultFloatVecFieldName, ipIdx).WithIndexName(idxName))
	common.CheckErr(t, err2, true)
	err = idxTask.Await(ctx)
	common.CheckErr(t, err, true)
	descIdx2, err2 := mc.DescribeIndex(ctx, client.NewDescribeIndexOption(schema.CollectionName, idxName))
	common.CheckErr(t, err2, true)
	common.CheckIndex(t, descIdx2, index.NewGenericIndex(idxName, ipIdx.Params()), common.TNewCheckIndexOpt(common.DefaultNb))
}

package helper

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/client/v2/entity"
	clientv2 "github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/tests/go_client/base"
	"github.com/milvus-io/milvus/tests/go_client/common"
)

func CreateContext(t *testing.T, timeout time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	t.Cleanup(func() {
		cancel()
	})
	return ctx
}

// var ArrayFieldType =
func GetAllArrayElementType() []entity.FieldType {
	return []entity.FieldType{
		entity.FieldTypeBool,
		entity.FieldTypeInt8,
		entity.FieldTypeInt16,
		entity.FieldTypeInt32,
		entity.FieldTypeInt64,
		entity.FieldTypeFloat,
		entity.FieldTypeDouble,
		entity.FieldTypeVarChar,
	}
}

func GetAllVectorFieldType() []entity.FieldType {
	return []entity.FieldType{
		entity.FieldTypeBinaryVector,
		entity.FieldTypeFloatVector,
		entity.FieldTypeFloat16Vector,
		entity.FieldTypeBFloat16Vector,
		entity.FieldTypeSparseVector,
	}
}

func GetAllScalarFieldType() []entity.FieldType {
	return []entity.FieldType{
		entity.FieldTypeBool,
		entity.FieldTypeInt8,
		entity.FieldTypeInt16,
		entity.FieldTypeInt32,
		entity.FieldTypeInt64,
		entity.FieldTypeFloat,
		entity.FieldTypeDouble,
		entity.FieldTypeVarChar,
		entity.FieldTypeArray,
		entity.FieldTypeJSON,
	}
}

func GetAllFieldsType() []entity.FieldType {
	allFieldType := GetAllScalarFieldType()
	allFieldType = append(allFieldType, entity.FieldTypeBinaryVector,
		entity.FieldTypeFloatVector,
		entity.FieldTypeFloat16Vector,
		entity.FieldTypeBFloat16Vector,
		// entity.FieldTypeSparseVector, max vector fields num is 4
	)
	return allFieldType
}

func GetInvalidPkFieldType() []entity.FieldType {
	nonPkFieldTypes := []entity.FieldType{
		entity.FieldTypeNone,
		entity.FieldTypeBool,
		entity.FieldTypeInt8,
		entity.FieldTypeInt16,
		entity.FieldTypeInt32,
		entity.FieldTypeFloat,
		entity.FieldTypeDouble,
		entity.FieldTypeString,
		entity.FieldTypeJSON,
		entity.FieldTypeArray,
	}
	return nonPkFieldTypes
}

func GetInvalidPartitionKeyFieldType() []entity.FieldType {
	nonPkFieldTypes := []entity.FieldType{
		entity.FieldTypeBool,
		entity.FieldTypeInt8,
		entity.FieldTypeInt16,
		entity.FieldTypeInt32,
		entity.FieldTypeFloat,
		entity.FieldTypeDouble,
		entity.FieldTypeJSON,
		entity.FieldTypeArray,
		entity.FieldTypeFloatVector,
	}
	return nonPkFieldTypes
}

// CollectionPrepare ----------------- prepare data --------------------------
type CollectionPrepare struct{}

var (
	CollPrepare CollectionPrepare
	FieldsFact  FieldsFactory
)

func mergeOptions(schema *entity.Schema, opts ...CreateCollectionOpt) clientv2.CreateCollectionOption {
	//
	collectionOption := clientv2.NewCreateCollectionOption(schema.CollectionName, schema)
	tmpOption := &createCollectionOpt{}
	for _, o := range opts {
		o(tmpOption)
	}

	if !common.IsZeroValue(tmpOption.shardNum) {
		collectionOption.WithShardNum(tmpOption.shardNum)
	}

	if !common.IsZeroValue(tmpOption.enabledDynamicSchema) {
		collectionOption.WithDynamicSchema(tmpOption.enabledDynamicSchema)
	}

	if !common.IsZeroValue(tmpOption.properties) {
		for k, v := range tmpOption.properties {
			collectionOption.WithProperty(k, v)
		}
	}

	if !common.IsZeroValue(tmpOption.consistencyLevel) {
		collectionOption.WithConsistencyLevel(tmpOption.consistencyLevel)
	}

	return collectionOption
}

func (chainTask *CollectionPrepare) CreateCollection(ctx context.Context, t *testing.T, mc *base.MilvusClient,
	cp *CreateCollectionParams, fieldOpt *GenFieldsOption, schemaOpt *GenSchemaOption, opts ...CreateCollectionOpt,
) (*CollectionPrepare, *entity.Schema) {
	fields := FieldsFact.GenFieldsForCollection(cp.CollectionFieldsType, fieldOpt)
	schemaOpt.Fields = fields
	schema := GenSchema(schemaOpt)

	createCollectionOption := mergeOptions(schema, opts...)
	err := mc.CreateCollection(ctx, createCollectionOption)
	common.CheckErr(t, err, true)

	t.Cleanup(func() {
		// The collection will be cleanup after the test
		// But some ctx is setted with timeout for only a part of unittest,
		// which will cause the drop collection failed with timeout.
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second*10)
		defer cancel()

		err := mc.DropCollection(ctx, clientv2.NewDropCollectionOption(schema.CollectionName))
		common.CheckErr(t, err, true)
	})
	return chainTask, schema
}

func (chainTask *CollectionPrepare) InsertData(ctx context.Context, t *testing.T, mc *base.MilvusClient,
	ip *InsertParams, option *GenDataOption,
) (*CollectionPrepare, clientv2.InsertResult) {
	if nil == ip.Schema || ip.Schema.CollectionName == "" {
		log.Fatal("[InsertData] Nil Schema is not expected")
	}
	// print option
	log.Info("GenDataOption", zap.Any("option", option))
	columns, dynamicColumns := GenColumnsBasedSchema(ip.Schema, option)
	insertOpt := clientv2.NewColumnBasedInsertOption(ip.Schema.CollectionName).WithColumns(columns...).WithColumns(dynamicColumns...)
	if ip.PartitionName != "" {
		insertOpt.WithPartition(ip.PartitionName)
	}
	insertRes, err := mc.Insert(ctx, insertOpt)
	common.CheckErr(t, err, true)
	require.Equal(t, option.nb, insertRes.IDs.Len())
	return chainTask, insertRes
}

func (chainTask *CollectionPrepare) FlushData(ctx context.Context, t *testing.T, mc *base.MilvusClient, collName string) *CollectionPrepare {
	flushTask, err := mc.Flush(ctx, clientv2.NewFlushOption(collName))
	common.CheckErr(t, err, true)
	err = flushTask.Await(ctx)
	common.CheckErr(t, err, true)
	return chainTask
}

func (chainTask *CollectionPrepare) CreateIndex(ctx context.Context, t *testing.T, mc *base.MilvusClient, ip *IndexParams) *CollectionPrepare {
	if nil == ip.Schema || ip.Schema.CollectionName == "" {
		log.Fatal("[CreateIndex] Empty collection name is not expected")
	}
	collName := ip.Schema.CollectionName
	mFieldIndex := ip.FieldIndexMap

	for _, field := range ip.Schema.Fields {
		if field.DataType >= 100 {
			if idx, ok := mFieldIndex[field.Name]; ok {
				log.Info("CreateIndex", zap.String("indexName", idx.Name()), zap.Any("indexType", idx.IndexType()), zap.Any("indexParams", idx.Params()))
				createIndexTask, err := mc.CreateIndex(ctx, clientv2.NewCreateIndexOption(collName, field.Name, idx))
				common.CheckErr(t, err, true)
				err = createIndexTask.Await(ctx)
				common.CheckErr(t, err, true)
			} else {
				idx := GetDefaultVectorIndex(field.DataType)
				log.Info("CreateIndex", zap.String("indexName", idx.Name()), zap.Any("indexType", idx.IndexType()), zap.Any("indexParams", idx.Params()))
				createIndexTask, err := mc.CreateIndex(ctx, clientv2.NewCreateIndexOption(collName, field.Name, idx))
				common.CheckErr(t, err, true)
				err = createIndexTask.Await(ctx)
				common.CheckErr(t, err, true)
			}
		}
	}
	return chainTask
}

func (chainTask *CollectionPrepare) Load(ctx context.Context, t *testing.T, mc *base.MilvusClient, lp *LoadParams) *CollectionPrepare {
	if lp.CollectionName == "" {
		log.Fatal("[Load] Empty collection name is not expected")
	}
	loadTask, err := mc.LoadCollection(ctx, clientv2.NewLoadCollectionOption(lp.CollectionName).WithReplica(lp.Replica).WithLoadFields(lp.LoadFields...).WithSkipLoadDynamicField(lp.SkipLoadDynamicField))
	common.CheckErr(t, err, true)
	err = loadTask.Await(ctx)
	common.CheckErr(t, err, true)
	return chainTask
}

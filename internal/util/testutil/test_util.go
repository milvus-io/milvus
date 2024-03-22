package testutil

import (
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/common"
)

const (
	testMaxVarCharLength = 100
)

func ConstructCollectionSchemaWithKeys(collectionName string,
	fieldName2DataType map[string]schemapb.DataType,
	primaryFieldName string,
	partitionKeyFieldName string,
	clusteringKeyFieldName string,
	autoID bool,
	dim int,
) *schemapb.CollectionSchema {
	schema := ConstructCollectionSchemaByDataType(collectionName,
		fieldName2DataType,
		primaryFieldName,
		autoID,
		dim)
	for _, field := range schema.Fields {
		if field.Name == partitionKeyFieldName {
			field.IsPartitionKey = true
		}
		if field.Name == clusteringKeyFieldName {
			field.IsClusteringKey = true
		}
	}

	return schema
}

func isVectorType(dataType schemapb.DataType) bool {
	return dataType == schemapb.DataType_FloatVector ||
		dataType == schemapb.DataType_BinaryVector ||
		dataType == schemapb.DataType_Float16Vector ||
		dataType == schemapb.DataType_BFloat16Vector
}

func ConstructCollectionSchemaByDataType(collectionName string,
	fieldName2DataType map[string]schemapb.DataType,
	primaryFieldName string,
	autoID bool,
	dim int,
) *schemapb.CollectionSchema {
	fieldsSchema := make([]*schemapb.FieldSchema, 0)
	fieldIdx := int64(0)
	for fieldName, dataType := range fieldName2DataType {
		fieldSchema := &schemapb.FieldSchema{
			Name:     fieldName,
			DataType: dataType,
			FieldID:  fieldIdx,
		}
		fieldIdx += 1
		if isVectorType(dataType) {
			fieldSchema.TypeParams = []*commonpb.KeyValuePair{
				{
					Key:   common.DimKey,
					Value: strconv.Itoa(dim),
				},
			}
		}
		if dataType == schemapb.DataType_VarChar {
			fieldSchema.TypeParams = []*commonpb.KeyValuePair{
				{
					Key:   common.MaxLengthKey,
					Value: strconv.Itoa(testMaxVarCharLength),
				},
			}
		}
		if fieldName == primaryFieldName {
			fieldSchema.IsPrimaryKey = true
			fieldSchema.AutoID = autoID
		}

		fieldsSchema = append(fieldsSchema, fieldSchema)
	}

	return &schemapb.CollectionSchema{
		Name:   collectionName,
		Fields: fieldsSchema,
	}
}

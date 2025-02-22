package typeutil

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func preHandleEmptyResult(result RetrieveResults) {
	result.PreHandle()
}

func appendFieldData(result RetrieveResults, fieldData *schemapb.FieldData) {
	result.AppendFieldData(fieldData)
}

func FillRetrieveResultIfEmpty(result RetrieveResults, outputFieldIDs []int64, schema *schemapb.CollectionSchema) error {
	if !result.ResultEmpty() {
		return nil
	}

	preHandleEmptyResult(result)

	helper, err := typeutil.CreateSchemaHelper(schema)
	if err != nil {
		return err
	}
	for _, outputFieldID := range outputFieldIDs {
		field, err := helper.GetFieldFromID(outputFieldID)
		if err != nil {
			return err
		}

		emptyFieldData, err := typeutil.GenEmptyFieldData(field)
		if err != nil {
			return err
		}

		appendFieldData(result, emptyFieldData)
	}

	return nil
}

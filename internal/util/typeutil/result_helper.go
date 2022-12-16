package typeutil

import (
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
)

func preHandleEmptyResult(result RetrieveResults) {
	result.PreHandle()
}

func appendFieldData(result RetrieveResults, fieldData *schemapb.FieldData) {
	result.AppendFieldData(fieldData)
}

func FillRetrieveResultIfEmpty(result RetrieveResults, outputFieldIds []int64, schema *schemapb.CollectionSchema) error {
	if !result.ResultEmpty() {
		return nil
	}

	preHandleEmptyResult(result)

	helper, err := CreateSchemaHelper(schema)
	if err != nil {
		return err
	}
	for _, outputFieldID := range outputFieldIds {
		field, err := helper.GetFieldFromID(outputFieldID)
		if err != nil {
			return err
		}

		emptyFieldData, err := GenEmptyFieldData(field)
		if err != nil {
			return err
		}

		appendFieldData(result, emptyFieldData)
	}

	return nil
}

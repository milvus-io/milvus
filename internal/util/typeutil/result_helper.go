package typeutil

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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
		// System fields (RowID=0, Timestamp=1) are never part of the user/collection
		// schema, but the proxy appends common.TimeStampField to OutputFieldsId for
		// the iterator MVCC dedup. The non-empty retrieve path emits them as Int64
		// columns (synthesized for external collections), so the empty-fill path must
		// do the same instead of failing the schema lookup with "fieldID(1) not found".
		// See https://github.com/milvus-io/milvus/issues/50188.
		if common.IsSystemField(outputFieldID) {
			appendFieldData(result, genEmptySystemFieldData(outputFieldID))
			continue
		}

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

// genEmptySystemFieldData builds an empty Int64 field data for a reserved system
// field (RowID / Timestamp). System fields are stored as Int64 in segcore and are
// not present in the user collection schema, so they cannot be resolved via the
// SchemaHelper.
func genEmptySystemFieldData(fieldID int64) *schemapb.FieldData {
	name := ""
	switch fieldID {
	case common.RowIDField:
		name = common.RowIDFieldName
	case common.TimeStampField:
		name = common.TimeStampFieldName
	}
	field := &schemapb.FieldSchema{
		FieldID:  fieldID,
		Name:     name,
		DataType: schemapb.DataType_Int64,
	}
	// GenEmptyFieldData never returns an error for the Int64 data type.
	emptyFieldData, _ := typeutil.GenEmptyFieldData(field)
	return emptyFieldData
}

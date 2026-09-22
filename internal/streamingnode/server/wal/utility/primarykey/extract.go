// Package primarykey validates the primary key payload of WAL insert and delete
// messages on the append path: a bad payload must be rejected with an error,
// it must never panic and never be accepted leniently.
//
// The permissive counterparts, storage.ParseIDs2PrimaryKeysBatch and
// typeutil.GetPrimaryFieldData, copy the values, match a field by name as well
// and do not validate; they serve trusted data and can not replace this package.
package primarykey

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
)

// KeysOfInsert returns the values of the primary key field fieldID of an insert
// body and checks that the payload matches the schema type dataType.
func KeysOfInsert(body *msgpb.InsertRequest, fieldID int64, dataType schemapb.DataType) (Keys, error) {
	keys, err := KeysOfInsertField(body, fieldID)
	if err != nil {
		return Keys{}, err
	}
	if err := validateScalarType(keys, dataType); err != nil {
		return Keys{}, err
	}
	return keys, nil
}

// KeysOfInsertDeclared is KeysOfInsert plus a check that the type declared on
// the field data equals the schema type. Legacy inserts may leave the declared
// type unset, so requiring it is the caller's choice.
func KeysOfInsertDeclared(body *msgpb.InsertRequest, fieldID int64, dataType schemapb.DataType) (Keys, error) {
	field, err := findField(body.GetFieldsData(), fieldID)
	if err != nil {
		return Keys{}, err
	}
	if field.GetType() != dataType {
		return Keys{}, status.NewUnrecoverableError(
			"primary key field type %s does not match schema type %s",
			field.GetType().String(),
			dataType.String(),
		)
	}
	keys, err := keysOfFieldData(field, fieldID)
	if err != nil {
		return Keys{}, err
	}
	if err := validateScalarType(keys, dataType); err != nil {
		return Keys{}, err
	}
	return keys, nil
}

// KeysOfInsertField returns the values of the field identified by fieldID,
// without any knowledge of the schema type.
func KeysOfInsertField(body *msgpb.InsertRequest, fieldID int64) (Keys, error) {
	field, err := findField(body.GetFieldsData(), fieldID)
	if err != nil {
		return Keys{}, err
	}
	return keysOfFieldData(field, fieldID)
}

// KeysOfDelete returns the primary keys of a delete body.
func KeysOfDelete(body *msgpb.DeleteRequest) (Keys, error) {
	return keysOfIDs(body.GetPrimaryKeys())
}

func findField(fields []*schemapb.FieldData, fieldID int64) (*schemapb.FieldData, error) {
	var matched *schemapb.FieldData
	for _, field := range fields {
		if field == nil || field.GetFieldId() != fieldID {
			continue
		}
		if matched != nil {
			return nil, status.NewUnrecoverableError("insert primary key field %d is duplicated", fieldID)
		}
		matched = field
	}
	if matched == nil {
		return nil, status.NewUnrecoverableError("insert primary key field %d is missing", fieldID)
	}
	return matched, nil
}

func keysOfFieldData(field *schemapb.FieldData, fieldID int64) (Keys, error) {
	switch values := field.GetScalars().GetData().(type) {
	case *schemapb.ScalarField_LongData:
		if values == nil {
			return Keys{}, status.NewUnrecoverableError("int64 primary keys are nil")
		}
		return keysOfIDs(&schemapb.IDs{
			IdField: &schemapb.IDs_IntId{IntId: values.LongData},
		})
	case *schemapb.ScalarField_StringData:
		if values == nil {
			return Keys{}, status.NewUnrecoverableError("varchar primary keys are nil")
		}
		return keysOfIDs(&schemapb.IDs{
			IdField: &schemapb.IDs_StrId{StrId: values.StringData},
		})
	default:
		return Keys{}, status.NewUnrecoverableError("insert primary key field %d must be int64 or varchar", fieldID)
	}
}

// keysOfIDs converts schemapb.IDs into Keys without copying the values.
func keysOfIDs(ids *schemapb.IDs) (Keys, error) {
	if ids == nil {
		return Keys{}, status.NewUnrecoverableError("primary keys are nil")
	}
	switch values := ids.GetIdField().(type) {
	case *schemapb.IDs_IntId:
		if values == nil || values.IntId == nil {
			return Keys{}, status.NewUnrecoverableError("int64 primary keys are nil")
		}
		if len(values.IntId.GetData()) == 0 {
			return Keys{}, status.NewUnrecoverableError("primary keys are empty")
		}
		return Keys{
			Kind:        KindInt64,
			Int64Values: values.IntId.GetData(),
		}, nil
	case *schemapb.IDs_StrId:
		if values == nil || values.StrId == nil {
			return Keys{}, status.NewUnrecoverableError("varchar primary keys are nil")
		}
		if len(values.StrId.GetData()) == 0 {
			return Keys{}, status.NewUnrecoverableError("primary keys are empty")
		}
		return Keys{
			Kind:         KindString,
			StringValues: values.StrId.GetData(),
		}, nil
	default:
		return Keys{}, status.NewUnrecoverableError("unsupported primary key ids type %T", values)
	}
}

func validateScalarType(keys Keys, dataType schemapb.DataType) error {
	var expected Kind
	switch dataType {
	case schemapb.DataType_Int64:
		expected = KindInt64
	case schemapb.DataType_VarChar:
		expected = KindString
	default:
		return status.NewUnrecoverableError("primary key has unsupported data type %s", dataType.String())
	}
	if keys.Kind != expected {
		return status.NewUnrecoverableError("primary key payload does not match schema type %s", dataType.String())
	}
	return nil
}

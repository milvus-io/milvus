package partialupdate

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
)

// TestExtractPKsErrorText pins the error code and text of the primary key
// extraction failures that are reachable from the partial-update entry points.
// The checks live in wal/utility and wal/utility/primarykey; their texts name
// the failure, not the interceptor that ran into it.
func TestExtractPKsErrorText(t *testing.T) {
	varcharDescriptor := &staticPrimaryKeyDescriptorGetter{
		descriptor: shards.PrimaryKeyDescriptor{FieldID: 100, DataType: schemapb.DataType_VarChar},
	}
	fromDelete := func(ids *schemapb.IDs) error {
		_, _, err := extractPKs(newDeleteMessage(ids))
		return err
	}
	fromInsert := func(fields ...*schemapb.FieldData) error {
		_, err := extractPKsFromInsert(newInsertMessage(fields), 100)
		return err
	}
	vectorField := int64PKFieldData(10)
	vectorField.Field = &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{}}

	cases := []struct {
		name string
		err  error
		text string
	}{
		{"delete corrupt body", func() error {
			_, _, err := extractPKs(corruptMessageBody(newDeleteMessage(&schemapb.IDs{})))
			return err
		}(), "decode delete body failed"},
		{"delete nil ids", fromDelete(nil), "primary keys are nil"},
		{"delete unset id field", fromDelete(&schemapb.IDs{}), "unsupported primary key ids type <nil>"},
		{"delete empty int64", fromDelete(&schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{}}}), "primary keys are empty"},
		{"insert nil message", func() error {
			_, err := extractPKsFromInsert(nil, 100)
			return err
		}(), "insert primary key field id is invalid"},
		{"insert wrong message type", func() error {
			_, err := extractPKsFromInsert(newDeleteMessage(&schemapb.IDs{}), 100)
			return err
		}(), "decode insert message failed"},
		{"insert corrupt body", func() error {
			_, err := extractPKsFromInsert(corruptMessageBody(newInsertMessage(nil)), 100)
			return err
		}(), "decode insert body failed"},
		{"insert missing field", fromInsert(), "insert primary key field 100 is missing"},
		{"insert duplicated field", fromInsert(int64PKFieldData(1), int64PKFieldData(2)), "insert primary key field 100 is duplicated"},
		{"insert unsupported field", fromInsert(vectorField), "insert primary key field 100 must be int64 or varchar"},
		{"cas field type mismatch", func() error {
			_, _, err := extractPKsFromCASInsert(newInsertMessage([]*schemapb.FieldData{int64PKFieldData(1)}), varcharDescriptor)
			return err
		}(), "primary key field type Int64 does not match schema type VarChar"},
		{"ordinary payload type mismatch", func() error {
			field := int64PKFieldData(1)
			field.Type = schemapb.DataType_VarChar
			_, _, err := extractPKsFromOrdinaryInsert(newInsertMessage([]*schemapb.FieldData{field}), varcharDescriptor)
			return err
		}(), "primary key payload does not match schema type VarChar"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			requireUnrecoverable(t, c.err)
			require.Contains(t, c.err.Error(), c.text)
		})
	}
}

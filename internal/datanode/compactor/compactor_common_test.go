package compactor

import (
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestCompactionSegmentBinlogFieldsUsesChildFields(t *testing.T) {
	fields := compactionSegmentBinlogFields(&datapb.CompactionSegmentBinlogs{
		FieldBinlogs: []*datapb.FieldBinlog{
			{FieldID: 900, ChildFields: []int64{102, 103}},
			{FieldID: 104},
		},
	})

	require.Contains(t, fields, int64(102))
	require.Contains(t, fields, int64(103))
	require.Contains(t, fields, int64(104))
	require.NotContains(t, fields, int64(900))
}

func TestFilterV1CompactionFieldBinlogs(t *testing.T) {
	fieldBinlogs := []*datapb.FieldBinlog{
		nil,
		{FieldID: 102},
		{FieldID: 200},
	}

	filtered := filterV1CompactionFieldBinlogs(fieldBinlogs, map[int64]struct{}{102: {}})
	require.Len(t, filtered, 1)
	require.EqualValues(t, 102, filtered[0].GetFieldID())
}

func TestCompactionReadSchemaKeepsAbsentOrdinaryDropsMissingFunctionOutputs(t *testing.T) {
	// Absent ordinary fields (scalars and struct children) stay in the read
	// schema — the reader layer fills them. Only function outputs missing from
	// storage are dropped: they are computed by the RecordMaterializer.
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "missing", DataType: schemapb.DataType_Int64},
			{FieldID: 102, Name: "sparse_missing", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
			{FieldID: 103, Name: "sparse_present", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
		},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{
			{
				FieldID: 200,
				Name:    "struct_with_child",
				Fields: []*schemapb.FieldSchema{
					{FieldID: 201, Name: "child_present", DataType: schemapb.DataType_Int64},
					{FieldID: 202, Name: "child_missing", DataType: schemapb.DataType_Int64},
				},
			},
			{
				FieldID: 300,
				Name:    "struct_without_child",
				Fields: []*schemapb.FieldSchema{
					{FieldID: 301, Name: "child_missing", DataType: schemapb.DataType_Int64},
				},
			},
		},
		Functions: []*schemapb.FunctionSchema{{
			Name: "f", OutputFieldIds: []int64{102, 103},
		}},
	}

	fieldIDs := func(fields []*schemapb.FieldSchema) []int64 {
		ids := make([]int64, 0, len(fields))
		for _, field := range fields {
			ids = append(ids, field.GetFieldID())
		}
		return ids
	}

	readSchema := compactionReadSchema(schema, map[int64]struct{}{100: {}, 201: {}, 103: {}})
	require.NotNil(t, readSchema)
	require.ElementsMatch(t, []int64{100, 101, 103}, fieldIDs(readSchema.GetFields()))
	require.Len(t, readSchema.GetStructArrayFields(), 2)
	require.ElementsMatch(t, []int64{201, 202}, fieldIDs(readSchema.GetStructArrayFields()[0].GetFields()))
	require.ElementsMatch(t, []int64{301}, fieldIDs(readSchema.GetStructArrayFields()[1].GetFields()))
}

func TestCompactionReadSchemaNilSchema(t *testing.T) {
	require.Nil(t, compactionReadSchema(nil, map[int64]struct{}{}))
}

func TestValidateSchemaBumpIntegrity(t *testing.T) {
	base := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "input", DataType: schemapb.DataType_VarChar},
			{FieldID: 101, Name: "output", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
		},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{{
			FieldID: 200,
			Name:    "struct",
			Fields: []*schemapb.FieldSchema{
				{FieldID: 201, Name: "child_a", DataType: schemapb.DataType_Int64},
				{FieldID: 202, Name: "child_b", DataType: schemapb.DataType_Int64},
			},
		}},
		Functions: []*schemapb.FunctionSchema{{
			Name: "f", InputFieldIds: []int64{100}, OutputFieldIds: []int64{101},
		}},
	}

	tests := []struct {
		name           string
		mutate         func(*schemapb.CollectionSchema)
		existingFields map[int64]struct{}
		wantErr        string
	}{
		{name: "valid", existingFields: map[int64]struct{}{201: {}, 202: {}}},
		{name: "whole struct absent", existingFields: map[int64]struct{}{}},
		{
			name: "output declared by multiple functions",
			mutate: func(schema *schemapb.CollectionSchema) {
				schema.Functions = append(schema.Functions, &schemapb.FunctionSchema{
					Name: "second", OutputFieldIds: []int64{101},
				})
			},
			wantErr: "field 101 is declared by both function f and function second",
		},
		{
			name: "output duplicated within one function",
			mutate: func(schema *schemapb.CollectionSchema) {
				schema.Functions[0].OutputFieldIds = []int64{101, 101}
			},
			wantErr: "function f declares output field 101 more than once",
		},
		{
			name: "declared output missing marker",
			mutate: func(schema *schemapb.CollectionSchema) {
				schema.Fields[1].IsFunctionOutput = false
			},
			wantErr: "is not marked as a function output",
		},
		{
			name: "marked output missing producer",
			mutate: func(schema *schemapb.CollectionSchema) {
				schema.Functions = nil
			},
			wantErr: "has no producing function",
		},
		{
			name: "declared output missing field",
			mutate: func(schema *schemapb.CollectionSchema) {
				schema.Fields[1].IsFunctionOutput = false
				schema.Functions[0].OutputFieldIds = []int64{999}
			},
			wantErr: "output field 999 not found in persisted schema",
		},
		{
			name: "struct child marked as function output",
			mutate: func(schema *schemapb.CollectionSchema) {
				schema.StructArrayFields[0].Fields[0].IsFunctionOutput = true
			},
			wantErr: "child field 201 cannot be a function output",
		},
		{
			name: "function targets struct child",
			mutate: func(schema *schemapb.CollectionSchema) {
				schema.Fields[1].IsFunctionOutput = false
				schema.StructArrayFields[0].Fields[0].IsFunctionOutput = true
				schema.Functions[0].OutputFieldIds = []int64{201}
			},
			wantErr: "output field 201 must be a top-level field",
		},
		{
			name:           "struct partially present",
			existingFields: map[int64]struct{}{201: {}},
			wantErr:        "partially present: 1 of 2 children exist",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			schema := proto.Clone(base).(*schemapb.CollectionSchema)
			if test.mutate != nil {
				test.mutate(schema)
			}
			outputs, err := validateSchemaBumpIntegrity(schema, test.existingFields)
			if test.wantErr == "" {
				require.NoError(t, err)
				require.Contains(t, outputs, int64(101))
				return
			}
			require.ErrorIs(t, err, merr.ErrDataIntegrity)
			require.ErrorContains(t, err, test.wantErr)
		})
	}
}

func TestDroppedSchemaFieldIDs(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "text", DataType: schemapb.DataType_VarChar},
			{FieldID: 101, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector},
		},
	}
	droppedUserField := int64(common.StartOfUserFieldID + 1000)
	systemField := int64(common.StartOfUserFieldID - 1)
	existingFields := map[int64]struct{}{
		100:              {},
		droppedUserField: {},
		systemField:      {},
	}

	dropped := droppedSchemaFieldIDs(schema, existingFields)
	require.Equal(t, []int64{droppedUserField}, dropped)
}

func removeFieldBinlogForTest(kvs map[string][]byte, fieldBinlogs map[int64]*datapb.FieldBinlog, fieldID int64) {
	for _, binlog := range fieldBinlogs[fieldID].GetBinlogs() {
		delete(kvs, binlog.GetLogPath())
	}
	delete(fieldBinlogs, fieldID)
}

func downloadValuesForPathsForTest(kvs map[string][]byte, paths []string) ([][]byte, error) {
	values := make([][]byte, 0, len(paths))
	for _, path := range paths {
		value, ok := kvs[path]
		if !ok {
			return nil, errors.Newf("unexpected download path %s", path)
		}
		values = append(values, value)
	}
	return values, nil
}

func TestFieldBinlogEntriesForTestUsesChildFields(t *testing.T) {
	fieldBinlogs := []*datapb.FieldBinlog{
		{FieldID: 0, ChildFields: []int64{101, 107}, Binlogs: []*datapb.Binlog{{EntriesNum: 3}}},
		{FieldID: 108, Binlogs: []*datapb.Binlog{{EntriesNum: 5}}},
	}

	require.EqualValues(t, 3, fieldBinlogEntriesForTest(fieldBinlogs, 107))
	require.EqualValues(t, 5, fieldBinlogEntriesForTest(fieldBinlogs, 108))
	require.EqualValues(t, 0, fieldBinlogEntriesForTest(fieldBinlogs, 109))
}

func fieldBinlogEntriesForTest(fieldBinlogs []*datapb.FieldBinlog, fieldID int64) int64 {
	var entries int64
	for _, fieldBinlog := range fieldBinlogs {
		matchesField := fieldBinlog.GetFieldID() == fieldID
		for _, childFieldID := range fieldBinlog.GetChildFields() {
			if childFieldID == fieldID {
				matchesField = true
				break
			}
		}
		if !matchesField {
			continue
		}
		for _, binlog := range fieldBinlog.GetBinlogs() {
			entries += binlog.GetEntriesNum()
		}
	}
	return entries
}

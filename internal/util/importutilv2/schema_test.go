// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package importutilv2

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestValidateImportSchema(t *testing.T) {
	nested := func(name string) *schemapb.FieldSchema {
		leaf := &schemapb.TypeSchema{Kind: &schemapb.TypeSchema_LeafType{LeafType: schemapb.DataType_Int64}}
		inner := &schemapb.TypeSchema{Kind: &schemapb.TypeSchema_ArrayElement{ArrayElement: leaf}}
		root := &schemapb.TypeSchema{Kind: &schemapb.TypeSchema_ArrayElement{ArrayElement: inner}}
		return &schemapb.FieldSchema{
			Name: name, DataType: schemapb.DataType_Array,
			ElementType: schemapb.DataType_Array, TypeSchema: root,
		}
	}
	fields := []struct {
		name       string
		field      *schemapb.FieldSchema
		inStruct   bool
		nativeList bool
	}{
		{"legacy scalar array", &schemapb.FieldSchema{Name: "scalar", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64}, false, false},
		{"legacy vector array", &schemapb.FieldSchema{Name: "vector", DataType: schemapb.DataType_ArrayOfVector, ElementType: schemapb.DataType_FloatVector}, false, false},
		{"element-nullable scalar array", &schemapb.FieldSchema{Name: "scalar", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64, ElementNullable: true}, false, true},
		{"element-nullable vector array", &schemapb.FieldSchema{Name: "vector", DataType: schemapb.DataType_ArrayOfVector, ElementType: schemapb.DataType_FloatVector, ElementNullable: true}, false, true},
		{"nested scalar array", nested("nested"), false, true},
		{"legacy struct sub-field", &schemapb.FieldSchema{Name: "scalar", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64}, true, false},
		{"legacy struct vector sub-field", &schemapb.FieldSchema{Name: "vector", DataType: schemapb.DataType_ArrayOfVector, ElementType: schemapb.DataType_FloatVector}, true, false},
		{"element-nullable struct sub-field", &schemapb.FieldSchema{Name: "scalar", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64, ElementNullable: true}, true, true},
		{"normalized element-nullable struct sub-field", &schemapb.FieldSchema{Name: "s[scalar]", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64, ElementNullable: true}, true, true},
		{"element-nullable struct vector sub-field", &schemapb.FieldSchema{Name: "vector", DataType: schemapb.DataType_ArrayOfVector, ElementType: schemapb.DataType_FloatVector, ElementNullable: true}, true, true},
		{"nested struct sub-field", nested("nested"), true, true},
	}
	kv := func(key, value string) *commonpb.KeyValuePair {
		return &commonpb.KeyValuePair{Key: key, Value: value}
	}
	imports := []struct {
		name    string
		options Options
		// kind is the import kind named in the rejection; empty means the
		// import never rejects a native-list target.
		kind string
	}{
		{"file import", nil, ""},
		{"backup disabled", Options{kv(BackupFlag, "false")}, ""},
		{"l0 import", Options{kv(L0Import, "true")}, ""},
		{"backup import", Options{kv(BackupFlag, "true")}, "binlog import (backup restore)"},
		{"backup import upper case", Options{kv(BackupFlag, "TRUE")}, "binlog import (backup restore)"},
		{"snapshot-source import", Options{kv(BackupFlag, "true"), kv(SourceType, SourceTypeSnapshot)}, "snapshot-source import"},
		// The gate runs before ValidateSnapshotSourceOptions, so a snapshot
		// source without the backup flag must be rejected here too.
		{"snapshot-source without backup flag", Options{kv(SourceType, SourceTypeSnapshot)}, "snapshot-source import"},
	}
	for _, imp := range imports {
		for _, tc := range fields {
			t.Run(imp.name+"/"+tc.name, func(t *testing.T) {
				schema := &schemapb.CollectionSchema{}
				fieldName := tc.field.GetName()
				if tc.inStruct {
					schema.StructArrayFields = []*schemapb.StructArrayFieldSchema{{Name: "s", Fields: []*schemapb.FieldSchema{tc.field}}}
					name, err := typeutil.ExtractStructFieldName(fieldName)
					require.NoError(t, err)
					fieldName = "s." + name
				} else {
					schema.Fields = []*schemapb.FieldSchema{tc.field}
				}
				err := ValidateImportSchema(schema, imp.options)
				if imp.kind == "" || !tc.nativeList {
					require.NoError(t, err)
					return
				}
				require.ErrorIs(t, err, merr.ErrParameterInvalid)
				require.Contains(t, err.Error(), imp.kind+" into a collection with element-nullable or nested Array field "+fieldName+" is not supported")
			})
		}
	}
}

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

package dml

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func newDynamicFieldValidationSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		EnableDynamicField: true,
		Fields: []*schemapb.FieldSchema{
			{FieldID: common.RowIDField, Name: "RowID", DataType: schemapb.DataType_Int64},
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "Int64Field", DataType: schemapb.DataType_Int64},
			{FieldID: 102, Name: "$meta", DataType: schemapb.DataType_JSON, IsDynamic: true},
		},
	}
}

func newDynamicFieldValidationMsg(schema *schemapb.CollectionSchema, rows [][]byte) *msgstream.InsertMsg {
	return &msgstream.InsertMsg{
		InsertRequest: &msgpb.InsertRequest{
			FieldsData: []*schemapb.FieldData{autoGenDynamicFieldData(schema, rows)},
			NumRows:    uint64(len(rows)),
		},
	}
}

func TestVerifyDynamicFieldDataJSONCompatibility(t *testing.T) {
	schema := newDynamicFieldValidationSchema()

	tests := []struct {
		name                     string
		row                      string
		skipStaticFieldNameCheck bool
		wantErr                  bool
		contains                 string
	}{
		{name: "empty object", row: `{}`},
		{name: "system field name is allowed", row: `{"RowID":1}`},
		{name: "escaped system field name is allowed", row: `{"\u0052owID":1}`},
		{name: "invalid value takes precedence over static conflict", row: `{"Int64Field":1,"bad":1e10000}`, wantErr: true, contains: "only json map is supported"},
		{name: "empty object with whitespace", row: " { \n\t } "},
		{name: "null remains accepted", row: `null`},
		{name: "nested reserved name is allowed", row: `{"nested":{"$meta":1}}`},
		{name: "non conflicting object", row: `{"color":"blue","size":42}`},
		{name: "array is rejected", row: `[]`, wantErr: true, contains: "only json map is supported"},
		{name: "string is rejected", row: `"value"`, wantErr: true, contains: "only json map is supported"},
		{name: "number is rejected", row: `1`, wantErr: true, contains: "only json map is supported"},
		{name: "boolean is rejected", row: `true`, wantErr: true, contains: "only json map is supported"},
		{name: "malformed json is rejected", row: `{invalid`, wantErr: true, contains: "only json map is supported"},
		{name: "trailing content is rejected", row: `{"color":"blue"} trailing`, wantErr: true, contains: "only json map is supported"},
		{name: "number overflow is rejected", row: `{"number":1e10000}`, wantErr: true, contains: "only json map is supported"},
		{name: "number underflow remains accepted", row: `{"number":1e-10000}`},
		{name: "nested number overflow is rejected", row: `{"nested":{"number":1e10000}}`, wantErr: true, contains: "only json map is supported"},
		{name: "array number overflow is rejected", row: `{"numbers":[1,1e10000]}`, wantErr: true, contains: "only json map is supported"},
		{name: "invalid utf8 key follows legacy behavior", row: "{\"\xff\":1}"},
		{name: "reserved name is rejected", row: `{"$meta":1}`, wantErr: true, contains: "$meta"},
		{name: "escaped reserved name is rejected", row: `{"\u0024meta":1}`, wantErr: true, contains: "$meta"},
		{name: "static name is rejected", row: `{"Int64Field":1}`, wantErr: true, contains: "Int64Field"},
		{name: "escaped static name is rejected", row: `{"\u0049nt64Field":1}`, wantErr: true, contains: "Int64Field"},
		{name: "duplicate reserved name is rejected", row: `{"color":"blue","$meta":1,"$meta":2}`, wantErr: true, contains: "$meta"},
		{name: "partial update allows static name", row: `{"Int64Field":1}`, skipStaticFieldNameCheck: true},
		{name: "partial update still rejects reserved name", row: `{"$meta":1}`, skipStaticFieldNameCheck: true, wantErr: true, contains: "$meta"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			legacyErr := verifyDynamicFieldDataWithUnmarshal(schema, []byte(test.row), test.skipStaticFieldNameCheck)
			require.Equal(t, test.wantErr, legacyErr != nil)

			msg := newDynamicFieldValidationMsg(schema, [][]byte{[]byte(test.row)})
			err := doCheckDynamicFieldData(schema, msg, test.skipStaticFieldNameCheck)
			if legacyErr == nil {
				require.NoError(t, err)
				return
			}

			require.Equal(t, merr.Status(legacyErr), merr.Status(err))
			if test.contains != "" {
				assert.Contains(t, err.Error(), test.contains)
			}
		})
	}

	t.Run("dynamic schema disabled", func(t *testing.T) {
		disabledSchema := newDynamicFieldValidationSchema()
		disabledSchema.EnableDynamicField = false
		msg := newDynamicFieldValidationMsg(disabledSchema, [][]byte{[]byte(`{}`)})
		err := verifyDynamicFieldData(disabledSchema, msg, false)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "without dynamic schema enabled")
	})

	t.Run("static conflict follows schema order", func(t *testing.T) {
		orderedSchema := &schemapb.CollectionSchema{
			EnableDynamicField: true,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "fieldA", DataType: schemapb.DataType_Int64},
				{FieldID: 101, Name: "fieldB", DataType: schemapb.DataType_Int64},
				{FieldID: 102, Name: "$meta", DataType: schemapb.DataType_JSON, IsDynamic: true},
			},
		}
		msg := newDynamicFieldValidationMsg(orderedSchema, [][]byte{[]byte(`{"fieldB":1,"fieldA":2}`)})
		err := verifyDynamicFieldData(orderedSchema, msg, false)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "fieldA")
	})
}

func BenchmarkVerifyDynamicFieldData(b *testing.B) {
	schema := newDynamicFieldValidationSchema()

	benchmarks := []struct {
		name    string
		payload []byte
	}{
		{name: "empty", payload: []byte(`{}`)},
		{name: "nested", payload: []byte(`{"color":"blue","score":1.25,"nested":{"items":[1,2,3],"config":{"retries":3}}}`)},
	}

	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, func(b *testing.B) {
			rows := make([][]byte, 1000)
			for i := range rows {
				rows[i] = benchmark.payload
			}
			msg := newDynamicFieldValidationMsg(schema, rows)
			b.ReportAllocs()
			b.SetBytes(int64(len(benchmark.payload) * len(rows)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := verifyDynamicFieldData(schema, msg, false); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

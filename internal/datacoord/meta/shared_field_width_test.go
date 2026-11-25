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

package meta

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

func TestFieldSizePerRecordSharedWidth(t *testing.T) {
	for _, tc := range []struct {
		name         string
		field        *schemapb.FieldSchema
		width        int64
		exact, fixed bool
	}{
		{"integer", &schemapb.FieldSchema{DataType: schemapb.DataType_Int64}, 8, true, true},
		{"nullable", &schemapb.FieldSchema{DataType: schemapb.DataType_Int64, Nullable: true}, 8, false, true},
		{"bool", &schemapb.FieldSchema{DataType: schemapb.DataType_Bool}, 1, false, false},
		{"vector", &schemapb.FieldSchema{DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "128"}}}, 512, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.field.FieldID = 100
			estimate, err := fieldSizePerRecord(&schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{tc.field}}, 100)
			require.NoError(t, err)
			require.Equal(t, tc.width, estimate.sizePerRecord)
			require.Equal(t, tc.exact, estimate.exact)
			require.Equal(t, tc.fixed, estimate.fixedWidth)
		})
	}
	_, err := fieldSizePerRecord(&schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100, DataType: schemapb.DataType_FloatVector}}}, 100)
	require.Error(t, err)
	_, err = fieldSizePerRecord(&schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100, DataType: schemapb.DataType_None}}}, 100)
	require.Error(t, err)
	_, err = fieldSizePerRecord(&schemapb.CollectionSchema{}, 100)
	require.Error(t, err)
}

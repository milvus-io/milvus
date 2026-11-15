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

package typeutil

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestIsBlobType(t *testing.T) {
	assert.True(t, IsBlobType(schemapb.DataType_Blob))
	assert.False(t, IsBlobType(schemapb.DataType_VarChar))
	assert.False(t, IsBlobType(schemapb.DataType_JSON))
	assert.False(t, IsStringType(schemapb.DataType_Blob))
	assert.False(t, IsPrimitiveType(schemapb.DataType_Blob))
	assert.False(t, IsClusteringKeyType(schemapb.DataType_Blob))
	assert.False(t, IsPrimaryFieldType(schemapb.DataType_Blob))
	assert.False(t, isExternalFieldTypeSupported(schemapb.DataType_Blob))
}

func TestRejectBlobWrite(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		},
	}
	assert.NoError(t, RejectBlobWrite(schema))

	schema.Fields = append(schema.Fields, &schemapb.FieldSchema{FieldID: 101, Name: "blob", DataType: schemapb.DataType_Blob})
	assert.ErrorIs(t, RejectBlobWrite(schema), merr.ErrParameterInvalid)
}

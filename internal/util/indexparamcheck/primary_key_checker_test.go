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

package indexparamcheck

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
)

func TestPrimaryKeyChecker(t *testing.T) {
	c := newPrimaryKeyChecker()
	pk := &schemapb.FieldSchema{FieldID: 100, IsPrimaryKey: true, DataType: schemapb.DataType_Int64}
	assert.NoError(t, c.CheckValidDataType(IndexPrimaryKey, pk))
	assert.Error(t, c.CheckValidDataType(IndexPrimaryKey, &schemapb.FieldSchema{FieldID: 101, DataType: schemapb.DataType_Int64}))
	assert.Error(t, c.CheckValidDataType(IndexPrimaryKey, &schemapb.FieldSchema{FieldID: 100, IsPrimaryKey: true, AutoID: true, DataType: schemapb.DataType_Int64}))
	vc := &schemapb.FieldSchema{FieldID: 100, IsPrimaryKey: true, DataType: schemapb.DataType_VarChar}
	assert.NoError(t, c.CheckValidDataType(IndexPrimaryKey, vc))
	assert.False(t, IsBuiltPerSegment(IndexPrimaryKey))
	assert.True(t, IsBuiltPerSegment(IndexINVERTED))
	assert.Error(t, ValidateMmapIndexParams(IndexPrimaryKey, map[string]string{common.MmapEnabledKey: "true"}))

	assert.NoError(t, c.CheckTrain(schemapb.DataType_Int64, schemapb.DataType_None, map[string]string{common.IndexTypeKey: IndexPrimaryKey}))
	assert.Error(t, c.CheckTrain(schemapb.DataType_Int64, schemapb.DataType_None, map[string]string{common.IndexTypeKey: IndexPrimaryKey, "foo": "bar"}))

	checker, err := GetIndexCheckerMgrInstance().GetChecker(IndexPrimaryKey)
	assert.NoError(t, err)
	assert.NotNil(t, checker)
	assert.False(t, IsScalarIndexType(IndexPrimaryKey))
}

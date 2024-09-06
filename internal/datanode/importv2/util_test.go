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

package importv2

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/util/testutil"
	"github.com/milvus-io/milvus/pkg/common"
)

func Test_AppendSystemFieldsData(t *testing.T) {
	const count = 100

	pkField := &schemapb.FieldSchema{
		FieldID:      100,
		Name:         "pk",
		IsPrimaryKey: true,
		AutoID:       true,
	}
	vecField := &schemapb.FieldSchema{
		FieldID:  101,
		Name:     "vec",
		DataType: schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   common.DimKey,
				Value: "4",
			},
		},
	}
	int64Field := &schemapb.FieldSchema{
		FieldID:  102,
		Name:     "int64",
		DataType: schemapb.DataType_Int64,
	}

	schema := &schemapb.CollectionSchema{}
	task := &ImportTask{
		req: &datapb.ImportRequest{
			Ts:     1000,
			Schema: schema,
		},
		allocator: allocator.NewLocalAllocator(0, count*2),
	}

	pkField.DataType = schemapb.DataType_Int64
	schema.Fields = []*schemapb.FieldSchema{pkField, vecField, int64Field}
	insertData, err := testutil.CreateInsertData(schema, count)
	assert.NoError(t, err)
	assert.Equal(t, 0, insertData.Data[pkField.GetFieldID()].RowNum())
	assert.Nil(t, insertData.Data[common.RowIDField])
	assert.Nil(t, insertData.Data[common.TimeStampField])
	err = AppendSystemFieldsData(task, insertData)
	assert.NoError(t, err)
	assert.Equal(t, count, insertData.Data[pkField.GetFieldID()].RowNum())
	assert.Equal(t, count, insertData.Data[common.RowIDField].RowNum())
	assert.Equal(t, count, insertData.Data[common.TimeStampField].RowNum())

	pkField.DataType = schemapb.DataType_VarChar
	schema.Fields = []*schemapb.FieldSchema{pkField, vecField, int64Field}
	insertData, err = testutil.CreateInsertData(schema, count)
	assert.NoError(t, err)
	assert.Equal(t, 0, insertData.Data[pkField.GetFieldID()].RowNum())
	assert.Nil(t, insertData.Data[common.RowIDField])
	assert.Nil(t, insertData.Data[common.TimeStampField])
	err = AppendSystemFieldsData(task, insertData)
	assert.NoError(t, err)
	assert.Equal(t, count, insertData.Data[pkField.GetFieldID()].RowNum())
	assert.Equal(t, count, insertData.Data[common.RowIDField].RowNum())
	assert.Equal(t, count, insertData.Data[common.TimeStampField].RowNum())
}

func Test_UnsetAutoID(t *testing.T) {
	pkField := &schemapb.FieldSchema{
		FieldID:      100,
		Name:         "pk",
		DataType:     schemapb.DataType_Int64,
		IsPrimaryKey: true,
		AutoID:       true,
	}
	vecField := &schemapb.FieldSchema{
		FieldID:  101,
		Name:     "vec",
		DataType: schemapb.DataType_FloatVector,
	}

	schema := &schemapb.CollectionSchema{}
	schema.Fields = []*schemapb.FieldSchema{pkField, vecField}
	UnsetAutoID(schema)
	for _, field := range schema.GetFields() {
		if field.GetIsPrimaryKey() {
			assert.False(t, schema.GetFields()[0].GetAutoID())
		}
	}
}

func Test_PickSegment(t *testing.T) {
	const (
		vchannel    = "ch-0"
		partitionID = 10
	)
	task := &ImportTask{
		req: &datapb.ImportRequest{
			RequestSegments: []*datapb.ImportRequestSegment{
				{
					SegmentID:   100,
					PartitionID: partitionID,
					Vchannel:    vchannel,
				},
				{
					SegmentID:   101,
					PartitionID: partitionID,
					Vchannel:    vchannel,
				},
				{
					SegmentID:   102,
					PartitionID: partitionID,
					Vchannel:    vchannel,
				},
				{
					SegmentID:   103,
					PartitionID: partitionID,
					Vchannel:    vchannel,
				},
			},
		},
	}

	importedSize := map[int64]int{}

	totalSize := 8 * 1024 * 1024 * 1024
	batchSize := 1 * 1024 * 1024

	for totalSize > 0 {
		picked, err := PickSegment(task.req.GetRequestSegments(), vchannel, partitionID)
		assert.NoError(t, err)
		importedSize[picked] += batchSize
		totalSize -= batchSize
	}
	expectSize := 2 * 1024 * 1024 * 1024
	fn := func(actual int) {
		t.Logf("actual=%d, expect*0.8=%f, expect*1.2=%f", actual, float64(expectSize)*0.9, float64(expectSize)*1.1)
		assert.True(t, float64(actual) > float64(expectSize)*0.8)
		assert.True(t, float64(actual) < float64(expectSize)*1.2)
	}
	fn(importedSize[int64(100)])
	fn(importedSize[int64(101)])
	fn(importedSize[int64(102)])
	fn(importedSize[int64(103)])

	// test no candidate segments found
	_, err := PickSegment(task.req.GetRequestSegments(), "ch-2", 20)
	assert.Error(t, err)
}

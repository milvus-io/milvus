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

package datacoord

import (
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/common"
)

func TestRegroupImportFiles(t *testing.T) {
	pk := &schemapb.FieldSchema{
		FieldID:      100,
		Name:         "pk",
		IsPrimaryKey: true,
		DataType:     schemapb.DataType_VarChar,
		TypeParams:   []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "1024"}},
		AutoID:       false,
	}
	json := &schemapb.FieldSchema{
		FieldID:    101,
		Name:       "json",
		DataType:   schemapb.DataType_JSON,
		TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "1024"}},
	}
	desc := &schemapb.FieldSchema{
		FieldID:    102,
		Name:       "desc",
		DataType:   schemapb.DataType_VarChar,
		TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "1024"}},
	}
	vec := &schemapb.FieldSchema{
		FieldID:      103,
		Name:         "vec",
		IsPrimaryKey: false,
		Description:  "",
		DataType:     schemapb.DataType_FloatVector,
		TypeParams:   []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "512"}},
		IndexParams:  nil,
		AutoID:       false,
	}

	schema := &schemapb.CollectionSchema{
		Name: "test_import_util",
		Fields: []*schemapb.FieldSchema{
			pk, json, desc, vec,
		},
	}

	task := &preImportTask{
		PreImportTask: &datapb.PreImportTask{
			JobID:        0,
			TaskID:       100,
			CollectionID: 1,
			PartitionIDs: []int64{2},
			Vchannels:    []string{"ch-0"},
			FileStats: []*datapb.ImportFileStats{
				{
					ImportFile: &internalpb.ImportFile{Paths: []string{"a.json"}},
					TotalRows:  20000,
				},
				{
					ImportFile: &internalpb.ImportFile{Paths: []string{"b.json"}},
					TotalRows:  20000,
				},
			},
		},
		schema: schema,
	}

	tasks := make([]ImportTask, 0)
	for i := 0; i < 10; i++ {
		tasks = append(tasks, task.Clone())
		task.TaskID++
	}

	groups, err := RegroupImportFiles(tasks)
	assert.NoError(t, err)
	t.Logf("file groups: %v", groups)
	assert.Equal(t, 20, lo.SumBy(groups, func(group []*datapb.ImportFileStats) int {
		return len(group)
	}))
	for _, group := range groups {
		segmentMaxRows, err := calBySchemaPolicy(schema)
		assert.NoError(t, err)
		fileRowsSum := lo.SumBy(group, func(stat *datapb.ImportFileStats) int64 {
			return stat.GetTotalRows()
		})
		assert.True(t, fileRowsSum <= int64(segmentMaxRows))
	}
}

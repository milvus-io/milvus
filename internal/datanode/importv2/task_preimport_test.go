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
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// deleteKeyRow is a delete-key file row: the primary key plus an extra column
// that a delete-key file's projected schema does not declare.
type deleteKeyRow struct {
	PK    int64  `json:"pk"`
	Extra string `json:"extra"`
}

type deleteKeyContent struct {
	Rows []deleteKeyRow `json:"rows,omitempty"`
}

func TestPreImportTask_DeleteMode(t *testing.T) {
	paramtable.Init()

	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "pk",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:  101,
				Name:     "vec",
				DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.DimKey, Value: "4"},
				},
			},
		},
	}

	content := &deleteKeyContent{
		Rows: []deleteKeyRow{
			{PK: 1, Extra: "ignored-0"},
			{PK: 2, Extra: "ignored-1"},
			{PK: 3, Extra: "ignored-2"},
		},
	}
	bytes, err := json.Marshal(content)
	require.NoError(t, err)

	cm := mocks.NewChunkManager(t)
	ioReader := strings.NewReader(string(bytes))
	cm.EXPECT().Size(mock.Anything, mock.Anything).Return(1024, nil)
	cm.EXPECT().Reader(mock.Anything, mock.Anything).Return(&mockReader{Reader: ioReader, Closer: io.NopCloser(ioReader)}, nil)

	manager := NewTaskManager()
	req := &datapb.PreImportRequest{
		JobID:        1,
		TaskID:       2,
		CollectionID: 3,
		PartitionIDs: []int64{common.AllPartitionsID},
		Vchannels:    []string{"ch0"},
		Schema:       schema,
		ImportFiles:  []*internalpb.ImportFile{{Paths: []string{"dummy.json"}}},
		Options: []*commonpb.KeyValuePair{
			{Key: importutilv2.WriteMode, Value: "Delete"},
		},
	}
	task := NewPreImportTask(req, manager, cm)
	manager.Add(task)

	futures := task.Execute()
	err = conc.AwaitAll(futures...)
	require.NoError(t, err)

	got := manager.Get(task.GetTaskID()).(*PreImportTask)
	require.Len(t, got.GetFileStats(), 1)
	fileStat := got.GetFileStats()[0]
	require.Equal(t, int64(3), fileStat.GetTotalRows())
	require.Contains(t, fileStat.GetHashedStats(), "ch0")
}

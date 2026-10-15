// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package importv2

import (
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestImportTaskRLSPredicate(t *testing.T) {
	paramtable.Init()
	schema := importRLSTestSchema()

	for _, test := range []struct {
		name       string
		expression string
		wantErr    bool
	}{
		{name: "allow", expression: "tenant == 7"},
		{name: "deny", expression: "tenant == 8", wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			manager := NewTaskManager()
			syncMgr := syncmgr.NewMockSyncManager(t)
			if !test.wantErr {
				syncMgr.EXPECT().SyncDataWithChunkManager(mock.Anything, mock.Anything, mock.Anything).
					Return(conc.Go(func() (struct{}, error) { return struct{}{}, nil }), nil).Once()
			}
			req := importRLSTestRequest(t, schema, test.expression)
			task := NewImportTask(req, manager, syncMgr, nil).(*ImportTask)
			require.NotNil(t, req.GetRlsCheckPredicate())

			reader := importutilv2.NewMockReader(t)
			var once sync.Once
			reader.EXPECT().Read().RunAndReturn(func() (*storage.InsertData, error) {
				var data *storage.InsertData
				once.Do(func() {
					data = importRLSTestData()
				})
				if data != nil {
					return data, nil
				}
				return nil, io.EOF
			})

			err := task.importFile(reader, nil, req.GetRlsCheckPredicate())
			if test.wantErr {
				require.ErrorIs(t, err, merr.ErrPrivilegeNotPermitted)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestImportTaskRLSPredicateWithAutoIDVarCharPrimaryKey(t *testing.T) {
	paramtable.Init()
	schema := importRLSTestSchema()
	schema.GetFields()[0].AutoID = true

	manager := NewTaskManager()
	syncMgr := syncmgr.NewMockSyncManager(t)
	syncMgr.EXPECT().SyncDataWithChunkManager(mock.Anything, mock.Anything, mock.Anything).
		Return(conc.Go(func() (struct{}, error) { return struct{}{}, nil }), nil).Once()
	req := importRLSTestRequest(t, schema, `pk == "100"`)
	task := NewImportTask(req, manager, syncMgr, nil).(*ImportTask)

	reader := importutilv2.NewMockReader(t)
	var once sync.Once
	reader.EXPECT().Read().RunAndReturn(func() (*storage.InsertData, error) {
		var data *storage.InsertData
		once.Do(func() {
			data = importRLSTestData()
			delete(data.Data, 100)
		})
		if data != nil {
			return data, nil
		}
		return nil, io.EOF
	})

	require.NoError(t, task.importFile(reader, nil, req.GetRlsCheckPredicate()))
}

func TestImportTaskEmptyRLSPredicateFailsDuringExecution(t *testing.T) {
	paramtable.Init()
	manager := NewTaskManager()
	req := importRLSTestRequest(t, importRLSTestSchema(), "tenant == 7")
	req.RlsCheckPredicate = &planpb.Expr{}
	task := NewImportTask(req, manager, syncmgr.NewMockSyncManager(t), nil)
	manager.Add(task)

	futures := task.Execute()
	require.Len(t, futures, 1)
	_, err := futures[0].Await()
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	require.Equal(t, datapb.ImportTaskStateV2_Failed, manager.Get(req.GetTaskID()).GetState())
	require.Contains(t, manager.Get(req.GetTaskID()).GetReason(), "persisted import RLS predicate has no expression")
}

func importRLSTestSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name: "rls_import",
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "pk",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.MaxLengthKey, Value: "128"},
				},
			},
			{
				FieldID:  101,
				Name:     "vec",
				DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.DimKey, Value: "2"},
				},
			},
			{FieldID: 102, Name: "tenant", DataType: schemapb.DataType_Int8},
		},
	}
}

func importRLSTestRequest(t *testing.T, schema *schemapb.CollectionSchema, expression string) *datapb.ImportRequest {
	t.Helper()
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)
	predicate, err := planparserv2.ParseExpr(helper, expression, nil)
	require.NoError(t, err)
	return &datapb.ImportRequest{
		JobID:             10,
		TaskID:            11,
		CollectionID:      12,
		PartitionIDs:      []int64{13},
		Vchannels:         []string{"v0"},
		Schema:            schema,
		Files:             []*internalpb.ImportFile{{Paths: []string{"dummy.json"}}},
		Ts:                1000,
		IDRange:           &datapb.IDRange{Begin: 100, End: 200},
		RequestSegments:   []*datapb.ImportRequestSegment{{SegmentID: 14, PartitionID: 13, Vchannel: "v0"}},
		RlsCheckPredicate: predicate,
	}
}

func importRLSTestData() *storage.InsertData {
	return &storage.InsertData{Data: map[int64]storage.FieldData{
		common.RowIDField: &storage.Int64FieldData{Data: []int64{1}},
		100:               &storage.StringFieldData{Data: []string{"one"}, DataType: schemapb.DataType_VarChar},
		101:               &storage.FloatVectorFieldData{Data: []float32{0.1, 0.2}, Dim: 2},
		102:               &storage.Int8FieldData{Data: []int8{7}},
	}}
}

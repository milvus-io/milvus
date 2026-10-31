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
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func partialUpdateJSONTestFieldData(rows [][]byte) *schemapb.FieldData {
	return &schemapb.FieldData{
		FieldName: "meta", FieldId: 102, Type: schemapb.DataType_JSON,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{Data: rows}}}},
	}
}

func partialUpdateValueTestFieldData(rows []int32) *schemapb.FieldData {
	return &schemapb.FieldData{
		FieldName: "value", FieldId: 101, Type: schemapb.DataType_Int32,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: rows}}}},
	}
}

// partialUpdateJSONTestTask builds a two-row partial upsert over a schema with
// a non-nullable JSON field; requestFields are the fields after the PK that the
// request itself carries.
func partialUpdateJSONTestTask(t *testing.T, requestFields ...*schemapb.FieldData) *UpsertTask {
	t.Helper()
	pk := partialUpdateCASPKFieldData([]int64{1, 2})
	pk.FieldName = "id"
	pk.FieldId = 100
	schema := mustNewSchemaInfo(&schemapb.CollectionSchema{
		Name: "test_partial_update_stored_json",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "value", DataType: schemapb.DataType_Int32},
			{FieldID: 102, Name: "meta", DataType: schemapb.DataType_JSON},
		},
	})
	fields := append([]*schemapb.FieldData{pk}, requestFields...)
	task := &UpsertTask{
		baseTask: baseTask{MetaCache: newTestCache()},
		ctx:      context.Background(), schema: schema, idAllocator: &allocator.IDAllocator{}, collectionID: 100,
		req: &milvuspb.UpsertRequest{Base: commonpbutil.NewMsgBase(), FieldsData: fields, NumRows: 2, PartialUpdate: true, CollectionName: schema.GetName()},
		upsertMsg: &msgstream.UpsertMsg{
			InsertMsg: &msgstream.InsertMsg{InsertRequest: &msgpb.InsertRequest{
				Base:           commonpbutil.NewMsgBase(),
				CollectionName: schema.GetName(), CollectionID: 100, PartitionName: "_default",
				FieldsData: fields, NumRows: 2, Version: msgpb.InsertDataVersion_ColumnBased,
			}},
			DeleteMsg: &msgstream.DeleteMsg{DeleteRequest: &msgpb.DeleteRequest{Base: commonpbutil.NewMsgBase(), CollectionName: schema.GetName(), CollectionID: 100, PartitionName: "_default"}},
		},
		node: &mockUpsertNode{}, result: &milvuspb.MutationResult{},
		partialUpdateOriginalFields: cloneFieldDataList(fields),
	}
	task.SetTs(12345)
	setPartialUpdateCASTestChannels(task, partialUpdateCASTestVChannels)
	return task
}

// A row stored before the proxy validated JSON documents may hold empty bytes.
// A partial upsert that leaves that JSON field out must still succeed (the
// stored row is carried over, not re-judged), while a request that itself
// supplies empty bytes for the JSON field must be rejected.
func TestPartialUpdateStoredInvalidJSONIsNotRejudged(t *testing.T) {
	storedMeta := [][]byte{[]byte(`{"a":1}`), {}}
	for _, testCase := range []struct {
		name        string
		request     []*schemapb.FieldData
		wantErr     bool
		wantMetaOut [][]byte
	}{
		{
			name:        "request omits the JSON field",
			request:     []*schemapb.FieldData{partialUpdateValueTestFieldData([]int32{100, 200})},
			wantMetaOut: storedMeta,
		},
		{
			name: "request supplies a valid JSON document",
			request: []*schemapb.FieldData{
				partialUpdateValueTestFieldData([]int32{100, 200}),
				partialUpdateJSONTestFieldData([][]byte{[]byte(`{"b":2}`), []byte(`{}`)}),
			},
			wantMetaOut: [][]byte{[]byte(`{"b":2}`), []byte(`{}`)},
		},
		{
			name: "request supplies empty bytes for the JSON field",
			request: []*schemapb.FieldData{
				partialUpdateValueTestFieldData([]int32{100, 200}),
				partialUpdateJSONTestFieldData([][]byte{[]byte(`{"b":2}`), {}}),
			},
			wantErr: true,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			task := partialUpdateJSONTestTask(t, testCase.request...)
			pk := partialUpdateCASPKFieldData([]int64{1, 2})
			pk.FieldName = "id"
			pk.FieldId = 100
			oldFields := []*schemapb.FieldData{
				pk,
				partialUpdateValueTestFieldData([]int32{10, 20}),
				partialUpdateJSONTestFieldData(storedMeta),
			}

			fakeWAL := newPartialUpdateCASTestWAL(t, 9)
			oldWAL := streaming.WAL()
			streaming.SetWALForTest(fakeWAL)
			defer streaming.SetWALForTest(oldWAL)
			read := mockey.Mock(retrieveByPKs).To(func(_ context.Context, task *UpsertTask, _ *schemapb.IDs, _ []string) (*milvuspb.QueryResults, segcore.StorageCost, error) {
				for _, proof := range task.partialUpdateCASGroups {
					proof.ReadTs = 1001
				}
				return &milvuspb.QueryResults{Status: merr.Success(), FieldsData: cloneFieldDataList(oldFields)}, segcore.StorageCost{}, nil
			}).Build()
			defer read.UnPatch()
			alloc := mockey.Mock(common.AllocAutoID).To(func(func(uint32) (typeutil.UniqueID, typeutil.UniqueID, error), uint32, uint64) (int64, int64, error) {
				return 1000, 1002, nil
			}).Build()
			defer alloc.UnPatch()

			require.NoError(t, task.prepareUpsert(context.Background()))
			err := task.insertPreExecute(context.Background())
			if testCase.wantErr {
				require.ErrorIs(t, err, merr.ErrParameterInvalid)
				require.ErrorContains(t, err, "meta")
				return
			}
			require.NoError(t, err)
			var meta *schemapb.FieldData
			for _, f := range task.upsertMsg.InsertMsg.GetFieldsData() {
				if f.GetFieldName() == "meta" {
					meta = f
				}
			}
			require.NotNil(t, meta)
			require.Equal(t, testCase.wantMetaOut, meta.GetScalars().GetJsonData().GetData())
			require.Equal(t, []int32{100, 200}, task.upsertMsg.InsertMsg.GetFieldsData()[1].GetScalars().GetIntData().GetData())
		})
	}
}

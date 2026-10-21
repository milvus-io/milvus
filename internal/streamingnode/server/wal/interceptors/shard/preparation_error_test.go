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

package shard

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/util/fileresource"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type preparationShardManager struct{ shards.ShardManager }

func (*preparationShardManager) Logger() *mlog.Logger { panic("unpatched") }

func (*preparationShardManager) CheckIfCollectionSchemaVersionMatch(*message.InsertMessageHeader) (int32, error) {
	panic("unpatched")
}

func TestMaterializationDefersPreparation(t *testing.T) {
	for _, tc := range []struct {
		name       string
		prepareErr error
		buildCalls int
	}{
		{"missing object", merr.WrapErrIoKeyNotFound("dictionary"), 0},
		{"denied access", merr.ErrIoPermissionDenied, 0},
		{"native construction", nil, 1},
	} {
		mockey.PatchConvey(tc.name, t, func() {
			mockey.Mock(fileresource.IsPullEnabled).Return(true).Build()
			acquire := mockey.Mock(fileresource.Acquire).To(func(context.Context, []int64, ...string) (*fileresource.Resources, error) {
				return &fileresource.Resources{}, tc.prepareErr
			}).Build()
			manager := function.NewFunctionRunnerManager()
			defer manager.Close()
			mockey.Mock(function.GetManager).Return(manager).Build()
			build := mockey.Mock(function.BuildEmbeddingRunner).Return(nil, merr.ErrServiceInternal).Build()
			mockey.Mock((*preparationShardManager).Logger).Return(mlog.With()).Build()
			mockey.Mock((*preparationShardManager).CheckIfCollectionSchemaVersionMatch).Return(int32(1), nil).Build()
			schema := &schemapb.CollectionSchema{
				Version: 1, FileResourceIds: []int64{7},
				Fields: []*schemapb.FieldSchema{
					{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{
						{Key: "analyzer_params", Value: `{"tokenizer":"standard","filter":[{"type":"stop","stop_words_file":{"type":"remote","resource_name":"dictionary","file_name":"stop.txt"}}]}`},
					}},
					{FieldID: 102, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
				},
				Functions: []*schemapb.FunctionSchema{{Id: 1, Type: schemapb.FunctionType_BM25, InputFieldIds: []int64{101}, OutputFieldIds: []int64{102}}},
			}
			require.NoError(t, manager.Alloc(1, WALFunctionRunnerKey("v1"), schema))
			item := &paramtable.Get().FunctionCfg.EnableWriteBeforeMaterialization
			previous := item.SwapTempValue("true")
			defer item.SwapTempValue(previous)
			impl := &shardInterceptor{shardManager: &preparationShardManager{}}
			msg := message.NewInsertMessageBuilderV1().WithVChannel("v1").
				WithHeader(&messagespb.InsertMessageHeader{CollectionId: 1}).
				WithBody(&msgpb.InsertRequest{}).MustBuildMutable()
			_, err := impl.handleInsertMessage(t.Context(), msg, func(context.Context, message.MutableMessage) (message.MessageID, error) {
				t.Error("failed preparation or construction must not append the message")
				return nil, nil
			})
			require.Error(t, err)
			var preparation *function.PreparationRequiredError
			require.ErrorAs(t, err, &preparation)
			require.Zero(t, acquire.Times(), "the interceptor must not fetch resources")
			require.Zero(t, build.Times(), "the interceptor must not initialize runners")
			err = preparation.Prepare(t.Context())
			require.Error(t, err)
			require.Equal(t, 1, acquire.Times())
			require.Equal(t, tc.buildCalls, build.Times())
			if tc.buildCalls > 0 {
				require.Contains(t, err.Error(), merr.ErrServiceInternal.Error(), "preparation must complete before native construction")
			}
			require.Empty(t, message.MustAsMutableInsertMessageV1(msg).MustBody().GetFieldsData())
		})
	}
}

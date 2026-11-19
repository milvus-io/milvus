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
	"strings"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/proxy/metacache"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestPKUpdatePrepareAndAppend(t *testing.T) {
	paramtable.Init()
	for _, name := range []string{"success", "oversize", "invalid", "namespace", "schema_mismatch", "mixed_failure"} {
		t.Run(name, func(t *testing.T) {
			cache := newTestCache()
			field := &schemapb.FieldSchema{FieldID: 101, Name: "name", DataType: schemapb.DataType_VarChar, Nullable: true, TypeParams: []*commonpb.KeyValuePair{{Key: "max_length", Value: "4096"}}}
			schema := mustNewSchemaInfo(updateTestSchema(field))
			schema.Version = 7
			if name == "namespace" {
				schema.EnableNamespace = true
			}
			info := &metacache.CollectionInfo{CollID: 10, Schema: schema, VChannels: []string{"p_10v0"}}
			patch := mockey.Mock((*metacache.MetaCache).GetCollectionInfo).Return(info, nil).Build()
			defer patch.UnPatch()
			patch = mockey.Mock((*metacache.CollectionInfo).GetWriteSchema).Return(schema, nil).Build()
			defer patch.UnPatch()
			wal := &invalidationTestWAL{}
			patch = mockey.Mock(streaming.WAL).Return(wal).Build()
			defer patch.UnPatch()
			appends, invalidations := 0, 0
			patch = mockey.Mock((*invalidationTestWAL).AppendMessages).To(func(_ *invalidationTestWAL, _ context.Context, msgs ...message.MutableMessage) streaming.AppendResponses {
				appends++
				require.NotEmpty(t, msgs)
				for _, m := range msgs {
					u := message.MustAsMutableUpdateMessageV2(m)
					require.EqualValues(t, 7, u.Header().GetSchemaVersion())
					require.EqualValues(t, -1, u.MustBody().GetPartitionId())
					require.Len(t, u.MustBody().GetAssignments(), 1)
				}
				switch name {
				case "schema_mismatch":
					return streaming.AppendResponses{Responses: []streaming.AppendResponse{{Error: status.NewSchemaVersionMismatch("schema advanced")}}}
				case "mixed_failure":
					return streaming.AppendResponses{Responses: []streaming.AppendResponse{{Error: status.NewOnShutdownError("first shard shutdown")}, {AppendResult: &types.AppendResult{TimeTick: 200}}, {Error: status.NewSchemaVersionMismatch("later shard advanced")}}}
				default:
					return streaming.AppendResponses{Responses: []streaming.AppendResponse{{AppendResult: &types.AppendResult{TimeTick: 200}}}}
				}
			}).Build()
			defer patch.UnPatch()
			patch = mockey.Mock((*metacache.MetaCache).RemoveCollectionsByID).To(func(_ *metacache.MetaCache, _ context.Context, id int64) []string {
				require.EqualValues(t, 10, id)
				invalidations++
				return nil
			}).Build()
			defer patch.UnPatch()
			req := &milvuspb.UpdateRecordsRequest{DbName: "default", CollectionName: "people", PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}}}}, Assignments: []*milvuspb.UpdateAssignment{{FieldName: "name", Literal: updateScalar(&schemapb.ValueField{Data: &schemapb.ValueField_StringData{StringData: "Alice"}})}}}
			if name == "invalid" {
				req.Assignments[0].FieldName = "pk"
			}
			if name == "oversize" {
				previous := paramtable.Get().PulsarCfg.MaxMessageSize.SwapTempValue("128")
				defer paramtable.Get().PulsarCfg.MaxMessageSize.SwapTempValue(previous)
				req.Assignments[0].Literal = updateScalar(&schemapb.ValueField{Data: &schemapb.ValueField_StringData{StringData: strings.Repeat("x", 1024)}})
			}
			result, err := UpdateRecords(t.Context(), cache, req)
			switch name {
			case "oversize", "invalid", "namespace":
				require.Error(t, err)
				require.Nil(t, result)
				require.Zero(t, appends)
			case "schema_mismatch":
				require.ErrorIs(t, err, merr.ErrCollectionSchemaMismatch)
				require.Equal(t, 1, appends)
				require.Equal(t, 1, invalidations)
			case "mixed_failure":
				require.ErrorContains(t, err, "first shard shutdown")
				require.Equal(t, 1, appends, "partially committed shards must not be replayed")
				require.Equal(t, 1, invalidations)
			case "success":
				require.NoError(t, err)
				require.EqualValues(t, 200, result.GetTimestamp())
				require.EqualValues(t, 3, result.GetUpdateCnt())
				require.True(t, result.GetAcknowledged())
				require.Equal(t, 1, appends)
			}
		})
	}
}

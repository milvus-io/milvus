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
	"strconv"
	"strings"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/proxy/metacache"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/l0materializer"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	walutil "github.com/milvus-io/milvus/internal/util/streamingutil/util"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type materializedUpdateRows struct {
	syncmgr.MetaWriter
	keys int64
}

func (r *materializedUpdateRows) UpdateSync(_ context.Context, task *syncmgr.SyncTask) error {
	_, _, delta, _ := task.Binlogs()
	for _, file := range delta.GetBinlogs() {
		r.keys += file.GetMutationCounts().GetUpdateKeys()
	}
	return nil
}

func checkPreparedUpdateMaterializes(t *testing.T, schema *schemapb.CollectionSchema, msgs []message.MutableMessage) {
	t.Helper()
	dir := t.TempDir()
	pt := paramtable.Get()
	oldType := pt.CommonCfg.StorageType.SwapTempValue("local")
	defer pt.CommonCfg.StorageType.SwapTempValue(oldType)
	oldPath := pt.LocalStorageCfg.Path.SwapTempValue(dir)
	defer pt.LocalStorageCfg.Path.SwapTempValue(oldPath)
	registry := &materializedUpdateRows{}
	writer := l0materializer.NewSyncMaterializer(storage.NewLocalChunkManager(objectstorage.RootPath(dir)), allocator.NewLocalAllocator(1, 100), registry)
	for i, msg := range msgs {
		tt := uint64(200 + i)
		sealed := msg.WithTimeTick(tt).WithLastConfirmed(walimplstest.NewTestMessageID(1)).IntoImmutableMessage(walimplstest.NewTestMessageID(int64(tt)))
		entry := messageutil.BuildTransformLogEntry(sealed, messageutil.TransformEntryOption{})
		require.NoError(t, writer.Materialize(t.Context(), l0materializer.MaterializeRequest{VChannel: msg.VChannel(), TargetTimeTick: tt, Schemas: map[uint64]*schemapb.CollectionSchema{tt: schema}, Entries: []*streamingpb.TransformLogEntry{entry}}))
	}
	require.EqualValues(t, 3, registry.keys)
}

func TestPKUpdatePrepareAndAppend(t *testing.T) {
	paramtable.Init()
	require.NoError(t, paramtable.Get().Save("proxy.pkUpdate.maxPkCount", "4"))
	t.Cleanup(func() { paramtable.Get().Reset("proxy.pkUpdate.maxPkCount") })
	for _, name := range []string{"struct_child", "success", "oversize", "invalid", "namespace", "schema_mismatch", "mixed_failure", "empty_keys", "below_limit", "at_limit", "over_limit", "duplicate_keys_over_limit", "kafka_oversize", "kafka_headroom", "kafka_larger_limit", "rocksmq", "prepared_oversize", "prepared_at_limit", "request_oversize"} {
		t.Run(name, func(t *testing.T) {
			cache := newTestCache()
			field := &schemapb.FieldSchema{FieldID: 101, Name: "name", DataType: schemapb.DataType_VarChar, Nullable: true, TypeParams: []*commonpb.KeyValuePair{{Key: "max_length", Value: "4096"}}}
			if strings.HasPrefix(name, "kafka_") {
				field.DataType, field.ElementType = schemapb.DataType_Array, schemapb.DataType_VarChar
				field.TypeParams = []*commonpb.KeyValuePair{{Key: "max_length", Value: "64"}, {Key: "max_capacity", Value: "4096"}}
			}
			collection := updateTestSchema(field)
			if name == "struct_child" {
				collection.StructArrayFields = []*schemapb.StructArrayFieldSchema{{FieldID: 102, Name: "profile", Fields: []*schemapb.FieldSchema{{FieldID: 103, Name: "profile[a]", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64, Nullable: true, TypeParams: []*commonpb.KeyValuePair{{Key: "max_capacity", Value: "8"}}}}}}
			}
			schema := mustNewSchemaInfo(collection)
			schema.Version = 7
			if name == "namespace" {
				schema.EnableNamespace = true
			}
			info := &metacache.CollectionInfo{CollID: 10, Schema: schema, VChannels: []string{"p_10v0", "p_10v1"}}
			patch := mockey.Mock((*metacache.MetaCache).GetCollectionInfo).Return(info, nil).Build()
			defer patch.UnPatch()
			walName := message.WALNamePulsar
			if name == "rocksmq" {
				walName = message.WALNameRocksmq
				previous := paramtable.Get().PulsarCfg.MaxMessageSize.SwapTempValue("128")
				defer paramtable.Get().PulsarCfg.MaxMessageSize.SwapTempValue(previous)
			}
			if strings.HasPrefix(name, "kafka_") {
				walName = message.WALNameKafka
				pulsarLimit, kafkaLimit := "2097152", "262144"
				if name == "kafka_larger_limit" {
					pulsarLimit, kafkaLimit = kafkaLimit, pulsarLimit
				}
				previous := paramtable.Get().PulsarCfg.MaxMessageSize.SwapTempValue(pulsarLimit)
				defer paramtable.Get().PulsarCfg.MaxMessageSize.SwapTempValue(previous)
				previous = paramtable.Get().KafkaCfg.ProducerMessageMaxBytes.SwapTempValue(kafkaLimit)
				defer paramtable.Get().KafkaCfg.ProducerMessageMaxBytes.SwapTempValue(previous)
			}
			patch = mockey.Mock(walutil.MustSelectWALName).Return(walName).Build()
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
				if name == "struct_child" {
					checkPreparedUpdateMaterializes(t, schema.CollectionSchema, msgs)
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
			if strings.HasPrefix(name, "kafka_") {
				values := make([]string, 4096)
				width := 64
				if name == "kafka_headroom" {
					width = 48 // Fits 256 KiB, but not after the 64 KiB envelope reserve.
				}
				for i := range values {
					values[i] = strings.Repeat("x", width)
				}
				req.Assignments[0].Literal = &schemapb.UpdateLiteral{Value: &schemapb.UpdateLiteral_ArrayValue{ArrayValue: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: values}}}}}
			}
			if strings.HasPrefix(name, "prepared_") || name == "request_oversize" {
				assignments, err := validateUpdateAssignments(schema.CollectionSchema, req.GetAssignments())
				require.NoError(t, err)
				prepared, err := buildUpdateMessages(info.CollID, schema.Version, -1, info.VChannels, req.GetPrimaryKeys(), assignments, 4096, 1<<20, nil)
				require.NoError(t, err)
				var budget int
				for _, msg := range prepared {
					budget += proto.Size(msg.IntoMessageProto())
				}
				if name == "prepared_oversize" {
					budget--
					require.Greater(t, budget, proto.Size(req), "fanout, not the incoming request, exceeds the budget")
				} else if name == "request_oversize" {
					budget = proto.Size(req) - 1
				}
				previous := paramtable.Get().ProxyGrpcServerCfg.ServerMaxRecvSize.SwapTempValue(strconv.Itoa(budget))
				defer paramtable.Get().ProxyGrpcServerCfg.ServerMaxRecvSize.SwapTempValue(previous)
			}
			if name == "rocksmq" {
				req.Assignments[0].Literal = updateScalar(&schemapb.ValueField{Data: &schemapb.ValueField_StringData{StringData: strings.Repeat("x", 512)}})
			}
			switch name {
			case "empty_keys":
				req.PrimaryKeys.GetIntId().Data = nil
			case "at_limit":
				req.PrimaryKeys.GetIntId().Data = []int64{1, 2, 3, 4}
			case "over_limit":
				req.PrimaryKeys.GetIntId().Data = []int64{1, 2, 3, 4, 5}
			case "duplicate_keys_over_limit":
				req.PrimaryKeys.GetIntId().Data = []int64{1, 1, 1, 1, 1}
			}
			if name == "invalid" {
				req.Assignments[0].FieldName = "pk"
			}
			if name == "oversize" {
				previous := paramtable.Get().PulsarCfg.MaxMessageSize.SwapTempValue("128")
				defer paramtable.Get().PulsarCfg.MaxMessageSize.SwapTempValue(previous)
				req.Assignments[0].Literal = updateScalar(&schemapb.ValueField{Data: &schemapb.ValueField_StringData{StringData: strings.Repeat("x", 1024)}})
			}
			if name == "struct_child" {
				for _, value := range []*schemapb.UpdateLiteral{updateNull(), {Value: &schemapb.UpdateLiteral_ArrayValue{ArrayValue: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1}}}}}}} {
					child := proto.Clone(req).(*milvuspb.UpdateRecordsRequest)
					child.Assignments = []*milvuspb.UpdateAssignment{{FieldName: "profile[a]", Literal: value}}
					_, err := UpdateRecords(t.Context(), cache, child)
					require.ErrorIs(t, err, merr.ErrParameterInvalid)
					require.Zero(t, appends)
				}
			}
			result, err := UpdateRecords(t.Context(), cache, req)
			switch name {
			case "oversize", "invalid", "namespace", "empty_keys", "over_limit", "duplicate_keys_over_limit", "kafka_oversize", "kafka_headroom", "prepared_oversize", "request_oversize":
				require.Error(t, err)
				require.Nil(t, result)
				require.Zero(t, appends)
				if name == "over_limit" || name == "duplicate_keys_over_limit" || name == "kafka_oversize" || name == "kafka_headroom" || name == "prepared_oversize" || name == "request_oversize" {
					require.ErrorIs(t, err, merr.ErrParameterTooLarge)
				}
			case "schema_mismatch":
				require.ErrorIs(t, err, merr.ErrCollectionSchemaMismatch)
				require.Equal(t, 1, appends)
				require.Equal(t, 1, invalidations)
			case "mixed_failure":
				require.ErrorContains(t, err, "first shard shutdown")
				require.Equal(t, 1, appends, "partially committed shards must not be replayed")
				require.Equal(t, 1, invalidations)
			case "struct_child", "success", "below_limit", "at_limit", "kafka_larger_limit", "rocksmq", "prepared_at_limit":
				require.NoError(t, err)
				require.EqualValues(t, 200, result.GetTimestamp())
				expectedCount := int64(3)
				if name == "at_limit" {
					expectedCount = 4
				}
				require.EqualValues(t, expectedCount, result.GetUpdateCnt())
				require.True(t, result.GetAcknowledged())
				require.Equal(t, 1, appends)
			}
		})
	}
}

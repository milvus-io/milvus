// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dml

import (
	"context"
	"fmt"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/proxy/metacache"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestPartialUpdateFencePreservesNonCASOutcome(t *testing.T) {
	for _, relative := range []bool{false, true} {
		for _, landed := range []bool{false, true} {
			for _, fenceFirst := range []bool{false, true} {
				for _, cause := range []error{status.New(streamingpb.StreamingCode_STREAMING_CODE_INVALID_TRANSACTION_STATE, "commit outcome is unknown"), context.DeadlineExceeded, status.NewReplicateViolation("role changed on sibling"), status.NewSchemaVersionMismatch("schema changed")} {
					t.Run(fmt.Sprintf("relative=%t/landed=%t/fence_first=%t/cause=%s", relative, landed, fenceFirst, cause), func(t *testing.T) {
						pre, post := twoShardSplit()
						fixture := newSplitFenceFixture(t, pre, post)
						task := fixture.upsertTask(t, seqPKs(2), seqPKs(2))
						task.req.PartialUpdate = true
						if relative {
							prepareMixedFenceTask(t, task, "relative")
						}
						responses := []streaming.AppendResponse{{Error: status.NewShardFenced(splitSource)}, {Error: cause}, {Error: status.NewPartialUpdateRetryable("CAS aborted")}}
						if !fenceFirst {
							responses[0], responses[1] = responses[1], responses[0]
						}
						if landed {
							responses = append(responses, streaming.AppendResponse{AppendResult: &types.AppendResult{TimeTick: 2000}})
						}
						err := task.partialUpdateFenceRefusal(context.Background(), streaming.AppendResponses{Responses: responses})
						if relative && landed {
							require.ErrorIs(t, err, merr.ErrCollectionPartialUpdateConflict)
							require.EqualValues(t, 111, merr.Status(err).GetCode())
							require.False(t, merr.Status(err).GetRetriable())
							require.NotContains(t, err.Error(), "STREAMING_CODE_REPLICATE_VIOLATION", "preserve the original fence cause instead of introducing an SDK redirect signal")
						} else {
							require.Equal(t, cause, err, "a split refusal cannot classify another channel's unknown result")
						}
						require.False(t, isPartialUpdateCASRetry(err))
						if (!relative || !landed) && status.AsStreamingError(cause).Code == streamingpb.StreamingCode_STREAMING_CODE_INVALID_TRANSACTION_STATE {
							public := merr.Status(err)
							require.EqualValues(t, 65535, public.GetCode())
							require.False(t, public.GetRetriable())
						}
						require.Equal(t, 1, fixture.evictions, "a mixed failure still invalidates the refused routing")
					})
				}
			}
		}
	}
}

// Exercise actual routing, Insert/Delete packing, CAS envelope attachment,
// per-channel row accounting and the outer conditional execution boundary. Only
// WAL results are injected, with the same per-vchannel transaction contract as
// AppendMessagesWithOptions; this is an in-process regression, not SDK e2e.
func TestPartialUpsertMixedFenceAccountsPackedCommit(t *testing.T) {
	channels := append(append([]string(nil), partialUpdateCASTestVChannels...), "by-dev-rootcoord-dml_2_1001v2")
	pks := make([]int64, len(channels))
	for pk := int64(1); ; pk++ {
		indexes, err := typeutil.HashPK2Channels(partialUpdateCASIDs([]int64{pk}), channels)
		require.NoError(t, err)
		if pks[indexes[0]] == 0 {
			pks[indexes[0]] = pk
		}
		if pks[0] != 0 && pks[1] != 0 && pks[2] != 0 {
			break
		}
	}
	for _, mode := range []string{"replace", "relative", "conditional"} {
		for _, failure := range []string{"deadline", "schema", "unknown_commit"} {
			t.Run(fmt.Sprintf("mode=%s/failure=%s", mode, failure), func(t *testing.T) {
				conditional := mode == "conditional"
				relative := mode == "relative"
				task := partialUpdateCASRealPackTestTask(t, pks, pks, pks)
				setPartialUpdateCASTestChannels(t, task, channels)
				prepareMixedFenceTask(t, task, mode)
				wal := newPartialUpdateCASTestWAL(t, 9)
				previous := streaming.WAL()
				streaming.SetWALForTest(wal)
				t.Cleanup(func() { streaming.SetWALForTest(previous) })
				preparePartialUpdateCASTestGroups(t, task)
				partition := mockey.Mock((*metacache.MetaCache).GetPartitionID).Return(UniqueID(100), nil).Build()
				defer partition.UnPatch()
				nextID := int64(1000)
				allocation := mockey.Mock((*allocator.IDAllocator).Alloc).To(func(_ *allocator.IDAllocator, n uint32) (UniqueID, UniqueID, error) {
					begin := nextID
					nextID += int64(n)
					return begin, nextID, nil
				}).Build()
				defer allocation.UnPatch()
				cause := context.DeadlineExceeded
				switch failure {
				case "schema":
					cause = status.NewSchemaVersionMismatch("changed schema on sibling")
				case "unknown_commit":
					cause = status.New(streamingpb.StreamingCode_STREAMING_CODE_INVALID_TRANSACTION_STATE, "commit outcome is unknown")
				}
				wal.appendHook = func(_ context.Context, messages ...message.MutableMessage) streaming.AppendResponses {
					responses := streaming.AppendResponses{Responses: make([]streaming.AppendResponse, len(messages))}
					for i, msg := range messages {
						if msg.MessageType() == message.MessageTypeInsert {
							body := message.MustAsMutableInsertMessageV1(msg).MustBody()
							require.Len(t, body.GetFieldsData(), len(task.schema.GetFields()), "packing must preserve every column of the prepared row")
							found := false
							for _, field := range body.GetFieldsData() {
								if relative && field.GetFieldName() == "tags" {
									found = true
									require.Equal(t, []int64{7, 1}, field.GetScalars().GetArrayData().GetData()[0].GetLongData().GetData())
								}
								if conditional && field.GetFieldName() == "version" {
									found = true
									require.Equal(t, []int64{5}, field.GetScalars().GetLongData().GetData())
								}
							}
							if relative || conditional {
								require.True(t, found, "real packing must preserve the prepared operand result")
							}
						}
						switch msg.VChannel() {
						case channels[0]:
							responses.Responses[i].Error = status.NewShardFenced(channels[0])
						case channels[1]:
							responses.Responses[i].Error = cause
						case channels[2]:
							responses.Responses[i].AppendResult = &types.AppendResult{TimeTick: 2000}
						default:
							t.Fatalf("unexpected packed channel: %s", msg.VChannel())
						}
					}
					return responses
				}
				err := task.Execute(context.Background())
				require.Error(t, err)
				if conditional {
					require.ErrorIs(t, err, merr.ErrCollectionConditionalUpsertExecution)
					require.False(t, merr.Status(err).GetRetriable())
				} else if relative {
					require.ErrorIs(t, err, merr.ErrCollectionPartialUpdateConflict)
					require.EqualValues(t, 111, merr.Status(err).GetCode())
					require.False(t, merr.Status(err).GetRetriable())
				} else if failure == "schema" {
					require.ErrorIs(t, err, merr.ErrCollectionSchemaMismatch)
				} else if failure == "deadline" {
					require.ErrorIs(t, err, context.DeadlineExceeded)
				} else {
					require.Equal(t, cause, err)
					require.EqualValues(t, 65535, merr.Status(err).GetCode())
					require.False(t, merr.Status(err).GetRetriable())
				}
				require.False(t, isPartialUpdateCASRetry(err))
				require.Equal(t, 1, wal.appendCalls, "mixed outcomes cannot start a CAS rebuild or a split resend")
				require.True(t, task.partialState.appendAttempted)
				require.Equal(t, map[int]any{2: pks[2]}, task.partialState.written)
				require.EqualValues(t, 1, task.partialState.insertCount)
				require.EqualValues(t, 1, task.partialState.deleteCount)
				require.EqualValues(t, 2000, task.partialState.timestamp)
				require.Empty(t, task.partialState.retryRows)
				require.NoError(t, task.PostExecute(context.Background()))
				require.Equal(t, []int64{pks[2]}, task.result.GetIDs().GetIntId().GetData())
				require.EqualValues(t, 1, task.result.GetUpsertCnt())
			})
		}
	}
}

// Build only these new tests' prepared partial-update rows. Conditional inputs
// pass the real validator; every mode supplies the complete merged row to packing.
func prepareMixedFenceTask(t *testing.T, task *UpsertTask, mode string) {
	t.Helper()
	schema := proto.Clone(task.schema.CollectionSchema).(*schemapb.CollectionSchema)
	n := len(task.result.GetIDs().GetIntId().GetData())
	task.req.PartialUpdate = true
	task.req.FieldOps = nil
	task.req.WriteCondition = nil
	mergedFields := make(map[string]*schemapb.FieldData)
	switch mode {
	case "replace":
	case "conditional":
		schema.Fields = append(schema.Fields, &schemapb.FieldSchema{FieldID: 103, Name: "version", DataType: schemapb.DataType_Int64})
		versions := make([]int64, n)
		for i := range versions {
			versions[i] = 5
		}
		version := &schemapb.FieldData{
			FieldId: 103, FieldName: "version", Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: versions}}}},
		}
		task.req.FieldsData = append(task.req.FieldsData, version)
		task.req.WriteCondition = &milvuspb.WriteCondition{Field: "version", Op: milvuspb.WriteCondition_GT}
	case "relative":
		// Reuse the partial-op tests' legal Int64-array schema and operands.
		schema.Fields = append(schema.Fields, arrayIntFieldSchema("tags", false, 8))
		operands, merged := make([][]int64, n), make([][]int64, n)
		for i := range operands {
			operands[i], merged[i] = []int64{1}, []int64{7, 1}
		}
		operand := arrayLongFieldData("tags", operands)
		operand.FieldId = 1000
		task.req.FieldsData = []*schemapb.FieldData{partialUpdateCASPKFieldData(task.result.GetIDs().GetIntId().GetData()), operand}
		task.req.FieldOps = []*schemapb.FieldPartialUpdateOp{op("tags", schemapb.FieldPartialUpdateOp_ARRAY_APPEND)}
		// Preparation has merged a stored [7] with the incoming [1].
		mergedFields["tags"] = arrayLongFieldData("tags", merged)
	default:
		t.Fatalf("unknown mixed-fence mode %q", mode)
	}
	task.schema = mustNewSchemaInfo(schema)
	require.NoError(t, task.validateWriteCondition(), "conditional fixture must satisfy the real request contract before execution")
	plans, relative, err := resolveFieldPartialUpdateOps(task.req, task.schema)
	require.NoError(t, err)
	require.Equal(t, mode == "relative", relative)
	task.fieldPartialUpdatePlans = plans
	task.partialUpdateOriginalFields = cloneFieldDataList(task.req.GetFieldsData())
	for _, field := range task.req.GetFieldsData() {
		if mergedFields[field.GetFieldName()] == nil {
			mergedFields[field.GetFieldName()] = proto.Clone(field).(*schemapb.FieldData)
		}
	}
	task.upsertMsg.InsertMsg.FieldsData = nil
	for _, field := range schema.GetFields() {
		data := mergedFields[field.GetName()]
		if data == nil {
			data = generateFieldData(field.GetDataType(), field.GetName(), n)
		}
		data.FieldId = field.GetFieldID()
		require.Equal(t, field.GetDataType(), data.GetType())
		task.upsertMsg.InsertMsg.FieldsData = append(task.upsertMsg.InsertMsg.FieldsData, data)
	}
	initPreparedPartialTestState(t, task)
}

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

package txn

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	upsertfixture "github.com/milvus-io/milvus/pkg/v3/streaming/util/message/testutils"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestUpsertSuccessfulReferencesAndTerminalCleanup(t *testing.T) {
	metrics := metricsutil.NewTxnMetrics("upsert-reference-test")
	defer metrics.Close()
	session := newTxnSession("p_1v0", message.TxnContext{TxnID: 1, Keepalive: time.Second}, 10, metrics.BeginTxn())
	body := message.NewUpsertMessageBuilderV2().WithVChannel("p_1v0").WithHeader(&message.UpsertMessageHeader{}).WithBody(&message.UpsertMessageBody{}).MustBuildMutable().WithTxnContext(session.TxnContext()).WithTimeTick(11).WithLastConfirmedUseMessageID().IntoImmutableMessage(walimplstest.NewTestMessageID(1))
	require.NoError(t, session.AddNewMessage(context.Background(), 11))
	session.RecordSuccessfulBody(body)
	session.AddNewMessageDoneAndKeepalive(11, message.MessageTypeUpsert)
	require.True(t, session.HasUpsert())
	require.Len(t, session.SuccessfulBodies(), 1)
	require.NoError(t, session.RequestCommitAndWait(context.Background(), 12, 0), "Upsert count is rejected at shard admission after the rejection defer exists")
	session.RejectCommit()
	require.Empty(t, session.SuccessfulBodies())
	require.False(t, session.HasUpsert())
}

func TestUpsertScopePreAdmission(t *testing.T) {
	metrics := metricsutil.NewTxnMetrics("upsert-scope-test")
	defer metrics.Close()
	session := newTxnSession("p_1v0", message.TxnContext{TxnID: 1, Keepalive: time.Second}, 10, metrics.BeginTxn())
	for _, channel := range []string{"", "p_vcchan", "p_01v0", "p_1v1", "p_2v0"} {
		require.Error(t, session.ValidateUpsertScope(channel))
		require.Zero(t, session.inFlightCount)
		require.Zero(t, session.bodyCount)
		require.Equal(t, uint64(10), session.lastTimetick)
	}
	require.NoError(t, session.ValidateUpsertScope("p_1v0"))
	session.Cleanup()
}

func TestLegacySuccessfulReferencesRetainedMemory(t *testing.T) {
	metrics := metricsutil.NewTxnMetrics("legacy-reference-memory")
	defer metrics.Close()
	session := newTxnSession("p_1v0", message.TxnContext{TxnID: 1, Keepalive: message.TxnKeepaliveInfinite}, 10, metrics.BeginTxn())
	runtime.GC()
	var before, retained, released runtime.MemStats
	runtime.ReadMemStats(&before)
	const bodyCount = 32
	const payloadBytes = 256 * 1024
	var estimated int64
	for i := 0; i < bodyCount; i++ {
		base := message.NewInsertMessageBuilderV1().WithVChannel("p_1v0").WithHeader(&message.InsertMessageHeader{CollectionId: 1}).WithBody(&msgpb.InsertRequest{}).MustBuildMutable()
		payload := make([]byte, payloadBytes)
		payload[0] = byte(i)
		body := message.NewMutableMessageBeforeAppend(payload, base.Properties().ToRawMap()).WithTimeTick(uint64(i + 11)).WithTxnContext(session.TxnContext()).WithLastConfirmedUseMessageID().IntoImmutableMessage(walimplstest.NewTestMessageID(int64(i + 1)))
		estimated += int64(body.EstimateSize())
		require.NoError(t, session.AddNewMessage(context.Background(), uint64(i+11)))
		session.RecordSuccessfulBody(body)
		session.AddNewMessageDoneAndKeepalive(uint64(i+11), message.MessageTypeInsert)
	}
	runtime.GC()
	runtime.ReadMemStats(&retained)
	require.Len(t, session.SuccessfulBodies(), bodyCount)
	require.GreaterOrEqual(t, estimated, int64(bodyCount*payloadBytes))
	session.abort()
	runtime.GC()
	runtime.ReadMemStats(&released)
	require.Empty(t, session.SuccessfulBodies())
	t.Logf("legacy refs=%d payload=%d estimated_retained_bytes=%d measured_heap_increase=%d measured_heap_release=%d", bodyCount, bodyCount*payloadBytes, estimated, int64(retained.HeapAlloc)-int64(before.HeapAlloc), int64(retained.HeapAlloc)-int64(released.HeapAlloc))
	runtime.KeepAlive(session)
}

func TestRetainedBeginAndRecoveredScopeSurviveUntilResolution(t *testing.T) {
	metrics := metricsutil.NewTxnMetrics("upsert-begin-test")
	defer metrics.Close()
	session := newTxnSession("p_1v0", message.TxnContext{TxnID: 1, Keepalive: message.TxnKeepaliveInfinite}, 10, metrics.BeginTxn())
	begin := message.NewBeginTxnMessageBuilderV2().WithVChannel("p_1v0").WithHeader(&message.BeginTxnMessageHeader{}).WithBody(&message.BeginTxnMessageBody{}).MustBuildMutable().WithTxnContext(session.TxnContext()).WithTimeTick(10).WithLastConfirmedUseMessageID().IntoImmutableMessage(walimplstest.NewTestMessageID(1))
	session.RecordBegin(begin)
	require.Same(t, begin, session.BeginMessage())
	body := message.NewUpsertMessageBuilderV2().WithVChannel("p_2v0").WithHeader(&message.UpsertMessageHeader{}).WithBody(&message.UpsertMessageBody{}).MustBuildMutable().WithTxnContext(session.TxnContext()).WithTimeTick(11).WithLastConfirmedUseMessageID().IntoImmutableMessage(walimplstest.NewTestMessageID(2))
	session.RecordSuccessfulBody(body)
	require.True(t, session.HasInvalidScope())
	require.Len(t, session.SuccessfulBodies(), 1)
	session.abort()
	require.Nil(t, session.BeginMessage())
	require.Empty(t, session.SuccessfulBodies())
}

func TestTerminalSessionCannotRetainLateSuccessfulBody(t *testing.T) {
	metrics := metricsutil.NewTxnMetrics("upsert-late-reference")
	defer metrics.Close()
	session := newTxnSession("p_1v0", message.TxnContext{TxnID: 1, Keepalive: message.TxnKeepaliveInfinite}, 10, metrics.BeginTxn())
	body := message.NewInsertMessageBuilderV1().WithVChannel("p_1v1").WithHeader(&message.InsertMessageHeader{}).WithBody(&msgpb.InsertRequest{}).MustBuildMutable().WithTxnContext(session.TxnContext()).WithTimeTick(11).WithLastConfirmedUseMessageID().IntoImmutableMessage(walimplstest.NewTestMessageID(2))
	session.abort()
	session.RecordBegin(body)
	session.RecordSuccessfulBody(body)
	require.Nil(t, session.BeginMessage())
	require.Empty(t, session.SuccessfulBodies())
}

func TestRecoveredUpsertCommitValidatesScopeAndCounts(t *testing.T) {
	for _, test := range []struct {
		name, bodyChannel, commitChannel string
		count                            uint64
		overlap                          bool
	}{
		{"foreign recovered body", "p_1v1", "p_1v0", 1, false},
		{"foreign commit", "p_1v0", "p_1v1", 1, false},
		{"control commit", "p_1v0", "p_vcchan", 1, false},
		{"zero count", "p_1v0", "p_1v0", 0, false},
		{"wrong count", "p_1v0", "p_1v0", 3, false},
		{"duplicate after lost ack", "p_1v0", "p_1v0", 1, true},
		{"foreign body wrong count", "p_1v1", "p_1v0", 3, false},
		{"foreign commit wrong count", "p_1v0", "p_1v1", 3, false},
		{"foreign collection wrong count", "p_1v0", "p_1v0", 3, false},
		{"unreplicable body wrong count", "p_1v0", "p_1v0", 3, false},
		{"overlapping bodies", "p_1v0", "p_1v0", 2, true},
		{"legacy duplicate disjoint", "p_1v0", "p_1v0", 2, false},
		{"legacy duplicate overlap", "p_1v0", "p_1v0", 2, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			resource.InitForTest(t)
			txnContext := message.TxnContext{TxnID: 10, Keepalive: message.TxnKeepaliveInfinite}
			begin := message.NewBeginTxnMessageBuilderV2().WithVChannel("p_1v0").WithHeader(&message.BeginTxnMessageHeader{}).WithBody(&message.BeginTxnMessageBody{}).MustBuildMutable().WithTxnContext(txnContext).WithTimeTick(1).WithLastConfirmedUseMessageID().IntoImmutableMessage(walimplstest.NewTestMessageID(1))
			builder := message.NewImmutableTxnMessageBuilder(message.MustAsImmutableBeginTxnMessageV2(begin))
			bodyMessage := upsertForTest(test.bodyChannel).WithTxnContext(txnContext).WithLastConfirmedUseMessageID()
			if test.name == "foreign collection wrong count" {
				typed := message.MustAsMutableUpsertMessageV2(bodyMessage)
				header := typed.Header()
				header.CollectionId = 2
				typed.OverwriteHeader(header)
			}
			if test.name == "unreplicable body wrong count" {
				bodyMessage.Properties().ToRawMap()["_ur"] = ""
			}
			body := bodyMessage.IntoImmutableMessage(walimplstest.NewTestMessageID(2))
			if test.name == "legacy duplicate disjoint" || test.name == "legacy duplicate overlap" {
				key := int64(8)
				if test.name == "legacy duplicate overlap" {
					key = 7
				}
				legacy := message.NewInsertMessageBuilderV1().WithVChannel("p_1v0").WithHeader(&message.InsertMessageHeader{CollectionId: 1}).WithBody(&msgpb.InsertRequest{CollectionID: 1, NumRows: 2, FieldsData: []*schemapb.FieldData{{FieldId: 100, Type: schemapb.DataType_Int64, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{key, key}}}}}}}}).MustBuildMutable().WithTxnContext(txnContext).WithTimeTick(2).WithLastConfirmedUseMessageID().IntoImmutableMessage(walimplstest.NewTestMessageID(3))
				builder.Add(legacy)
			}
			builder.Add(body)
			if test.overlap {
				builder.Add(upsertForTest(test.bodyChannel).WithTxnContext(txnContext).WithTimeTick(3).WithLastConfirmedUseMessageID().IntoImmutableMessage(walimplstest.NewTestMessageID(3)))
			}
			manager := NewTxnManager(types.PChannelInfo{Name: "p"}, map[message.TxnID]*message.ImmutableTxnMessageBuilder{10: builder})
			session, err := manager.GetSessionOfTxn(10)
			require.NoError(t, err)
			require.NoError(t, session.RequestCommitAndWait(context.Background(), 20, test.count))
			commit := message.NewCommitTxnMessageBuilderV2().WithVChannel(test.commitChannel).WithHeader(&message.CommitTxnMessageHeader{BodyCnt: test.count}).WithBody(&message.CommitTxnMessageBody{}).MustBuildMutable().WithTxnContext(txnContext).WithTimeTick(20)
			err = session.ValidateCommit(context.Background(), commit)
			if test.name == "legacy duplicate disjoint" || test.name == "legacy duplicate overlap" || test.name == "overlapping bodies" {
				require.NoError(t, err)
				session.CommitDone()
			} else {
				require.Error(t, err)
				if test.name == "wrong count" || test.name == "duplicate after lost ack" {
					require.True(t, status.AsStreamingError(err).IsTxnExpired(), "%v", err)
				} else {
					require.True(t, status.AsStreamingError(err).IsUnrecoverable(), "%v", err)
				}
				session.RejectCommit()
			}
			require.Empty(t, session.SuccessfulBodies())
			manager.FailTxnAtVChannel("")
		})
	}
}

func upsertForTest(vc string) message.MutableMessage {
	version := int32(1)
	return message.NewUpsertMessageBuilderV2().WithVChannel(vc).WithHeader(&message.UpsertMessageHeader{CollectionId: 1, SchemaVersion: &version, Partitions: []*message.PartitionSegmentAssignment{{PartitionId: 2, Rows: 1}}, DeleteRows: 1}).WithBody(upsertfixture.Body(&msgpb.InsertRequest{Version: msgpb.InsertDataVersion_ColumnBased, CollectionID: 1, PartitionID: 2, NumRows: 1, RowIDs: []int64{7}, FieldsData: []*schemapb.FieldData{{FieldId: 100, Type: schemapb.DataType_Int64, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{7}}}}}}}}, &msgpb.DeleteRequest{CollectionID: 1, PartitionID: -1, NumRows: 1, PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{7}}}}})).MustBuildMutable().WithTimeTick(10)
}

func TestUpsertCommitRejectsForbiddenEnvelopes(t *testing.T) {
	for _, recovered := range []bool{false, true} {
		for _, marker := range []string{"", "_ur", "_np", "_pcl", "_bh"} {
			for _, component := range []string{"Begin", "Upsert", "legacy Insert", "legacy Delete", "Commit"} {
				origin := "live"
				if recovered {
					origin = "recovered"
				}
				t.Run(origin+"/"+marker+"/"+component, func(t *testing.T) {
					resource.InitForTest(t)
					txnCtx := message.TxnContext{TxnID: 10, Keepalive: message.TxnKeepaliveInfinite}
					begin := message.NewBeginTxnMessageBuilderV2().WithVChannel("p_1v0").WithHeader(&message.BeginTxnMessageHeader{}).
						WithBody(&message.BeginTxnMessageBody{}).MustBuildMutable().WithTxnContext(txnCtx).WithTimeTick(1).WithLastConfirmedUseMessageID()
					if marker != "" && component == "Begin" {
						begin.Properties().ToRawMap()[marker] = ""
					}
					builder := message.NewImmutableTxnMessageBuilder(message.MustAsImmutableBeginTxnMessageV2(begin.IntoImmutableMessage(walimplstest.NewTestMessageID(1))))
					upsert := upsertForTest("p_1v0").WithTxnContext(txnCtx).WithLastConfirmedUseMessageID()
					if marker != "" && component == "Upsert" {
						upsert.Properties().ToRawMap()[marker] = ""
					}
					builder.Add(upsert.IntoImmutableMessage(walimplstest.NewTestMessageID(2)))
					count := uint64(1)
					if component == "legacy Insert" || component == "legacy Delete" {
						var legacy message.MutableMessage
						if component == "legacy Insert" {
							legacy = message.NewInsertMessageBuilderV1().WithVChannel("p_1v0").WithHeader(&message.InsertMessageHeader{CollectionId: 1}).WithBody(&msgpb.InsertRequest{CollectionID: 1}).MustBuildMutable()
						} else {
							legacy = message.NewDeleteMessageBuilderV1().WithVChannel("p_1v0").WithHeader(&message.DeleteMessageHeader{CollectionId: 1}).WithBody(&msgpb.DeleteRequest{CollectionID: 1}).MustBuildMutable()
						}
						if marker != "" {
							legacy.Properties().ToRawMap()[marker] = ""
						}
						builder.Add(legacy.WithTxnContext(txnCtx).WithTimeTick(3).WithLastConfirmedUseMessageID().IntoImmutableMessage(walimplstest.NewTestMessageID(3)))
						count++
					}
					var session *TxnSession
					if recovered {
						manager := NewTxnManager(types.PChannelInfo{Name: "p"}, map[message.TxnID]*message.ImmutableTxnMessageBuilder{10: builder})
						defer manager.Close()
						var err error
						session, err = manager.GetSessionOfTxn(10)
						require.NoError(t, err)
					} else {
						metrics := metricsutil.NewTxnMetrics("upsert-envelope-test")
						defer metrics.Close()
						session = newTxnSession("p_1v0", txnCtx, 1, metrics.BeginTxn())
						retainedBegin, bodies := builder.Messages()
						session.RecordBegin(retainedBegin)
						for _, body := range bodies {
							require.NoError(t, session.AddNewMessage(t.Context(), body.TimeTick()))
							session.RecordSuccessfulBody(body)
							session.AddNewMessageDoneAndKeepalive(body.TimeTick(), body.MessageType())
						}
					}
					require.NoError(t, session.RequestCommitAndWait(t.Context(), 20, count))
					commit := message.NewCommitTxnMessageBuilderV2().WithVChannel("p_1v0").WithHeader(&message.CommitTxnMessageHeader{BodyCnt: count}).
						WithBody(&message.CommitTxnMessageBody{}).MustBuildMutable().WithTxnContext(txnCtx).WithTimeTick(20)
					if marker != "" && component == "Commit" {
						commit.Properties().ToRawMap()[marker] = ""
					}
					err := session.ValidateCommit(t.Context(), commit)
					if marker == "" {
						require.NoError(t, err, "the same members must pass with valid envelopes")
						session.CommitDone()
					} else {
						require.Error(t, err)
						require.True(t, status.AsStreamingError(err).IsUnrecoverable())
						session.RejectCommit()
					}
					require.Empty(t, session.SuccessfulBodies())
					require.Nil(t, session.BeginMessage())
				})
			}
		}
	}
}

func TestRecoveredUpsertInsertOnlyClassification(t *testing.T) {
	resource.InitForTest(t)
	for _, effects := range []bool{false, true} {
		ctx := message.TxnContext{TxnID: 70}
		begin := message.NewBeginTxnMessageBuilderV2().WithVChannel("p_1v0").WithHeader(&message.BeginTxnMessageHeader{}).WithBody(&message.BeginTxnMessageBody{}).MustBuildMutable().WithTxnContext(ctx).WithTimeTick(10).WithLastConfirmedUseMessageID().IntoImmutableMessage(walimplstest.NewTestMessageID(10))
		header := &message.UpsertMessageHeader{CollectionId: 1}
		if effects {
			header.DeleteRows = 1
		}
		body := message.NewUpsertMessageBuilderV2().WithVChannel("p_1v0").WithHeader(header).WithBody(&message.UpsertMessageBody{}).MustBuildMutable().WithTxnContext(ctx).WithTimeTick(11).WithLastConfirmedUseMessageID().IntoImmutableMessage(walimplstest.NewTestMessageID(11))
		manager := NewTxnManager(types.PChannelInfo{Name: "test"}, map[message.TxnID]*message.ImmutableTxnMessageBuilder{70: message.NewImmutableTxnMessageBuilder(message.MustAsImmutableBeginTxnMessageV2(begin)).Add(body)})
		session, err := manager.GetSessionOfTxn(70)
		require.NoError(t, err)
		require.Equal(t, !effects, session.IsInsertOnly())
		manager.Close()
	}
}

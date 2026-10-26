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

package recovery

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/pkindex/dedup"
	"github.com/milvus-io/milvus/internal/pkindex/engine"
	snpkindex "github.com/milvus-io/milvus/internal/streamingnode/server/pkindex"
	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex/index"
	pkrecovery "github.com/milvus-io/milvus/internal/streamingnode/server/pkindex/recovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

// pkTestFieldID is the primary key field of pkTestMeta.
const pkTestFieldID int64 = 100

// pkTestMeta is the catalog meta of a vchannel whose collection has an Int64
// primary key without autoID, so the primary key index recovery opens an
// index for it.
func pkTestMeta(vchannel string, collectionID int64) *streamingpb.VChannelMeta {
	return &streamingpb.VChannelMeta{
		Vchannel: vchannel,
		State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
		CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
			CollectionId: collectionID,
			Schemas: []*streamingpb.CollectionSchemaOfVChannel{{Schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
				{FieldID: pkTestFieldID, Name: "pk", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
			}}}},
		},
	}
}

// pkTestInsert builds an insert of pks into segment segmentID of vchannel at
// time tick tt, with a message id made from tt as the WAL does at the append.
func pkTestInsert(vchannel string, tt uint64, segmentID int64, pks ...int64) message.ImmutableMessage {
	id := walimplstest.NewTestMessageID(int64(tt))
	return message.NewInsertMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&message.InsertMessageHeader{
			CollectionId: 1,
			Partitions: []*message.PartitionSegmentAssignment{{
				PartitionId:       2,
				Rows:              uint64(len(pks)),
				BinarySize:        1024,
				SegmentAssignment: &message.SegmentAssignment{SegmentId: segmentID},
			}},
		}).
		WithBody(&msgpb.InsertRequest{
			Base:         &commonpb.MsgBase{MsgType: commonpb.MsgType_Insert},
			CollectionID: 1,
			PartitionID:  2,
			NumRows:      uint64(len(pks)),
			FieldsData: []*schemapb.FieldData{{
				Type:    schemapb.DataType_Int64,
				FieldId: pkTestFieldID,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: pks}},
				}},
			}},
		}).
		MustBuildMutable().
		WithTimeTick(tt).
		WithLastConfirmed(id).
		IntoImmutableMessage(id)
}

// pkTestDropCollection builds a DropCollection of vchannel at time tick tt.
func pkTestDropCollection(vchannel string, tt uint64) message.ImmutableMessage {
	id := walimplstest.NewTestMessageID(int64(tt))
	return message.NewDropCollectionMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&message.DropCollectionMessageHeader{CollectionId: 1}).
		WithBody(&msgpb.DropCollectionRequest{
			Base:         &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollection},
			CollectionID: 1,
		}).
		MustBuildMutable().
		WithTimeTick(tt).
		WithLastConfirmed(id).
		IntoImmutableMessage(id)
}

// pkTestBarrier builds the recovery barrier at time tick tt with the last
// confirmed message id lastConfirmed.
func pkTestBarrier(t *testing.T, tt uint64, lastConfirmed int64) message.ImmutableMessage {
	t.Helper()
	mutable, err := message.NewRecoveryBarrierMessageBuilderV2().WithVChannel("").
		WithHeader(&message.RecoveryBarrierMessageHeader{}).
		WithBody(&message.RecoveryBarrierMessageBody{}).BuildMutable()
	require.NoError(t, err)
	return mutable.WithTimeTick(tt).WithLastConfirmed(walimplstest.NewTestMessageID(lastConfirmed)).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(tt)))
}

// pkTestRecover recovers the primary key index of the storage over the
// vchannel v1 from the checkpoint of the storage and attaches it.
func pkTestRecover(t *testing.T, env *snpkindex.FakeEnv, rs *recoveryStorageImpl) *pkrecovery.Recovery {
	t.Helper()
	rec, err := pkrecovery.Recover(context.Background(), env.Manager, pkrecovery.Param{
		PChannelConfig: snpkindex.PChannelConfig{PChannel: rs.channel, Seed: rs.checkpoint.TimeTick},
		Metas:          map[string]*streamingpb.VChannelMeta{"v1": pkTestMeta("v1", 1)},
		Sched:          rs.nodeScheduler,
	})
	require.NoError(t, err)
	rs.pkindex = rec
	return rec
}

func TestRecoveryStoragePKIndexTailReadsTheMetrics(t *testing.T) {
	rs := newTestRecoveryStorage(t, &utility.WALCheckpoint{MessageID: walimplstest.NewTestMessageID(1), TimeTick: 10})
	t.Cleanup(rs.metrics.Close)
	// 1000 bytes observed and completed, none published: the recovery tail
	// and the publish lag are 1000 bytes.
	rs.tailController.UpdateTrackerFrontiers(1000, 1000)

	require.Equal(t, RecoveryMetrics{RecoveryTimeTick: 10, RecoveryTailBytes: 1000, PublishLagBytes: 1000}, rs.Metrics())
	rs.mu.Lock()
	tail := rs.pkindexTailLocked()
	rs.mu.Unlock()
	assert.Equal(t, snpkindex.WALTailSnapshot{Bytes: 1000, CompletedTimeTick: 10}, tail)
}

func TestRecoveryStoragePKIndexClampsTheCheckpoint(t *testing.T) {
	ctx := context.Background()
	env := snpkindex.NewFakeEnv(t, t.TempDir())
	env.Committer.SetManifest("v1", &index.FakeManifest{View: index.ManifestView{Version: 1, CheckpointTimeTick: 100, SSTBasePath: "root/1/v1"}})
	rs := newTestRecoveryStorage(t, &utility.WALCheckpoint{MessageID: walimplstest.NewTestMessageID(1), TimeTick: 80})
	rec := pkTestRecover(t, env, rs)

	rs.observeMessage(ctx, newAckTestTimeTickMessage(t, 90, 90))
	// A write after the manifest checkpoint: the index is dirty at 100.
	rs.observeMessage(ctx, pkTestInsert("v1", 120, 7, 1))
	// The time tick moves the index watermark above the checkpoint, so the
	// clamp comes from the dirty index and not from the watermark.
	rs.observeMessage(ctx, newAckTestTimeTickMessage(t, 130, 130))
	require.NoError(t, rec.Run(ctx))

	assert.Equal(t, uint64(100), rec.LastAcked())
	assert.Equal(t, uint64(130), rs.ackTracker.CompletedPoint().TimeTick, "every message is complete")
	snapshot := rs.consumeDirtySnapshot()
	require.NotNil(t, snapshot)
	assert.Equal(t, uint64(90), snapshot.Checkpoint.TimeTick, "the checkpoint does not pass the index")

	rs.closeRecoveryResources()
	assert.Nil(t, env.Manager.GetForTest(rs.channel.Name), "close releases the index")
}

func TestRecoveryStoragePKIndexIsClosedWhenTheOpenFails(t *testing.T) {
	resource.InitForTest(t)
	env := snpkindex.NewFakeEnv(t, t.TempDir())
	rs := newTestRecoveryStorage(t, &utility.WALCheckpoint{MessageID: walimplstest.NewTestMessageID(1), TimeTick: 1})
	pkTestRecover(t, env, rs)
	require.NotNil(t, env.Manager.GetForTest(rs.channel.Name))

	_, err := rs.runBoundedRecovery(context.Background(), &recordingRecoveryStreamBuilder{}, newAckTestTimeTickMessage(t, 3, 3))
	require.ErrorContains(t, err, "ended before the startup barrier")

	rs.closeRecoveryResources()
	assert.Nil(t, env.Manager.GetForTest(rs.channel.Name), "the failed open releases the index")
}

// TestRecoveryStoragePKIndexIsInTheSnapshot checks that the recovery snapshot
// hands the index of the pchannel to the interceptor.
func TestRecoveryStoragePKIndexIsInTheSnapshot(t *testing.T) {
	ctx := context.Background()
	resource.InitForTest(t)
	env := snpkindex.NewFakeEnv(t, t.TempDir())
	rs := newTestRecoveryStorage(t, &utility.WALCheckpoint{MessageID: walimplstest.NewTestMessageID(1), TimeTick: 1})
	rec := pkTestRecover(t, env, rs)

	barrier := pkTestBarrier(t, 3, 2)
	stream := &blockingRecoveryStream{ch: make(chan message.ImmutableMessage, 1), closed: make(chan struct{})}
	stream.ch <- barrier
	snapshot, err := rs.runBoundedRecovery(ctx, &recordingRecoveryStreamBuilder{stream: stream}, barrier)
	require.NoError(t, err)
	assert.Same(t, rec.Manager(), snapshot.PKIndex)

	rs.closeRecoveryResources()
	assert.Nil(t, env.Manager.GetForTest(rs.channel.Name))
}

// TestRecoveryStoragePKIndexAppliesTheLiveMessages checks that the messages
// after the recovery barrier reach the index through the same observe as the
// replay before it.
func TestRecoveryStoragePKIndexAppliesTheLiveMessages(t *testing.T) {
	ctx := context.Background()
	resource.InitForTest(t)
	env := snpkindex.NewFakeEnv(t, t.TempDir())
	rs := newTestRecoveryStorage(t, &utility.WALCheckpoint{MessageID: walimplstest.NewTestMessageID(1), TimeTick: 1})
	rec := pkTestRecover(t, env, rs)

	rs.observeMessage(ctx, pkTestInsert("v1", 2, 7, 1))
	barrier := pkTestBarrier(t, 3, 2)
	stream := &blockingRecoveryStream{ch: make(chan message.ImmutableMessage, 1), closed: make(chan struct{})}
	stream.ch <- barrier
	_, err := rs.runBoundedRecovery(ctx, &recordingRecoveryStreamBuilder{stream: stream}, barrier)
	require.NoError(t, err)
	// the live scanner observes through the same call
	rs.observeMessage(ctx, pkTestInsert("v1", 4, 8, 2))
	rs.observeMessage(ctx, newAckTestTimeTickMessage(t, 5, 5))

	idx := rec.Manager().IndexForTest("v1")
	vs, err := idx.EngineForTest().MultiGet(ctx, [][]byte{dedup.Int64PK(1).Encode(), dedup.Int64PK(2).Encode()})
	require.NoError(t, err)
	assert.NotNil(t, vs[0], "the replayed insert is in the index")
	assert.NotNil(t, vs[1], "the live insert is in the index")
	assert.Equal(t, uint64(5), rec.Manager().LastObserved())

	rs.closeRecoveryResources()
}

func TestRecoveryStoragePKIndexFinishErrorFailsTheOpen(t *testing.T) {
	ctx := context.Background()
	resource.InitForTest(t)
	env := snpkindex.NewFakeEnv(t, t.TempDir())
	rs := newTestRecoveryStorage(t, &utility.WALCheckpoint{MessageID: walimplstest.NewTestMessageID(1), TimeTick: 1})
	pkTestRecover(t, env, rs)
	// A closed engine makes the replay of the insert fail.
	require.NoError(t, env.Manager.GetForTest(rs.channel.Name).IndexForTest("v1").EngineForTest().Close())

	rs.observeMessage(ctx, pkTestInsert("v1", 2, 7, 1))
	barrier := pkTestBarrier(t, 3, 2)
	stream := &blockingRecoveryStream{ch: make(chan message.ImmutableMessage, 1), closed: make(chan struct{})}
	stream.ch <- barrier
	_, err := rs.runBoundedRecovery(ctx, &recordingRecoveryStreamBuilder{stream: stream}, barrier)
	require.Error(t, err)
	assert.True(t, errors.Is(err, engine.ErrClosed), "got %v", err)

	rs.closeRecoveryResources()
	assert.Nil(t, env.Manager.GetForTest(rs.channel.Name))
}

// TestRecoveryStoragePKIndexClosesADroppedIndexInTheObserve checks that the
// close of the index of a dropped collection, which runs after the lock of
// the storage is released, is done when observeMessage returns.
func TestRecoveryStoragePKIndexClosesADroppedIndexInTheObserve(t *testing.T) {
	ctx := context.Background()
	env := snpkindex.NewFakeEnv(t, t.TempDir())
	rs := newTestRecoveryStorage(t, &utility.WALCheckpoint{MessageID: walimplstest.NewTestMessageID(1), TimeTick: 1})
	rec := pkTestRecover(t, env, rs)
	require.NoError(t, rec.Run(ctx))
	require.NotNil(t, rec.Manager().IndexForTest("v1"))

	rs.observeMessage(ctx, pkTestDropCollection("v1", 2))
	assert.Nil(t, rec.Manager().IndexForTest("v1"))
	require.True(t, rs.mu.TryLock(), "the storage lock is released")
	rs.mu.Unlock()

	rs.closeRecoveryResources()
}

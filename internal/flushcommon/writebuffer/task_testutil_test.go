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

package writebuffer

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

// testCollectionSchema builds the standard 4-field (rowID, timestamp, pk,
// 128-dim float vector) collection schema the writebuffer suites share.
func testCollectionSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name: "test_collection",
		Fields: []*schemapb.FieldSchema{
			{
				FieldID: common.RowIDField, Name: common.RowIDFieldName, DataType: schemapb.DataType_Int64,
			},
			{
				FieldID: common.TimeStampField, Name: common.TimeStampFieldName, DataType: schemapb.DataType_Int64,
			},
			{
				FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true,
			},
			{
				FieldID: 101, Name: "vector", DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.DimKey, Value: "128"},
				},
			},
		},
	}
}

// pkVectorSchema builds the minimal 2-field (pk + 128-dim float vector) schema.
func pkVectorSchema(name string) *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name: name,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true, Name: "pk"},
			{FieldID: 101, DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{
				{Key: common.DimKey, Value: "128"},
			}},
		},
	}
}

// pkVectorTextSchema is pkVectorSchema plus a text field (fieldID 102).
func pkVectorTextSchema(name string) *schemapb.CollectionSchema {
	schema := pkVectorSchema(name)
	schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
		FieldID: 102, DataType: schemapb.DataType_Text, Name: "text",
	})
	return schema
}

// int64FieldData builds an int64 scalar column for insert-message fixtures;
// name may be empty for messages that identify fields by ID only.
func int64FieldData(fieldID int64, name string, data []int64) *schemapb.FieldData {
	return &schemapb.FieldData{
		FieldId: fieldID, FieldName: name, Type: schemapb.DataType_Int64,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{
						Data: data,
					},
				},
			},
		},
	}
}

// composeInsertMsg builds a column-based insert message for rowCount rows of the
// standard test schema; segmentID 0 leaves the segment unset (buffer-level
// tests), pkType selects an int64 or varchar primary key.
func composeInsertMsg(segmentID int64, rowCount int, dim int, pkType schemapb.DataType) ([]int64, *msgstream.InsertMsg) {
	tss := lo.RepeatBy(rowCount, func(idx int) int64 { return int64(tsoutil.ComposeTSByTimeWithLogical(time.Now(), int64(idx))) })
	vectors := lo.RepeatBy(rowCount, func(_ int) []float32 {
		return lo.RepeatBy(dim, func(_ int) float32 { return rand.Float32() })
	})
	flatten := lo.Flatten(vectors)
	var pkField *schemapb.FieldData
	switch pkType {
	case schemapb.DataType_Int64:
		pkField = int64FieldData(common.StartOfUserFieldID, "pk", tss)
	case schemapb.DataType_VarChar:
		pkField = &schemapb.FieldData{
			FieldId: common.StartOfUserFieldID, FieldName: "pk", Type: schemapb.DataType_VarChar,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: lo.Map(tss, func(v int64, _ int) string { return fmt.Sprintf("%v", v) }),
						},
					},
				},
			},
		}
	}
	return tss, &msgstream.InsertMsg{
		InsertRequest: &msgpb.InsertRequest{
			SegmentID:  segmentID,
			Version:    msgpb.InsertDataVersion_ColumnBased,
			RowIDs:     tss,
			Timestamps: lo.Map(tss, func(id int64, _ int) uint64 { return uint64(id) }),
			FieldsData: []*schemapb.FieldData{
				int64FieldData(common.RowIDField, common.RowIDFieldName, tss),
				int64FieldData(common.TimeStampField, common.TimeStampFieldName, tss),
				pkField,
				{
					FieldId: common.StartOfUserFieldID + 1, FieldName: "vector", Type: schemapb.DataType_FloatVector,
					Field: &schemapb.FieldData_Vectors{
						Vectors: &schemapb.VectorField{
							Dim: int64(dim),
							Data: &schemapb.VectorField_FloatVector{
								FloatVector: &schemapb.FloatArray{
									Data: flatten,
								},
							},
						},
					},
				},
			},
		},
	}
}

// newBufferAt builds a segment buffer whose insert payload starts at insertTs
// and whose delta buffer starts at deltaTs; 0 leaves that position unset.
func newBufferAt(t *testing.T, schema *schemapb.CollectionSchema, segmentID int64, insertTs, deltaTs uint64) *segmentBuffer {
	buf, err := newSegmentBuffer(segmentID, schema)
	require.NoError(t, err)
	if insertTs > 0 {
		buf.payload.(*ownedPayload).insertBuffer.startPos = &msgpb.MsgPosition{Timestamp: insertTs}
	}
	if deltaTs > 0 {
		buf.deltaBuffer.startPos = &msgpb.MsgPosition{Timestamp: deltaTs}
	}
	return buf
}

// newInFlightFloorBuffer builds an empty segment buffer whose payload carries
// one in-flight snapshot floor at ts — the derived pin that replaced the
// registry's "syncing segments" candidates.
func newInFlightFloorBuffer(t *testing.T, schema *schemapb.CollectionSchema, segmentID int64, ts uint64) *segmentBuffer {
	buf, err := newSegmentBuffer(segmentID, schema)
	require.NoError(t, err)
	buf.payload.(*ownedPayload).floors = []ownedFloor{
		{snapshotID: 1, startPos: &msgpb.MsgPosition{Timestamp: ts}},
	}
	return buf
}

// saveParamForTest saves a paramtable override and restores it when the current
// (sub)test finishes.
func saveParamForTest(t *testing.T, key, value string) {
	paramtable.Get().Save(key, value)
	t.Cleanup(func() { paramtable.Get().Reset(key) })
}

// enableGrowingSourceFlush flips the two switches growing-source flush needs
// (loon FFI + the feature flag) on for the duration of the test.
func enableGrowingSourceFlush(t *testing.T) {
	params := paramtable.Get()
	saveParamForTest(t, params.CommonCfg.UseLoonFFI.Key, "true")
	saveParamForTest(t, params.CommonCfg.EnableGrowingSourceFlush.Key, "true")
}

// newMockMetaCacheForTest builds the standard mock metacache: schema, collection
// ID, and no segments owing a sealed flush (GetCheckpoint derives a candidate
// from those; with a mock metacache there are none).
func newMockMetaCacheForTest(t *testing.T, schema *schemapb.CollectionSchema, collID int64) *metacache.MockMetaCache {
	mc := metacache.NewMockMetaCache(t)
	mc.EXPECT().GetSchema(mock.Anything).Return(schema).Maybe()
	mc.EXPECT().Collection().Return(collID).Maybe()
	mc.EXPECT().GetSegmentsBy(mock.Anything).Return(nil).Maybe()
	return mc
}

// fakeGrowingFlushSource is the shared growing-flush-source double: TSafe far
// ahead unless pinned, deterministic primary keys, and hook points for the
// flush/release/commit behavior a test wants to observe or stage.
type fakeGrowingFlushSource struct {
	// tsafe is the consumption watermark this double reports. Zero means "far
	// ahead of anything the tests fence on", which is the usual case; a test
	// that wants the source to look behind sets it explicitly.
	tsafe uint64
	rows  int64
	// pkRows, when set, decides how many primary keys each call returns. Needed
	// where consecutive flushes carry different row counts: the fences are
	// timestamps, so the double cannot derive the count from them.
	pkRows      func() int64
	flushFunc   func(context.Context, uint64, uint64, *syncmgr.GrowingFlushConfig) (*syncmgr.GrowingFlushResult, error)
	releaseFunc func()
	commitFunc  func(uint64)
}

func (s fakeGrowingFlushSource) TSafe() uint64 {
	if s.tsafe > 0 {
		return s.tsafe
	}
	return math.MaxUint64
}

func (s fakeGrowingFlushSource) MaterializedFieldIDs(ctx context.Context) ([]int64, error) {
	return []int64{0, 1, 100, 101, 102}, nil
}

func (s fakeGrowingFlushSource) PrimaryKeys(ctx context.Context, startTs, endTs uint64) ([]storage.PrimaryKey, error) {
	rows := s.rows
	if s.pkRows != nil {
		rows = s.pkRows()
	}
	if rows == 0 {
		rows = 10
	}
	pks := make([]storage.PrimaryKey, 0, rows)
	for i := int64(0); i < rows; i++ {
		pks = append(pks, storage.NewInt64PrimaryKey(i))
	}
	return pks, nil
}

func (s fakeGrowingFlushSource) FlushGrowingData(ctx context.Context, startTs, endTs uint64, config *syncmgr.GrowingFlushConfig) (*syncmgr.GrowingFlushResult, error) {
	if s.flushFunc != nil {
		return s.flushFunc(ctx, startTs, endTs, config)
	}
	return &syncmgr.GrowingFlushResult{
		ManifestPath:           "manifest",
		NumRows:                10,
		TimestampFrom:          100,
		TimestampTo:            200,
		ColumnGroupMemorySizes: fakeColumnGroupMemorySizes(config, 80),
		FieldNullCounts:        map[int64]int64{},
	}, nil
}

func (s fakeGrowingFlushSource) Release() {
	if s.releaseFunc != nil {
		s.releaseFunc()
	}
}

func (s fakeGrowingFlushSource) CommitGrowingFlush(flushThroughTs uint64) {
	if s.commitFunc != nil {
		s.commitFunc(flushThroughTs)
	}
}

func fakeColumnGroupMemorySizes(config *syncmgr.GrowingFlushConfig, size int64) map[int64]int64 {
	if config == nil || len(config.ColumnGroups) == 0 {
		return nil
	}
	result := make(map[int64]int64, len(config.ColumnGroups))
	for _, columnGroup := range config.ColumnGroups {
		result[columnGroup.GroupID] = size
	}
	return result
}

type fakeGrowingSourceProvider struct {
	source syncmgr.GrowingFlushSource
	state  syncmgr.GrowingSourceState
}

func (p fakeGrowingSourceProvider) GetGrowingFlushSource(int64, *msgpb.MsgPosition) (syncmgr.GrowingFlushSource, syncmgr.GrowingSourceState) {
	return p.source, p.state
}

func (p fakeGrowingSourceProvider) PrepareGrowingSourceReleaseHandoff(context.Context, uint64) error {
	return nil
}

// resolveStatic is a growing-source resolver that always reports the same
// source and state, regardless of segment or fence.
func resolveStatic(source syncmgr.GrowingFlushSource, state syncmgr.GrowingSourceState) GrowingSourceResolverFunc {
	return func(int64, *msgpb.MsgPosition) (syncmgr.GrowingFlushSource, syncmgr.GrowingSourceState) {
		return source, state
	}
}

// runGrowingTaskWithCallbacks runs a growing-source task the way the sync
// manager does — both phases, HandleError on failure, then the completion
// callbacks in order.
func runGrowingTaskWithCallbacks(ctx context.Context, task *syncmgr.GrowingSourceSyncTask, callbacks []func(error) error) error {
	err := runSyncTaskInline(ctx, task)
	if err != nil {
		task.HandleError(err)
	}
	for _, callback := range callbacks {
		err = callback(err)
	}
	return err
}

// testSyncEntryOpt adjusts the sync pack or the queue entry newTestSyncEntry
// builds, before the task wrapping the pack is created.
type testSyncEntryOpt func(*syncmgr.SyncPack, *writeBufferSyncEntry)

// newTestSyncEntry builds the standard write-buffer queue-entry fixture: a
// SyncTask whose pack names the segment and checkpoint, wrapped in an entry
// with a fresh done channel.
func newTestSyncEntry(segmentID int64, ckptTs uint64, opts ...testSyncEntryOpt) *writeBufferSyncEntry {
	pack := new(syncmgr.SyncPack).
		WithSegmentID(segmentID).
		WithCheckpoint(&msgpb.MsgPosition{Timestamp: ckptTs})
	entry := &writeBufferSyncEntry{done: make(chan struct{})}
	for _, opt := range opts {
		opt(pack, entry)
	}
	entry.task = syncmgr.NewSyncTask().WithSyncPack(pack)
	return entry
}

func entrySubmitted(_ *syncmgr.SyncPack, entry *writeBufferSyncEntry) {
	entry.submitted = true
}

func entryFailed(_ *syncmgr.SyncPack, entry *writeBufferSyncEntry) {
	entry.failed = true
}

func entryStartPosition(ts uint64) testSyncEntryOpt {
	return func(pack *syncmgr.SyncPack, _ *writeBufferSyncEntry) {
		pack.WithStartPosition(&msgpb.MsgPosition{Timestamp: ts})
	}
}

func entryBatchRows(rows int64) testSyncEntryOpt {
	return func(pack *syncmgr.SyncPack, _ *writeBufferSyncEntry) {
		pack.WithBatchRows(rows)
	}
}

// growingSegment builds the standard growing-segment fixture the write-buffer
// tests share: partition 10, growing state, empty stats.
func growingSegment(id int64, storageVersion int64) *metacache.SegmentInfo {
	return metacache.NewSegmentInfo(&datapb.SegmentInfo{
		ID:             id,
		PartitionID:    10,
		State:          commonpb.SegmentState_Growing,
		StorageVersion: storageVersion,
	}, nil, nil, metacache.NewEmptySegmentStats())
}

// runSyncTaskInline runs both phases the way the dispatcher does, for tests that
// stub the sync manager and only need the task's end-to-end effect.
func runSyncTaskInline(ctx context.Context, task syncmgr.Task) error {
	if err := task.Prepare(ctx); err != nil {
		return err
	}
	return task.Commit(ctx)
}

// getWriteBufferSyncTaskForTest reproduces getSyncTask's snapshot+build pair
// for tests that construct segments directly and bypass the claim logic:
// snapshot the payload (if a buffer exists), then build the owned-path task
// from it. Caller must hold wb.mut.
func (wb *writeBufferBase) getWriteBufferSyncTaskForTest(ctx context.Context, segmentInfo *metacache.SegmentInfo) (syncmgr.Task, error) {
	buffer := wb.buffers[segmentInfo.SegmentID()]
	var input *flushInput
	if buffer != nil {
		var err error
		input, err = buffer.payload.Snapshot(ctx, wb.checkpoint.GetTimestamp())
		if err != nil {
			return nil, err
		}
	}
	return wb.getWriteBufferSyncTask(ctx, segmentInfo, buffer, input)
}

// seedRefLedgerForTest wires a segment into growing (ref) mode the way tests
// used to inject a growingSourceProgress entry: a segmentBuffer whose payload
// is a refPayload ledger, optionally mutated before use.
func seedRefLedgerForTest(wb *writeBufferBase, segmentID int64, mutate func(p *refPayload)) *refPayload {
	wb.mut.Lock()
	defer wb.mut.Unlock()
	return seedRefLedgerForTestLocked(wb, segmentID, mutate)
}

func seedRefLedgerForTestLocked(wb *writeBufferBase, segmentID int64, mutate func(p *refPayload)) *refPayload {
	payload, ok := wb.refPayloadLocked(segmentID)
	if !ok {
		payload = newRefPayload(wb, segmentID)
		wb.buffers[segmentID] = newSegmentBufferWithPayload(segmentID, payload)
	}
	if mutate != nil {
		mutate(payload)
	}
	return payload
}

// refLedgerForTest resolves a segment's growing ledger through the buffers map.
func refLedgerForTest(wb *writeBufferBase, segmentID int64) (*refPayload, bool) {
	wb.mut.RLock()
	defer wb.mut.RUnlock()
	return wb.refPayloadLocked(segmentID)
}

// hasRefLedgerForTest reports whether a segment is tracked in growing (ref)
// mode — the old `Contains(wb.growingSourceProgress, id)` assertion.
func hasRefLedgerForTest(wb *writeBufferBase, segmentID int64) bool {
	_, ok := refLedgerForTest(wb, segmentID)
	return ok
}

// setRefLedgerBatchesForTest replaces a payload's ledger batches wholesale and
// keeps the running row tally consistent, the way production Buffer/ack do.
func setRefLedgerBatchesForTest(p *refPayload, batches []growingSourceProgressBatch) {
	p.batches = batches
	p.rowsTotal = 0
	for _, batch := range batches {
		p.rowsTotal += batch.rowNum
	}
}

// refLedgerBatchesForTest returns a segment's recorded ledger batches, nil when
// the segment is not in ref mode.
func refLedgerBatchesForTest(wb *writeBufferBase, segmentID int64) []growingSourceProgressBatch {
	if p, ok := refLedgerForTest(wb, segmentID); ok {
		return p.batches
	}
	return nil
}

// registerGrowingTaskForTest registers a queue entry for a directly-built
// growing task — what getGrowingSourceSyncTask does in production — wiring a
// refPayload snapshot record over the segment's ledger if one exists.
func registerGrowingTaskForTest(wb *writeBufferBase, task *syncmgr.GrowingSourceSyncTask) *writeBufferSyncEntry {
	wb.mut.Lock()
	defer wb.mut.Unlock()
	if entry := wb.writeBufferSyncEntryLocked(task); entry != nil {
		return entry
	}
	entry := &writeBufferSyncEntry{task: task, done: make(chan struct{}), submitted: true}
	if payload, ok := wb.refPayloadLocked(task.SegmentID()); ok {
		payload.nextSnapshotID++
		payload.snapshots[payload.nextSnapshotID] = &refSnapshot{checkpoint: task.Checkpoint()}
		entry.payload = payload
		entry.snapshotID = payload.nextSnapshotID
	}
	wb.registerWriteBufferSyncLocked(entry)
	return entry
}

// finishGrowingTaskForTest runs the unified completion path on a directly-built
// growing task, registering its queue entry first if needed.
func finishGrowingTaskForTest(wb *writeBufferBase, task *syncmgr.GrowingSourceSyncTask, taskErr error) error {
	entry := registerGrowingTaskForTest(wb, task)
	return wb.finishWriteBufferSync(context.Background(), entry, task, taskErr)
}

// earliestReplayOriginForTest is the derived replacement for the deleted
// syncCheckpoint registry probe: the earliest replay origin any segment buffer
// still pins (payload floors, ledger batches, buffered delta), nil when every
// snapshot/ledger has settled and no buffered data remains.
func earliestReplayOriginForTest(wb *writeBufferBase) *msgpb.MsgPosition {
	wb.mut.RLock()
	defer wb.mut.RUnlock()
	var earliest *msgpb.MsgPosition
	for _, buffer := range wb.buffers {
		earliest = getEarliestCheckpoint(earliest, buffer.EarliestPosition())
	}
	return earliest
}

// anyGrowingRetryArmed replaces the old channel-wide growingSourceRetryScheduled
// flag: the clock is per segment now, on the sync queue's flushIntent — the
// same debt the owned path uses — so "is a growing retry pending" is a question
// about entry-less queues that still owe.
func anyGrowingRetryArmed(wb *writeBufferBase) bool {
	wb.mut.RLock()
	defer wb.mut.RUnlock()
	for segmentID, queue := range wb.writeBufferSyncQueues {
		if _, ok := wb.refPayloadLocked(segmentID); ok && queue.intent.owes {
			return true
		}
	}
	return false
}

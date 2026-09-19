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

package metacache

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReservationSettlesOnce(t *testing.T) {
	info := &SegmentInfo{segmentID: 1, bufferRows: 100}

	r := NewSyncReservation(1, 40)
	r.Apply()(info)
	assert.Equal(t, int64(60), info.bufferRows)
	assert.Equal(t, int64(40), info.syncingRows)
	assert.Equal(t, int32(1), info.syncingTasks)
	assert.Equal(t, int64(100), info.NumOfRows())

	r.Settle(SettleCommitted)(info)
	assert.Equal(t, int64(40), info.flushedRows)
	assert.Equal(t, int64(0), info.syncingRows)
	assert.Equal(t, int32(0), info.syncingTasks)
	assert.Equal(t, int64(100), info.NumOfRows())

	// A second settle is a no-op, whatever the outcome. Every terminal path may
	// therefore call Settle unconditionally.
	r.Settle(SettleFailed)(info)
	assert.Equal(t, int64(40), info.flushedRows)
	assert.Equal(t, int64(0), info.syncingRows)
	assert.Equal(t, int32(0), info.syncingTasks)
}

func TestReservationDiscardDropsRows(t *testing.T) {
	info := &SegmentInfo{segmentID: 2, bufferRows: 10}

	r := NewSyncReservation(2, 10)
	r.Apply()(info)
	r.Settle(SettleDiscarded)(info)

	// Discarded rows are neither buffered nor flushed: the payload is gone and
	// the segment is gone with it.
	assert.Equal(t, int64(0), info.bufferRows)
	assert.Equal(t, int64(0), info.syncingRows)
	assert.Equal(t, int64(0), info.flushedRows)
	assert.Equal(t, int32(0), info.syncingTasks)
}

func TestReservationFailedDropsRowsWithoutFlushing(t *testing.T) {
	info := &SegmentInfo{segmentID: 3, bufferRows: 7}

	r := NewSyncReservation(3, 7)
	r.Apply()(info)
	r.Settle(SettleFailed)(info)

	// The payload was yielded out of the buffer and released, so it is not
	// buffered; it was not persisted, so it is not flushed. Only a WAL replay
	// can bring it back, which is why the caller keeps the checkpoint pin.
	assert.Equal(t, int64(0), info.bufferRows)
	assert.Equal(t, int64(0), info.syncingRows)
	assert.Equal(t, int64(0), info.flushedRows)
	assert.Equal(t, int32(0), info.syncingTasks)
}

func TestReservationRejectsOverReservation(t *testing.T) {
	info := &SegmentInfo{segmentID: 4, bufferRows: 5}

	r := NewSyncReservation(4, 9)
	assert.Panics(t, func() { r.Apply()(info) },
		"reserving more rows than are buffered is a caller bug, not drift to absorb")
}

func TestReservationKeepsInvariant(t *testing.T) {
	info := &SegmentInfo{segmentID: 5, bufferRows: 30}
	total := info.NumOfRows()

	a := NewSyncReservation(5, 10)
	b := NewSyncReservation(5, 20)
	a.Apply()(info)
	b.Apply()(info)
	assert.Equal(t, total, info.NumOfRows())
	assert.Equal(t, int32(2), info.syncingTasks)

	a.Settle(SettleCommitted)(info)
	assert.Equal(t, total, info.NumOfRows())

	b.Settle(SettleFailed)(info)
	// Failed rows leave the accounting entirely, so the total drops by exactly
	// that batch and nothing leaks in syncingRows.
	assert.Equal(t, int64(10), info.NumOfRows())
	assert.Equal(t, int64(0), info.syncingRows)
	assert.Equal(t, int32(0), info.syncingTasks)
}

func TestReservationRowsAndSegmentID(t *testing.T) {
	r := NewSyncReservation(77, 12)
	assert.Equal(t, int64(77), r.SegmentID())
	assert.Equal(t, int64(12), r.Rows())
}

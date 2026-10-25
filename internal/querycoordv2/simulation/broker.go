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

//go:build test

package simulation

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

// simCollection is the DataCoord-side truth of one collection.
type simCollection struct {
	id         int64
	partition  int64
	channels   []string
	segments   map[int64]*datapb.SegmentInfo // flushed (sealed) segments
	dropped    map[int64]string              // segmentID -> channel
	checkpoint map[string]uint64
	// bytesPerRow overrides Broker.BytesPerRow (wide vs narrow rows).
	bytesPerRow int64
}

// Broker is a stateful in-memory DataCoord/RootCoord. Scenarios mutate it to
// model ingestion (flush) and compaction; QueryCoord reads it via meta.Broker.
type Broker struct {
	mu          sync.RWMutex
	collections map[int64]*simCollection
	nextID      int64
	// BytesPerRow converts rows into the binlog sizes QueryCoord estimates
	// memory from.
	BytesPerRow int64
	// PChannels is the number of physical channels vchannels map onto.
	PChannels int
	// RowsSigma, when positive, draws each new segment's rows from a
	// lognormal around the requested rows (production segment sizes are
	// heavy-tailed: p90 is several times the median).
	RowsSigma float64
	// MaxSegmentRows caps what a compaction may merge into (Milvus never
	// compacts past the segment max size); 0 means no cap.
	MaxSegmentRows int64
	// Store is the shared metacache DataCoord fills in production: QueryCoord
	// resolves target segments and collection schemas from it, so the broker
	// writes every segment and collection it creates or drops there too.
	Store metacache.MetaStore

	rng             *rand.Rand
	pendingRowBytes int64
	// MemSigma, when positive, varies each segment's memory per row by a
	// lognormal factor while keeping its row count: within one table,
	// indexed vs unindexed segments, text lengths and nullable fields make
	// bytes per row differ, but QueryCoord only scores rows.
	MemSigma float64
	growing  map[string]int64 // vchannel -> unflushed rows held by its delegator
}

// PChannelName names the physical channel with the given index.
func PChannelName(index int) string {
	return fmt.Sprintf("sim-dml_%d", index)
}

func NewBroker() *Broker {
	return &Broker{
		collections: make(map[int64]*simCollection), nextID: 1_000_000, BytesPerRow: 512, PChannels: 16,
		rng: rand.New(rand.NewSource(1)), growing: make(map[string]int64),
		Store: metacache.NewMetaStore(nil),
	}
}

// putCollectionLocked publishes the collection to the shared store, as
// DataCoord does once it knows the collection.
func (b *Broker) putCollectionLocked(c *simCollection) {
	b.Store.PutCollection(&metacache.CollectionInfo{
		ID:            c.id,
		Schema:        b.schema(c.id),
		Partitions:    []int64{c.partition},
		VChannelNames: append([]string(nil), c.channels...),
		DatabaseName:  "default",
	})
}

func (b *Broker) allocLocked() int64 {
	b.nextID++
	return b.nextID
}

// CreateCollection registers a collection with numChannels vchannels and
// segmentsPerChannel flushed segments of rowsPerSegment rows each.
func (b *Broker) CreateCollection(id int64, numChannels, segmentsPerChannel int, rowsPerSegment int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	c := &simCollection{
		id:          id,
		partition:   id*100 + 1,
		segments:    make(map[int64]*datapb.SegmentInfo),
		dropped:     make(map[int64]string),
		checkpoint:  make(map[string]uint64),
		bytesPerRow: b.pendingRowBytes,
	}
	b.pendingRowBytes = 0
	for i := 0; i < numChannels; i++ {
		// Spread collections over pchannels as RootCoord does.
		ch := fmt.Sprintf("%s_%dv%d", PChannelName((int(id)+i)%max(1, b.PChannels)), id, i)
		c.channels = append(c.channels, ch)
		c.checkpoint[ch] = tsoutil.ComposeTSByTime(time.Now())
		for j := 0; j < segmentsPerChannel; j++ {
			b.addSegmentLocked(c, ch, rowsPerSegment)
		}
	}
	b.collections[id] = c
	b.putCollectionLocked(c)
}

func (b *Broker) addSegmentLocked(c *simCollection, channel string, rows int64) *datapb.SegmentInfo {
	if b.RowsSigma > 0 {
		// Truncate at 3 sigma: real size distributions have a bounded tail
		// (segment max size, compaction).
		z := math.Max(-3, math.Min(3, b.rng.NormFloat64()))
		rows = max(1, int64(float64(rows)*math.Exp(z*b.RowsSigma)))
	}
	seg := &datapb.SegmentInfo{
		ID:            b.allocLocked(),
		CollectionID:  c.id,
		PartitionID:   c.partition,
		InsertChannel: channel,
		NumOfRows:     rows,
		State:         commonpb.SegmentState_Flushed,
		Level:         datapb.SegmentLevel_L1,
		IsSorted:      true,
		Binlogs: []*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{
			EntriesNum: rows, LogSize: rows * b.rowBytes(c), MemorySize: b.segmentMemory(c, rows),
		}}}},
	}
	c.segments[seg.ID] = seg
	b.Store.PutSegment(seg)
	return seg
}

func (b *Broker) segmentMemory(c *simCollection, rows int64) int64 {
	mem := rows * b.rowBytes(c)
	if b.MemSigma > 0 {
		z := math.Max(-3, math.Min(3, b.rng.NormFloat64()))
		mem = max(1, int64(float64(mem)*math.Exp(z*b.MemSigma)))
	}
	return mem
}

// CreateCollectionChannels creates a collection whose vchannels hold
// different numbers of segments (uneven data per channel).
func (b *Broker) CreateCollectionChannels(id int64, segmentsPerChannel []int, rowsPerSegment int64) {
	b.CreateCollection(id, len(segmentsPerChannel), 0, rowsPerSegment)
	b.mu.Lock()
	defer b.mu.Unlock()
	c := b.collections[id]
	for i, n := range segmentsPerChannel {
		for j := 0; j < n; j++ {
			b.addSegmentLocked(c, c.channels[i], rowsPerSegment)
		}
	}
}

// RowBytes is the bytes per row QueryCoord estimates for the collection.
func (b *Broker) RowBytes(collectionID int64) int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	if c := b.collections[collectionID]; c != nil {
		return b.rowBytes(c)
	}
	return b.BytesPerRow
}

func (b *Broker) rowBytes(c *simCollection) int64 {
	if c.bytesPerRow > 0 {
		return c.bytesPerRow
	}
	return b.BytesPerRow
}

// CreateWideCollection is CreateCollection with a per-collection row width
// (e.g. 4096-dim vectors vs 128-dim), which QueryNodes see as memory while
// QueryCoord's score only counts rows.
func (b *Broker) CreateWideCollection(id int64, numChannels, segmentsPerChannel int, rowsPerSegment, bytesPerRow int64) {
	b.mu.Lock()
	b.pendingRowBytes = bytesPerRow
	b.mu.Unlock()
	b.CreateCollection(id, numChannels, segmentsPerChannel, rowsPerSegment)
}

// Ingest adds unflushed rows to every channel of the collection; delegators
// report them as growing rows, which QueryCoord's score counts.
func (b *Broker) Ingest(collectionID int64, rowsPerChannel int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if c := b.collections[collectionID]; c != nil {
		for _, ch := range c.channels {
			b.growing[ch] += rowsPerChannel
		}
	}
}

// GrowingBytes is the memory of a channel's unflushed rows.
func (b *Broker) GrowingBytes(collectionID int64, channel string) int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	rows := b.growing[channel]
	if rows == 0 {
		return 0
	}
	if c := b.collections[collectionID]; c != nil {
		return rows * b.rowBytes(c)
	}
	return rows * b.BytesPerRow
}

// CreateCollectionWidths creates a collection whose segments all have the
// same row count but a bytes-per-row drawn uniformly from [minBytes,
// maxBytes]: QueryCoord sees equal segments, QueryNodes do not.
func (b *Broker) CreateCollectionWidths(id int64, numChannels, segmentsPerChannel int, rows, minBytes, maxBytes int64) {
	b.CreateCollection(id, numChannels, 0, rows)
	b.mu.Lock()
	defer b.mu.Unlock()
	c := b.collections[id]
	for _, ch := range c.channels {
		for j := 0; j < segmentsPerChannel; j++ {
			width := minBytes + b.rng.Int63n(max(1, maxBytes-minBytes+1))
			seg := b.addExactSegmentLocked(c, ch, rows)
			seg.Binlogs[0].Binlogs[0].LogSize = rows * width
			seg.Binlogs[0].Binlogs[0].MemorySize = rows * width
			b.Store.PutSegment(seg)
		}
	}
}

// GrowingRows returns the unflushed rows of a channel.
func (b *Broker) GrowingRows(channel string) int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.growing[channel]
}

// FlushGrowing seals each channel's growing rows into one segment (when
// there are any) and returns the new segment IDs.
func (b *Broker) FlushGrowing(collectionID int64) []int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	c := b.collections[collectionID]
	if c == nil {
		return nil
	}
	var ids []int64
	for _, ch := range c.channels {
		if rows := b.growing[ch]; rows > 0 {
			ids = append(ids, b.addExactSegmentLocked(c, ch, rows).ID)
			b.growing[ch] = 0
			c.checkpoint[ch] = tsoutil.ComposeTSByTime(time.Now())
		}
	}
	return ids
}

func (b *Broker) addExactSegmentLocked(c *simCollection, channel string, rows int64) *datapb.SegmentInfo {
	sigma := b.RowsSigma
	b.RowsSigma = 0
	defer func() { b.RowsSigma = sigma }()
	return b.addSegmentLocked(c, channel, rows)
}

// Flush adds one new sealed segment to every channel of the collection and
// advances the channel checkpoints (a handoff for QueryCoord).
func (b *Broker) Flush(collectionID int64, rows int64) []int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	c := b.collections[collectionID]
	if c == nil {
		return nil
	}
	var ids []int64
	for _, ch := range c.channels {
		ids = append(ids, b.addSegmentLocked(c, ch, rows).ID)
		c.checkpoint[ch] = tsoutil.ComposeTSByTime(time.Now())
	}
	return ids
}

// Compact merges the n smallest segments of each channel into one segment.
func (b *Broker) Compact(collectionID int64, n int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	c := b.collections[collectionID]
	if c == nil || n < 2 {
		return
	}
	for _, ch := range c.channels {
		var segs []*datapb.SegmentInfo
		for _, s := range c.segments {
			if s.GetInsertChannel() == ch {
				segs = append(segs, s)
			}
		}
		if len(segs) < n {
			continue
		}
		sort.Slice(segs, func(i, j int) bool {
			if segs[i].GetNumOfRows() != segs[j].GetNumOfRows() {
				return segs[i].GetNumOfRows() < segs[j].GetNumOfRows()
			}
			return segs[i].GetID() < segs[j].GetID()
		})
		// Merge the n smallest, but never past the max segment size: take
		// the smallest ones that still fit, at least two of them.
		var rows int64
		var picked []*datapb.SegmentInfo
		for _, s := range segs[:n] {
			if b.MaxSegmentRows > 0 && len(picked) >= 2 && rows+s.GetNumOfRows() > b.MaxSegmentRows {
				break
			}
			rows += s.GetNumOfRows()
			picked = append(picked, s)
		}
		if len(picked) < 2 || (b.MaxSegmentRows > 0 && rows > b.MaxSegmentRows) {
			continue
		}
		for _, s := range picked {
			delete(c.segments, s.GetID())
			c.dropped[s.GetID()] = ch
			b.Store.RemoveSegment(s.GetID())
		}
		b.addSegmentLocked(c, ch, rows)
		c.checkpoint[ch] = tsoutil.ComposeTSByTime(time.Now())
	}
}

// Channels returns the vchannels of a collection.
func (b *Broker) Channels(collectionID int64) []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if c := b.collections[collectionID]; c != nil {
		return append([]string(nil), c.channels...)
	}
	return nil
}

// Partition returns the single partition of a collection.
func (b *Broker) Partition(collectionID int64) int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if c := b.collections[collectionID]; c != nil {
		return c.partition
	}
	return 0
}

// SegmentRows returns the row count of every flushed segment of a collection.
func (b *Broker) SegmentRows(collectionID int64) map[int64]int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	rows := make(map[int64]int64)
	if c := b.collections[collectionID]; c != nil {
		for id, s := range c.segments {
			rows[id] = s.GetNumOfRows()
		}
	}
	return rows
}

func (b *Broker) schema(id int64) *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name: fmt.Sprintf("sim_%d", id),
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "8"}}},
		},
	}
}

func (b *Broker) DescribeCollection(_ context.Context, collectionID int64) (*milvuspb.DescribeCollectionResponse, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	c := b.collections[collectionID]
	if c == nil {
		return nil, merr.WrapErrCollectionNotFound(collectionID)
	}
	return &milvuspb.DescribeCollectionResponse{
		Status:              merr.Success(),
		CollectionID:        collectionID,
		DbName:              "default",
		Schema:              b.schema(collectionID),
		VirtualChannelNames: append([]string(nil), c.channels...),
		ShardsNum:           int32(len(c.channels)),
	}, nil
}

func (b *Broker) GetPartitions(_ context.Context, collectionID int64) ([]int64, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if c := b.collections[collectionID]; c != nil {
		return []int64{c.partition}, nil
	}
	return nil, merr.WrapErrCollectionNotFound(collectionID)
}

func (b *Broker) GetRecoveryInfo(context.Context, int64, int64) ([]*datapb.VchannelInfo, []*datapb.SegmentBinlogs, error) {
	return nil, nil, merr.WrapErrServiceUnimplemented(fmt.Errorf("GetRecoveryInfo"))
}

func (b *Broker) ListIndexes(context.Context, int64) ([]*indexpb.IndexInfo, error) {
	return nil, nil
}

func (b *Broker) GetSegmentInfo(_ context.Context, segmentIDs ...int64) ([]*datapb.SegmentInfo, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	var infos []*datapb.SegmentInfo
	for _, id := range segmentIDs {
		for _, c := range b.collections {
			if s, ok := c.segments[id]; ok {
				infos = append(infos, s)
				break
			}
		}
	}
	if len(infos) == 0 && len(segmentIDs) > 0 {
		return nil, merr.WrapErrSegmentNotFound(segmentIDs[0])
	}
	return infos, nil
}

func (b *Broker) GetIndexInfo(_ context.Context, _ int64, _ ...int64) (map[int64][]*querypb.FieldIndexInfo, error) {
	return map[int64][]*querypb.FieldIndexInfo{}, nil
}

func (b *Broker) GetRecoveryInfoV2(_ context.Context, collectionID int64, _ ...int64) ([]*datapb.VchannelInfo, []*datapb.SegmentInfo, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	c := b.collections[collectionID]
	if c == nil {
		return nil, nil, merr.WrapErrCollectionNotFound(collectionID)
	}
	var channels []*datapb.VchannelInfo
	var segments []*datapb.SegmentInfo
	for _, ch := range c.channels {
		info := &datapb.VchannelInfo{
			CollectionID: collectionID,
			ChannelName:  ch,
			SeekPosition: &msgpb.MsgPosition{ChannelName: ch, Timestamp: c.checkpoint[ch]},
		}
		for id, s := range c.segments {
			if s.GetInsertChannel() == ch {
				info.FlushedSegmentIds = append(info.FlushedSegmentIds, id)
			}
		}
		for id, droppedCh := range c.dropped {
			if droppedCh == ch {
				info.DroppedSegmentIds = append(info.DroppedSegmentIds, id)
			}
		}
		channels = append(channels, info)
	}
	for _, s := range c.segments {
		segments = append(segments, s)
	}
	return channels, segments, nil
}

func (b *Broker) DescribeDatabase(context.Context, string) (*rootcoordpb.DescribeDatabaseResponse, error) {
	return &rootcoordpb.DescribeDatabaseResponse{Status: merr.Success(), DbName: "default"}, nil
}

func (b *Broker) GetCollectionLoadInfo(context.Context, int64) ([]string, int64, error) {
	return nil, -1, nil
}

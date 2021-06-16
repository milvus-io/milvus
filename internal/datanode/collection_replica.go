// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package datanode

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/types"
)

type Replica interface {
	getCollectionID() UniqueID
	getCollectionSchema(collectionID UniqueID, ts Timestamp) (*schemapb.CollectionSchema, error)

	// segment
	addSegment(segmentID, collID, partitionID UniqueID, channelName string) error
	removeSegment(segmentID UniqueID) error
	hasSegment(segmentID UniqueID) bool
	updateStatistics(segmentID UniqueID, numRows int64) error
	getSegmentStatisticsUpdates(segmentID UniqueID) (*internalpb.SegmentStatisticsUpdates, error)
	getSegmentByID(segmentID UniqueID) (*Segment, error)
	getChannelName(segID UniqueID) (string, error)
	setStartPositions(segmentID UniqueID, startPos []*internalpb.MsgPosition) error
	setEndPositions(segmentID UniqueID, endPos []*internalpb.MsgPosition) error
	getAllStartPositions() []*datapb.SegmentStartPosition
	getSegmentPositions(segID UniqueID) ([]*internalpb.MsgPosition, []*internalpb.MsgPosition)
	listOpenSegmentCheckPointAndNumRows(segs []UniqueID) (map[UniqueID]internalpb.MsgPosition, map[UniqueID]int64)
}

// Segment is the data structure of segments in data node replica.
type Segment struct {
	segmentID    UniqueID
	collectionID UniqueID
	partitionID  UniqueID
	numRows      int64
	memorySize   int64
	isNew        atomic.Value // bool
	channelName  string
	field2Paths  map[UniqueID][]string // fieldID to binlog paths, only auto-flushed paths will be buffered.
}

// CollectionSegmentReplica is the data replication of persistent data in datanode.
// It implements `Replica` interface.
type CollectionSegmentReplica struct {
	mu           sync.RWMutex
	collectionID UniqueID
	collSchema   *schemapb.CollectionSchema

	segments    map[UniqueID]*Segment
	metaService *metaService

	posMu          sync.Mutex
	startPositions map[UniqueID][]*internalpb.MsgPosition
	endPositions   map[UniqueID][]*internalpb.MsgPosition
}

var _ Replica = &CollectionSegmentReplica{}

func newReplica(ms types.MasterService, collectionID UniqueID) Replica {
	metaService := newMetaService(ms, collectionID)
	segments := make(map[UniqueID]*Segment)

	var replica Replica = &CollectionSegmentReplica{
		segments:       segments,
		collectionID:   collectionID,
		metaService:    metaService,
		startPositions: make(map[UniqueID][]*internalpb.MsgPosition),
		endPositions:   make(map[UniqueID][]*internalpb.MsgPosition),
	}
	return replica
}

func (replica *CollectionSegmentReplica) getChannelName(segID UniqueID) (string, error) {
	replica.mu.RLock()
	defer replica.mu.RUnlock()

	seg, ok := replica.segments[segID]
	if !ok {
		return "", fmt.Errorf("Cannot find segment, id = %v", segID)
	}

	return seg.channelName, nil
}

func (replica *CollectionSegmentReplica) getSegmentByID(segmentID UniqueID) (*Segment, error) {
	replica.mu.RLock()
	defer replica.mu.RUnlock()

	if seg, ok := replica.segments[segmentID]; ok {
		return seg, nil
	}
	return nil, fmt.Errorf("Cannot find segment, id = %v", segmentID)

}

// `addSegment` add a new segment into replica when data node see the segment
func (replica *CollectionSegmentReplica) addSegment(
	segmentID UniqueID,
	collID UniqueID,
	partitionID UniqueID,
	channelName string) error {

	replica.mu.Lock()
	defer replica.mu.Unlock()
	log.Debug("Add Segment", zap.Int64("Segment ID", segmentID))

	seg := &Segment{
		segmentID:    segmentID,
		collectionID: collID,
		partitionID:  partitionID,
		channelName:  channelName,
		field2Paths:  make(map[UniqueID][]string),
	}

	seg.isNew.Store(true)

	replica.segments[segmentID] = seg
	return nil
}

func (replica *CollectionSegmentReplica) getAllStartPositions() []*datapb.SegmentStartPosition {
	replica.mu.RLock()
	defer replica.mu.RUnlock()

	result := make([]*datapb.SegmentStartPosition, 0, len(replica.segments))
	for id, seg := range replica.segments {

		if seg.isNew.Load().(bool) {

			pos, ok := replica.startPositions[id]
			if !ok {
				log.Warn("Segment has no start positions")
				continue
			}

			result = append(result, &datapb.SegmentStartPosition{
				SegmentID:     id,
				StartPosition: pos[0],
			})
			seg.isNew.Store(false)
		}

	}
	return result
}

func (replica *CollectionSegmentReplica) removeSegment(segmentID UniqueID) error {
	replica.mu.Lock()
	delete(replica.segments, segmentID)
	replica.mu.Unlock()

	replica.posMu.Lock()
	delete(replica.startPositions, segmentID)
	delete(replica.endPositions, segmentID)
	replica.posMu.Unlock()

	return nil
}

func (replica *CollectionSegmentReplica) hasSegment(segmentID UniqueID) bool {
	replica.mu.RLock()
	defer replica.mu.RUnlock()

	_, ok := replica.segments[segmentID]
	return ok
}

// `updateStatistics` updates the number of rows of a segment in replica.
func (replica *CollectionSegmentReplica) updateStatistics(segmentID UniqueID, numRows int64) error {
	replica.mu.Lock()
	defer replica.mu.Unlock()

	if seg, ok := replica.segments[segmentID]; ok {
		log.Debug("updating segment", zap.Int64("Segment ID", segmentID), zap.Int64("numRows", numRows))
		seg.memorySize = 0
		seg.numRows += numRows
		return nil
	}

	return fmt.Errorf("There's no segment %v", segmentID)
}

// `getSegmentStatisticsUpdates` gives current segment's statistics updates.
func (replica *CollectionSegmentReplica) getSegmentStatisticsUpdates(segmentID UniqueID) (*internalpb.SegmentStatisticsUpdates, error) {
	replica.mu.Lock()
	defer replica.mu.Unlock()

	if seg, ok := replica.segments[segmentID]; ok {
		updates := &internalpb.SegmentStatisticsUpdates{
			SegmentID:  segmentID,
			MemorySize: seg.memorySize,
			NumRows:    seg.numRows,
		}

		return updates, nil
	}
	return nil, fmt.Errorf("Error, there's no segment %v", segmentID)
}

// --- collection ---

func (replica *CollectionSegmentReplica) getCollectionID() UniqueID {
	return replica.collectionID
}

// getCollectionSchema will get collection schema from masterservice for a certain time.
// If you want the latest collection schema, ts should be 0
func (replica *CollectionSegmentReplica) getCollectionSchema(collID UniqueID, ts Timestamp) (*schemapb.CollectionSchema, error) {
	replica.mu.Lock()
	defer replica.mu.Unlock()

	if !replica.validCollection(collID) {
		log.Error("Illegal Collection for the replica")
		return nil, fmt.Errorf("Not supported collection %v", collID)
	}

	sch, err := replica.metaService.getCollectionSchema(context.Background(), collID, ts)
	if err != nil {
		log.Error("Grpc error", zap.Error(err))
		return nil, err
	}

	return sch, nil
}

func (replica *CollectionSegmentReplica) validCollection(collID UniqueID) bool {
	return collID == replica.collectionID
}

// setStartPositions set segment `Start Position` - means the `startPositions` from the MsgPack when segment is first found
func (replica *CollectionSegmentReplica) setStartPositions(segID UniqueID, startPositions []*internalpb.MsgPosition) error {
	replica.posMu.Lock()
	defer replica.posMu.Unlock()
	replica.startPositions[segID] = startPositions
	return nil
}

// setEndPositions set segment `End Position` - means the `endPositions` from the MsgPack when segment need to be flushed
func (replica *CollectionSegmentReplica) setEndPositions(segID UniqueID, endPositions []*internalpb.MsgPosition) error {
	replica.posMu.Lock()
	defer replica.posMu.Unlock()
	replica.endPositions[segID] = endPositions
	return nil
}

// getSegmentPositions returns stored segment start-end Positions
// To te Noted: start/end positions are NOT start&end position from one single MsgPack, they are from different MsgPack!
// see setStartPositions, setEndPositions comment
func (replica *CollectionSegmentReplica) getSegmentPositions(segID UniqueID) ([]*internalpb.MsgPosition, []*internalpb.MsgPosition) {
	replica.posMu.Lock()
	defer replica.posMu.Unlock()
	startPos := replica.startPositions[segID]
	endPos := replica.endPositions[segID]
	return startPos, endPos
}

func (replica *CollectionSegmentReplica) listOpenSegmentCheckPointAndNumRows(segs []UniqueID) (map[UniqueID]internalpb.MsgPosition, map[UniqueID]int64) {
	replica.posMu.Lock()
	defer replica.posMu.Unlock()
	r1 := make(map[UniqueID]internalpb.MsgPosition)
	r2 := make(map[UniqueID]int64)
	for _, seg := range segs {
		r1[seg] = *replica.endPositions[seg][0]
		r2[seg] = replica.segments[seg].numRows
	}
	return r1, r2
}

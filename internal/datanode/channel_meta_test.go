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

package datanode

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"path"
	"testing"
	"time"

	bloom "github.com/bits-and-blooms/bloom/v3"
	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/metautil"
)

var channelMetaNodeTestDir = "/tmp/milvus_test/channel_meta"

func TestNewChannel(t *testing.T) {
	rc := &RootCoordFactory{}
	cm := storage.NewLocalChunkManager(storage.RootPath(channelMetaNodeTestDir))
	defer cm.RemoveWithPrefix(context.Background(), cm.RootPath())
	channel := newChannel("channel", 0, nil, rc, cm)
	assert.NotNil(t, channel)
}

type mockDataCM struct {
	storage.ChunkManager
}

func (kv *mockDataCM) MultiRead(ctx context.Context, keys []string) ([][]byte, error) {
	stats := &storage.PrimaryKeyStats{
		FieldID: common.RowIDField,
		Min:     0,
		Max:     10,
		BF:      bloom.NewWithEstimates(storage.BloomFilterSize, storage.MaxBloomFalsePositive),
	}
	buffer, _ := json.Marshal(stats)
	return [][]byte{buffer}, nil
}

type mockPkfilterMergeError struct {
	storage.ChunkManager
}

func (kv *mockPkfilterMergeError) MultiRead(ctx context.Context, keys []string) ([][]byte, error) {
	/*
		stats := &storage.PrimaryKeyStats{
			FieldID: common.RowIDField,
			Min:     0,
			Max:     10,
			BF:      bloom.NewWithEstimates(1, 0.0001),
		}
		buffer, _ := json.Marshal(stats)
		return [][]byte{buffer}, nil*/
	return nil, errors.New("mocked multi read error")
}

type mockDataCMError struct {
	storage.ChunkManager
}

func (kv *mockDataCMError) MultiRead(ctx context.Context, keys []string) ([][]byte, error) {
	return nil, fmt.Errorf("mock error")
}

type mockDataCMStatsError struct {
	storage.ChunkManager
}

func (kv *mockDataCMStatsError) MultiRead(ctx context.Context, keys []string) ([][]byte, error) {
	return [][]byte{[]byte("3123123,error,test")}, nil
}

func getSimpleFieldBinlog() *datapb.FieldBinlog {
	return &datapb.FieldBinlog{
		FieldID: 106,
		Binlogs: []*datapb.Binlog{{LogPath: "test"}},
	}
}

func TestChannelMeta_InnerFunction(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rc := &RootCoordFactory{
		pkType: schemapb.DataType_Int64,
	}

	var (
		collID  = UniqueID(1)
		cm      = storage.NewLocalChunkManager(storage.RootPath(channelMetaNodeTestDir))
		channel = newChannel("insert-01", collID, nil, rc, cm)
	)
	defer cm.RemoveWithPrefix(ctx, cm.RootPath())

	require.False(t, channel.hasSegment(0, true))
	require.False(t, channel.hasSegment(0, false))

	var err error

	startPos := &msgpb.MsgPosition{ChannelName: "insert-01", Timestamp: Timestamp(100)}
	endPos := &msgpb.MsgPosition{ChannelName: "insert-01", Timestamp: Timestamp(200)}
	err = channel.addSegment(
		addSegmentReq{
			segType:     datapb.SegmentType_New,
			segID:       0,
			collID:      1,
			partitionID: 2,
			startPos:    startPos,
			endPos:      endPos,
		})
	assert.NoError(t, err)
	assert.True(t, channel.hasSegment(0, true))

	seg, ok := channel.segments[UniqueID(0)]
	assert.True(t, ok)
	require.NotNil(t, seg)
	assert.Equal(t, UniqueID(0), seg.segmentID)
	assert.Equal(t, UniqueID(1), seg.collectionID)
	assert.Equal(t, UniqueID(2), seg.partitionID)
	assert.Equal(t, Timestamp(100), seg.startPos.Timestamp)
	assert.Equal(t, int64(0), seg.numRows)
	assert.Equal(t, datapb.SegmentType_New, seg.getType())

	channel.updateSegmentRowNumber(0, 10)
	assert.Equal(t, int64(10), seg.numRows)
	channel.updateSegmentMemorySize(0, 10)
	assert.Equal(t, int64(10), seg.memorySize)

	segPos := channel.listNewSegmentsStartPositions()
	assert.Equal(t, 1, len(segPos))
	assert.Equal(t, UniqueID(0), segPos[0].SegmentID)
	assert.Equal(t, "insert-01", segPos[0].StartPosition.ChannelName)
	assert.Equal(t, Timestamp(100), segPos[0].StartPosition.Timestamp)

	channel.transferNewSegments(lo.Map(segPos, func(pos *datapb.SegmentStartPosition, _ int) UniqueID {
		return pos.GetSegmentID()
	}))

	updates, err := channel.getSegmentStatisticsUpdates(0)
	assert.NoError(t, err)
	assert.Equal(t, int64(10), updates.NumRows)

	totalSegments := channel.filterSegments(common.InvalidPartitionID)
	assert.Equal(t, len(totalSegments), 1)
}

// TODO GOOSE
func TestChannelMeta_getChannelName(t *testing.T) {
	t.Skip()
}

func TestChannelMeta_getCollectionAndPartitionID(t *testing.T) {
	tests := []struct {
		segID   UniqueID
		segType datapb.SegmentType

		inCollID    UniqueID
		inParID     UniqueID
		description string
	}{
		{100, datapb.SegmentType_New, 1, 10, "Segment 100 of type New"},
		{200, datapb.SegmentType_Normal, 2, 20, "Segment 200 of type Normal"},
		{300, datapb.SegmentType_Flushed, 3, 30, "Segment 300 of type Flushed"},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			seg := Segment{
				collectionID: test.inCollID,
				partitionID:  test.inParID,
				segmentID:    test.segID,
			}
			seg.setType(test.segType)
			channel := &ChannelMeta{
				segments: map[UniqueID]*Segment{
					test.segID: &seg},
			}

			collID, parID, err := channel.getCollectionAndPartitionID(test.segID)
			assert.NoError(t, err)
			assert.Equal(t, test.inCollID, collID)
			assert.Equal(t, test.inParID, parID)
		})
	}
}

func TestChannelMeta_segmentFlushed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rc := &RootCoordFactory{
		pkType: schemapb.DataType_Int64,
	}
	collID := UniqueID(1)
	cm := storage.NewLocalChunkManager(storage.RootPath(channelMetaNodeTestDir))
	defer cm.RemoveWithPrefix(ctx, cm.RootPath())

	t.Run("Test coll mot match", func(t *testing.T) {
		channel := newChannel("channel", collID, nil, rc, cm)
		err := channel.addSegment(
			addSegmentReq{
				segType:     datapb.SegmentType_New,
				segID:       1,
				collID:      collID + 1,
				partitionID: 0,
				startPos:    nil,
				endPos:      nil,
			})
		assert.Error(t, err)
	})

	t.Run("Test segmentFlushed", func(t *testing.T) {
		channel := &ChannelMeta{
			segments: make(map[UniqueID]*Segment),
		}

		type Test struct {
			inSegType datapb.SegmentType
			inSegID   UniqueID
		}

		tests := []Test{
			// new segment
			{datapb.SegmentType_New, 1},
			{datapb.SegmentType_New, 2},
			{datapb.SegmentType_New, 3},
			// normal segment
			{datapb.SegmentType_Normal, 10},
			{datapb.SegmentType_Normal, 20},
			{datapb.SegmentType_Normal, 30},
			// flushed segment
			{datapb.SegmentType_Flushed, 100},
			{datapb.SegmentType_Flushed, 200},
			{datapb.SegmentType_Flushed, 300},
		}

		newSeg := func(channel *ChannelMeta, sType datapb.SegmentType, id UniqueID) {
			s := Segment{segmentID: id}
			s.setType(sType)
			channel.segments[id] = &s
		}

		for _, test := range tests {
			// prepare case
			newSeg(channel, test.inSegType, test.inSegID)

			channel.segmentFlushed(test.inSegID)
			flushedSeg, ok := channel.segments[test.inSegID]
			assert.True(t, ok)
			assert.Equal(t, test.inSegID, flushedSeg.segmentID)
			assert.Equal(t, datapb.SegmentType_Flushed, flushedSeg.getType())
		}
	})
}

func TestChannelMeta_InterfaceMethod(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rc := &RootCoordFactory{
		pkType: schemapb.DataType_Int64,
	}
	cm := storage.NewLocalChunkManager(storage.RootPath(channelMetaNodeTestDir))
	defer cm.RemoveWithPrefix(ctx, cm.RootPath())

	t.Run("Test addFlushedSegmentWithPKs", func(t *testing.T) {
		tests := []struct {
			isvalid bool

			incollID      UniqueID
			channelCollID UniqueID
			description   string
		}{
			{true, 1, 1, "valid input collection"},
			{false, 1, 2, "invalid input collection"},
		}

		primaryKeyData := &storage.Int64FieldData{
			Data: []int64{9},
		}
		for _, test := range tests {
			t.Run(test.description, func(t *testing.T) {
				channel := newChannel("a", test.channelCollID, nil, rc, cm)
				if test.isvalid {
					channel.addFlushedSegmentWithPKs(100, test.incollID, 10, 1, primaryKeyData)

					assert.True(t, channel.hasSegment(100, true))
					assert.False(t, channel.hasSegment(100, false))
				} else {
					channel.addFlushedSegmentWithPKs(100, test.incollID, 10, 1, primaryKeyData)
					assert.False(t, channel.hasSegment(100, true))
					assert.False(t, channel.hasSegment(100, false))
				}
			})
		}
	})

	t.Run("Test_addNewSegment", func(t *testing.T) {
		tests := []struct {
			isValidCase   bool
			channelCollID UniqueID
			inCollID      UniqueID
			inSegID       UniqueID

			instartPos *msgpb.MsgPosition

			expectedSegType datapb.SegmentType

			description string
		}{
			{isValidCase: false, channelCollID: 1, inCollID: 2, inSegID: 300, description: "input CollID 2 mismatch with channel collID"},
			{true, 1, 1, 200, new(msgpb.MsgPosition), datapb.SegmentType_New, "nill address for startPos"},
			{true, 1, 1, 200, &msgpb.MsgPosition{}, datapb.SegmentType_New, "empty struct for startPos"},
		}

		for _, test := range tests {
			t.Run(test.description, func(t *testing.T) {
				channel := newChannel("a", test.channelCollID, nil, rc, cm)
				require.False(t, channel.hasSegment(test.inSegID, true))
				err := channel.addSegment(
					addSegmentReq{
						segType:     datapb.SegmentType_New,
						segID:       test.inSegID,
						collID:      test.inCollID,
						partitionID: 1,
						startPos:    test.instartPos,
						endPos:      &msgpb.MsgPosition{},
					})
				if test.isValidCase {
					assert.NoError(t, err)
					assert.True(t, channel.hasSegment(test.inSegID, true))

					seg, ok := channel.segments[test.inSegID]
					assert.True(t, ok)
					assert.Equal(t, test.expectedSegType, seg.getType())
				} else {
					assert.Error(t, err)
					assert.False(t, channel.hasSegment(test.inSegID, true))
				}
			})
		}
	})

	t.Run("Test_addNormalSegment", func(t *testing.T) {
		tests := []struct {
			isValidCase   bool
			channelCollID UniqueID
			inCollID      UniqueID
			inSegID       UniqueID

			expectedSegType datapb.SegmentType

			description string
		}{
			{isValidCase: false, channelCollID: 1, inCollID: 2, inSegID: 300, description: "input CollID 2 mismatch with channel collID"},
			{true, 1, 1, 200, datapb.SegmentType_Normal, "normal case"},
		}

		for _, test := range tests {
			t.Run(test.description, func(t *testing.T) {
				channel := newChannel("a", test.channelCollID, nil, rc, &mockDataCM{})
				require.False(t, channel.hasSegment(test.inSegID, true))
				err := channel.addSegment(
					addSegmentReq{
						segType:      datapb.SegmentType_Normal,
						segID:        test.inSegID,
						collID:       test.inCollID,
						partitionID:  1,
						numOfRows:    0,
						statsBinLogs: []*datapb.FieldBinlog{getSimpleFieldBinlog()},
						recoverTs:    0,
					})
				if test.isValidCase {
					assert.NoError(t, err)
					assert.True(t, channel.hasSegment(test.inSegID, true))

					seg, ok := channel.segments[test.inSegID]
					assert.True(t, ok)
					assert.Equal(t, test.expectedSegType, seg.getType())
				} else {
					assert.Error(t, err)
					assert.False(t, channel.hasSegment(test.inSegID, true))
				}
			})
		}
	})

	t.Run("Test_addNormalSegmentWithNilDml", func(t *testing.T) {
		channel := newChannel("a", 1, nil, rc, &mockDataCM{})
		segID := int64(101)
		require.False(t, channel.hasSegment(segID, true))
		assert.NotPanics(t, func() {
			err := channel.addSegment(
				addSegmentReq{
					segType:      datapb.SegmentType_Normal,
					segID:        segID,
					collID:       1,
					partitionID:  10,
					numOfRows:    0,
					statsBinLogs: []*datapb.FieldBinlog{},
					recoverTs:    0,
				})
			assert.NoError(t, err)
		})
	})

	t.Run("Test_getCollectionSchema", func(t *testing.T) {
		tests := []struct {
			isValid       bool
			channelCollID UniqueID
			inputCollID   UniqueID

			metaServiceErr bool
			description    string
		}{
			{true, 1, 1, false, "Normal case"},
			{false, 1, 2, false, "Input collID 2 mismatch with channel collID 1"},
			{false, 1, 1, true, "RPC call fails"},
		}

		for _, test := range tests {
			t.Run(test.description, func(t *testing.T) {
				channel := newChannel("a", test.channelCollID, nil, rc, cm)

				if test.metaServiceErr {
					channel.collSchema = nil
					rc.setCollectionID(-1)
				} else {
					rc.setCollectionID(1)
				}

				s, err := channel.getCollectionSchema(test.inputCollID, Timestamp(0))
				if test.isValid {
					assert.NoError(t, err)
					assert.NotNil(t, s)
				} else {
					assert.Error(t, err)
					assert.Nil(t, s)
				}
			})
		}
		rc.setCollectionID(1)
	})

	t.Run("Test listAllSegmentIDs", func(t *testing.T) {
		s1 := Segment{segmentID: 1}
		s2 := Segment{segmentID: 2}
		s3 := Segment{segmentID: 3}

		s1.setType(datapb.SegmentType_New)
		s2.setType(datapb.SegmentType_Normal)
		s3.setType(datapb.SegmentType_Flushed)
		channel := &ChannelMeta{
			segments: map[UniqueID]*Segment{
				1: &s1,
				2: &s2,
				3: &s3,
			},
		}

		ids := channel.listAllSegmentIDs()
		assert.ElementsMatch(t, []UniqueID{1, 2, 3}, ids)
	})

	t.Run("Test listPartitionSegments", func(t *testing.T) {
		channel := &ChannelMeta{segments: make(map[UniqueID]*Segment)}
		segs := []struct {
			segID   UniqueID
			partID  UniqueID
			segType datapb.SegmentType
		}{
			{1, 1, datapb.SegmentType_New},
			{2, 1, datapb.SegmentType_Normal},
			{3, 1, datapb.SegmentType_Flushed},
			{4, 2, datapb.SegmentType_New},
			{5, 2, datapb.SegmentType_Normal},
			{6, 2, datapb.SegmentType_Flushed},
		}

		for _, seg := range segs {
			s := Segment{
				segmentID:   seg.segID,
				partitionID: seg.partID,
			}

			s.setType(seg.segType)
			channel.segments[seg.segID] = &s
		}

		ids := channel.listPartitionSegments(1)
		assert.ElementsMatch(t, []UniqueID{1, 2, 3}, ids)
	})

	t.Run("Test_addSegmentMinIOLoadError", func(t *testing.T) {
		channel := newChannel("a", 1, nil, rc, cm)
		channel.chunkManager = &mockDataCMError{}

		err := channel.addSegment(
			addSegmentReq{
				segType:      datapb.SegmentType_Normal,
				segID:        1,
				collID:       1,
				partitionID:  2,
				numOfRows:    int64(10),
				statsBinLogs: []*datapb.FieldBinlog{getSimpleFieldBinlog()},
				recoverTs:    0,
			})
		assert.Error(t, err)
		err = channel.addSegment(
			addSegmentReq{
				segType:      datapb.SegmentType_Flushed,
				segID:        1,
				collID:       1,
				partitionID:  2,
				numOfRows:    int64(0),
				statsBinLogs: []*datapb.FieldBinlog{getSimpleFieldBinlog()},
				recoverTs:    0,
			})
		assert.Error(t, err)
	})

	t.Run("Test_addSegmentStatsError", func(t *testing.T) {
		channel := newChannel("insert-01", 1, nil, rc, cm)
		channel.chunkManager = &mockDataCMStatsError{}
		var err error

		err = channel.addSegment(
			addSegmentReq{
				segType:      datapb.SegmentType_Normal,
				segID:        1,
				collID:       1,
				partitionID:  2,
				numOfRows:    int64(10),
				statsBinLogs: []*datapb.FieldBinlog{getSimpleFieldBinlog()},
				recoverTs:    0,
			})
		assert.Error(t, err)
		err = channel.addSegment(
			addSegmentReq{
				segType:      datapb.SegmentType_Flushed,
				segID:        1,
				collID:       1,
				partitionID:  2,
				numOfRows:    int64(0),
				statsBinLogs: []*datapb.FieldBinlog{getSimpleFieldBinlog()},
				recoverTs:    0,
			})
		assert.Error(t, err)
	})

	t.Run("Test_addSegmentPkfilterError", func(t *testing.T) {
		channel := newChannel("insert-01", 1, nil, rc, cm)
		channel.chunkManager = &mockPkfilterMergeError{}
		var err error

		err = channel.addSegment(
			addSegmentReq{
				segType:      datapb.SegmentType_Normal,
				segID:        1,
				collID:       1,
				partitionID:  2,
				numOfRows:    int64(10),
				statsBinLogs: []*datapb.FieldBinlog{getSimpleFieldBinlog()},
				recoverTs:    0,
			})
		assert.Error(t, err)
		err = channel.addSegment(
			addSegmentReq{
				segType:      datapb.SegmentType_Flushed,
				segID:        1,
				collID:       1,
				partitionID:  2,
				numOfRows:    int64(0),
				statsBinLogs: []*datapb.FieldBinlog{getSimpleFieldBinlog()},
				recoverTs:    0,
			})
		assert.Error(t, err)
	})

	t.Run("Test_mergeFlushedSegments", func(t *testing.T) {
		channel := newChannel("channel", 1, nil, rc, cm)

		primaryKeyData := &storage.Int64FieldData{
			Data: []UniqueID{1},
		}
		tests := []struct {
			description string
			isValid     bool
			stored      bool

			inCompactedFrom []UniqueID
			expectedFrom    []UniqueID
			inSeg           *Segment
		}{
			{"mismatch collection", false, false, []UniqueID{1, 2}, []UniqueID{1, 2}, &Segment{
				segmentID:    3,
				collectionID: -1,
			}},
			{"no match flushed segment", true, true, []UniqueID{1, 6}, []UniqueID{1}, &Segment{
				segmentID:    3,
				collectionID: 1,
				numRows:      15,
			}},
			{"numRows==0", true, false, []UniqueID{1, 2}, []UniqueID{1, 2}, &Segment{
				segmentID:    3,
				collectionID: 1,
				numRows:      0,
			}},
			{"numRows>0", true, true, []UniqueID{1, 2}, []UniqueID{1, 2}, &Segment{
				segmentID:    3,
				collectionID: 1,
				numRows:      15,
			}},
			{"segment exists but not flushed", true, true, []UniqueID{1, 4}, []UniqueID{1}, &Segment{
				segmentID:    3,
				collectionID: 1,
				numRows:      15,
			}},
		}

		for _, test := range tests {
			t.Run(test.description, func(t *testing.T) {
				// prepare segment
				if !channel.hasSegment(1, true) {
					channel.addFlushedSegmentWithPKs(1, 1, 0, 10, primaryKeyData)
				}

				if !channel.hasSegment(2, true) {
					channel.addFlushedSegmentWithPKs(2, 1, 0, 10, primaryKeyData)
				}

				if !channel.hasSegment(4, false) {
					channel.removeSegments(4)
					channel.addSegment(addSegmentReq{
						segType:     datapb.SegmentType_Normal,
						segID:       4,
						collID:      1,
						partitionID: 0,
					})
				}

				if channel.hasSegment(3, true) {
					channel.removeSegments(3)
				}

				require.True(t, channel.hasSegment(1, true))
				require.True(t, channel.hasSegment(2, true))
				require.True(t, channel.hasSegment(4, false))
				require.False(t, channel.hasSegment(3, true))

				// tests start
				err := channel.mergeFlushedSegments(context.Background(), test.inSeg, 100, test.inCompactedFrom)
				if test.isValid {
					assert.NoError(t, err)
				} else {
					assert.Error(t, err)
				}

				if test.stored {
					assert.True(t, channel.hasSegment(3, true))

					to2from := channel.listCompactedSegmentIDs()
					assert.NotEmpty(t, to2from)

					from, ok := to2from[3]
					assert.True(t, ok)
					assert.ElementsMatch(t, test.expectedFrom, from)
				} else {
					assert.False(t, channel.hasSegment(3, true))
				}

			})
		}
	})

}

func TestChannelMeta_loadStats(t *testing.T) {
	f := &MetaFactory{}
	rc := &RootCoordFactory{
		pkType: schemapb.DataType_Int64,
	}

	t.Run("list with merged stats log", func(t *testing.T) {
		meta := f.GetCollectionMeta(UniqueID(10001), "test_load_stats", schemapb.DataType_Int64)
		// load normal stats log
		seg1 := &Segment{
			segmentID:   1,
			partitionID: 2,
		}

		// load flushed stats log
		seg2 := &Segment{
			segmentID:   2,
			partitionID: 2,
		}

		//gen pk stats bytes
		stats := storage.NewPrimaryKeyStats(106, int64(schemapb.DataType_Int64), 10)
		iCodec := storage.NewInsertCodecWithSchema(meta)

		cm := &mockCm{}

		channel := newChannel("channel", 1, meta.Schema, rc, cm)
		channel.segments[seg1.segmentID] = seg1
		channel.segments[seg2.segmentID] = seg2

		// load normal stats log
		blob, err := iCodec.SerializePkStats(stats, 10)
		assert.NoError(t, err)
		cm.MultiReadReturn = [][]byte{blob.Value}

		_, err = channel.loadStats(
			context.Background(), seg1.segmentID, 1,
			[]*datapb.FieldBinlog{{
				FieldID: 106,
				Binlogs: []*datapb.Binlog{{
					//<StatsLogPath>/<collectionID>/<partitionID>/<segmentID>/<FieldID>/<logIdx>
					LogPath: path.Join(common.SegmentStatslogPath, metautil.JoinIDPath(1, 2, 1, 106, 10)),
				}}}}, 0)
		assert.NoError(t, err)

		// load flushed stats log
		blob, err = iCodec.SerializePkStatsList([]*storage.PrimaryKeyStats{stats}, 10)
		assert.NoError(t, err)
		cm.MultiReadReturn = [][]byte{blob.Value}

		_, err = channel.loadStats(
			context.Background(), seg2.segmentID, 1,
			[]*datapb.FieldBinlog{{
				FieldID: 106,
				Binlogs: []*datapb.Binlog{{
					//<StatsLogPath>/<collectionID>/<partitionID>/<segmentID>/<FieldID>/<logIdx>
					LogPath: path.Join(common.SegmentStatslogPath, metautil.JoinIDPath(1, 2, 2, 106), storage.CompoundStatsType.LogIdx()),
				}}}}, 0)
		assert.NoError(t, err)
	})
}

func TestChannelMeta_UpdatePKRange(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rc := &RootCoordFactory{
		pkType: schemapb.DataType_Int64,
	}
	collID := UniqueID(1)
	partID := UniqueID(2)
	chanName := "insert-02"
	startPos := &msgpb.MsgPosition{ChannelName: chanName, Timestamp: Timestamp(100)}
	endPos := &msgpb.MsgPosition{ChannelName: chanName, Timestamp: Timestamp(200)}

	cm := storage.NewLocalChunkManager(storage.RootPath(channelMetaNodeTestDir))
	defer cm.RemoveWithPrefix(ctx, cm.RootPath())
	channel := newChannel("chanName", collID, nil, rc, cm)
	channel.chunkManager = &mockDataCM{}

	err := channel.addSegment(
		addSegmentReq{
			segType:     datapb.SegmentType_New,
			segID:       1,
			collID:      collID,
			partitionID: partID,
			startPos:    startPos,
			endPos:      endPos,
		})
	assert.NoError(t, err)
	err = channel.addSegment(
		addSegmentReq{
			segType:      datapb.SegmentType_Normal,
			segID:        2,
			collID:       collID,
			partitionID:  partID,
			numOfRows:    100,
			statsBinLogs: []*datapb.FieldBinlog{getSimpleFieldBinlog()},
			recoverTs:    0,
		})
	assert.NoError(t, err)

	segNew := channel.segments[1]
	segNormal := channel.segments[2]

	cases := make([]int64, 0, 100)
	for i := 0; i < 100; i++ {
		cases = append(cases, rand.Int63())
	}
	for _, c := range cases {
		channel.updateSegmentPKRange(1, &storage.Int64FieldData{Data: []int64{c}}) // new segment
		channel.updateSegmentPKRange(2, &storage.Int64FieldData{Data: []int64{c}}) // normal segment
		channel.updateSegmentPKRange(3, &storage.Int64FieldData{Data: []int64{c}}) // non-exist segment

		pk := newInt64PrimaryKey(c)

		assert.True(t, segNew.isPKExist(pk))
		assert.True(t, segNormal.isPKExist(pk))
	}

}

func TestChannelMeta_ChannelCP(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rc := &RootCoordFactory{
		pkType: schemapb.DataType_Int64,
	}

	mockVChannel := "fake-by-dev-rootcoord-dml-1-testchannelcp-v0"
	mockPChannel := "fake-by-dev-rootcoord-dml-1"

	collID := UniqueID(1)
	cm := storage.NewLocalChunkManager(storage.RootPath(channelMetaNodeTestDir))
	defer func() {
		err := cm.RemoveWithPrefix(ctx, cm.RootPath())
		assert.NoError(t, err)
	}()

	t.Run("get and set", func(t *testing.T) {
		pos := &msgpb.MsgPosition{
			ChannelName: mockPChannel,
			Timestamp:   1000,
		}
		channel := newChannel(mockVChannel, collID, nil, rc, cm)
		channel.chunkManager = &mockDataCM{}
		position := channel.getChannelCheckpoint(pos)
		assert.NotNil(t, position)
		assert.True(t, position.ChannelName == pos.ChannelName)
		assert.True(t, position.Timestamp == pos.Timestamp)
	})

	t.Run("set insertBuffer&deleteBuffer then get", func(t *testing.T) {
		run := func(curInsertPos, curDeletePos *msgpb.MsgPosition,
			hisInsertPoss, hisDeletePoss []*msgpb.MsgPosition,
			ttPos, expectedPos *msgpb.MsgPosition) {
			segmentID := UniqueID(1)
			channel := newChannel(mockVChannel, collID, nil, rc, cm)
			channel.chunkManager = &mockDataCM{}
			err := channel.addSegment(
				addSegmentReq{
					segType: datapb.SegmentType_New,
					segID:   segmentID,
					collID:  collID,
				})
			assert.NoError(t, err)
			// set history insert buffers
			for _, pos := range hisInsertPoss {
				pos.MsgID = []byte{1}
				channel.setCurInsertBuffer(segmentID, &BufferData{
					startPos: pos,
				})
				channel.rollInsertBuffer(segmentID)
			}
			// set history delete buffers
			for _, pos := range hisDeletePoss {
				pos.MsgID = []byte{1}
				channel.setCurDeleteBuffer(segmentID, &DelDataBuf{
					startPos: pos,
				})
				channel.rollDeleteBuffer(segmentID)
			}
			// set cur buffers
			if curInsertPos != nil {
				curInsertPos.MsgID = []byte{1}
				channel.setCurInsertBuffer(segmentID, &BufferData{
					startPos: curInsertPos,
				})
			}
			if curDeletePos != nil {
				curDeletePos.MsgID = []byte{1}
				channel.setCurDeleteBuffer(segmentID, &DelDataBuf{
					startPos: curDeletePos,
				})
			}
			// set channelCP
			resPos := channel.getChannelCheckpoint(ttPos)
			assert.NotNil(t, resPos)
			assert.True(t, resPos.ChannelName == expectedPos.ChannelName)
			assert.True(t, resPos.Timestamp == expectedPos.Timestamp)
		}

		run(&msgpb.MsgPosition{Timestamp: 50}, &msgpb.MsgPosition{Timestamp: 60},
			[]*msgpb.MsgPosition{{Timestamp: 70}}, []*msgpb.MsgPosition{{Timestamp: 120}},
			&msgpb.MsgPosition{Timestamp: 120}, &msgpb.MsgPosition{Timestamp: 50})

		run(&msgpb.MsgPosition{Timestamp: 50}, &msgpb.MsgPosition{Timestamp: 60},
			[]*msgpb.MsgPosition{{Timestamp: 70}}, []*msgpb.MsgPosition{{Timestamp: 120}},
			&msgpb.MsgPosition{Timestamp: 30}, &msgpb.MsgPosition{Timestamp: 50})

		// nil cur buffer
		run(nil, nil,
			[]*msgpb.MsgPosition{{Timestamp: 120}}, []*msgpb.MsgPosition{{Timestamp: 110}},
			&msgpb.MsgPosition{Timestamp: 130}, &msgpb.MsgPosition{Timestamp: 110})

		// nil history buffer
		run(&msgpb.MsgPosition{Timestamp: 50}, &msgpb.MsgPosition{Timestamp: 100},
			nil, nil,
			&msgpb.MsgPosition{Timestamp: 100}, &msgpb.MsgPosition{Timestamp: 50})

		// nil buffer
		run(nil, nil,
			nil, nil,
			&msgpb.MsgPosition{Timestamp: 100}, &msgpb.MsgPosition{Timestamp: 100})
	})
}

// ChannelMetaSuite setup test suite for ChannelMeta
type ChannelMetaSuite struct {
	suite.Suite
	channel *ChannelMeta

	collID    UniqueID
	partID    UniqueID
	vchanName string
	cm        *storage.LocalChunkManager
}

func (s *ChannelMetaSuite) SetupSuite() {
	rc := &RootCoordFactory{
		pkType: schemapb.DataType_Int64,
	}
	s.collID = 1
	s.cm = storage.NewLocalChunkManager(storage.RootPath(channelMetaNodeTestDir))
	s.channel = newChannel("channel", s.collID, nil, rc, s.cm)
	s.vchanName = "channel"
}

func (s *ChannelMetaSuite) TearDownSuite() {
	s.cm.RemoveWithPrefix(context.Background(), s.cm.RootPath())
}

func (s *ChannelMetaSuite) SetupTest() {
	var err error
	err = s.channel.addSegment(addSegmentReq{
		segType:     datapb.SegmentType_New,
		segID:       1,
		collID:      s.collID,
		partitionID: s.partID,
		startPos:    &msgpb.MsgPosition{},
		endPos:      nil,
	})
	s.Require().NoError(err)
	err = s.channel.addSegment(addSegmentReq{
		segType:      datapb.SegmentType_Normal,
		segID:        2,
		collID:       s.collID,
		partitionID:  s.partID,
		numOfRows:    10,
		statsBinLogs: nil,
		recoverTs:    0,
	})
	s.Require().NoError(err)
	err = s.channel.addSegment(addSegmentReq{
		segType:      datapb.SegmentType_Flushed,
		segID:        3,
		collID:       s.collID,
		partitionID:  s.partID,
		numOfRows:    10,
		statsBinLogs: nil,
		recoverTs:    0,
	})
	s.Require().NoError(err)
}

func (s *ChannelMetaSuite) TearDownTest() {
	s.channel.removeSegments(1, 2, 3)
}

func (s *ChannelMetaSuite) TestHasSegment() {
	segs := []struct {
		segID UniqueID
		sType datapb.SegmentType
	}{
		{100, datapb.SegmentType_New},
		{101, datapb.SegmentType_New},
		{200, datapb.SegmentType_Normal},
		{201, datapb.SegmentType_Normal},
		{300, datapb.SegmentType_Flushed},
		{301, datapb.SegmentType_Flushed},
		{400, datapb.SegmentType_Compacted},
		{401, datapb.SegmentType_Compacted},
	}

	channel := &ChannelMeta{
		segments: make(map[UniqueID]*Segment),
	}

	for _, seg := range segs {
		s := Segment{segmentID: seg.segID}
		s.setType(seg.sType)
		channel.segments[seg.segID] = &s
	}

	tests := []struct {
		description  string
		inSegID      UniqueID
		countFlushed bool

		expected bool
	}{
		{"segment 100 exist/not countFlushed", 100, false, true},
		{"segment 101 exist/countFlushed", 101, true, true},
		{"segment 200 exist/not countFlushed", 200, false, true},
		{"segment 201 exist/countFlushed", 201, true, true},
		{"segment 300 not exist/not countFlushed", 300, false, false},
		{"segment 301 exist/countFlushed", 301, true, true},
		{"segment 400 not exist/not countFlushed", 400, false, false},
		{"segment 401 not exist/countFlushed", 401, true, false},
		{"segment 500 not exist/not countFlushed", 500, false, false},
		{"segment 501 not exist/countFlushed", 501, true, false},
	}

	for _, test := range tests {
		s.Run(test.description, func() {
			got := channel.hasSegment(test.inSegID, test.countFlushed)
			s.Assert().Equal(test.expected, got)
		})
	}
}

func TestChannelMetaSuite(t *testing.T) {
	suite.Run(t, new(ChannelMetaSuite))
}

type ChannelMetaMockSuite struct {
	suite.Suite
	channel *ChannelMeta

	collID    UniqueID
	partID    UniqueID
	vchanName string
	cm        *mocks.ChunkManager
}

func (s *ChannelMetaMockSuite) SetupTest() {
	rc := &RootCoordFactory{
		pkType: schemapb.DataType_Int64,
	}

	s.cm = mocks.NewChunkManager(s.T())
	s.collID = 1
	s.channel = newChannel("channel", s.collID, nil, rc, s.cm)
	s.vchanName = "channel"
}

func (s *ChannelMetaMockSuite) TestAddSegment_SkipBFLoad() {
	Params.Save(Params.DataNodeCfg.SkipBFStatsLoad.Key, "true")
	defer func() {
		Params.Save(Params.DataNodeCfg.SkipBFStatsLoad.Key, "false")
	}()

	s.Run("success_load", func() {
		defer s.SetupTest()
		ch := make(chan struct{})
		s.cm.EXPECT().MultiRead(mock.Anything, []string{"rootPath/stats/1/0/100/10001"}).
			Run(func(_ context.Context, _ []string) {
				<-ch
			}).Return([][]byte{}, nil)

		err := s.channel.addSegment(addSegmentReq{
			segType:     datapb.SegmentType_Flushed,
			segID:       100,
			collID:      s.collID,
			partitionID: s.partID,
			statsBinLogs: []*datapb.FieldBinlog{
				{
					FieldID: 106,
					Binlogs: []*datapb.Binlog{
						{LogPath: "rootPath/stats/1/0/100/10001"},
					},
				},
			},
		})

		s.NoError(err)

		seg, ok := s.getSegmentByID(100)
		s.Require().True(ok)
		s.True(seg.isLoadingLazy())
		s.True(seg.isPKExist(&storage.Int64PrimaryKey{Value: 100}))

		close(ch)
		s.Eventually(func() bool {
			return !seg.isLoadingLazy()
		}, time.Second, time.Millisecond*100)
	})

	s.Run("fail_nosuchkey", func() {
		defer s.SetupTest()
		ch := make(chan struct{})
		s.cm.EXPECT().MultiRead(mock.Anything, []string{"rootPath/stats/1/0/100/10001"}).
			Run(func(_ context.Context, _ []string) {
				<-ch
			}).Return(nil, storage.WrapErrNoSuchKey("rootPath/stats/1/0/100/10001"))

		err := s.channel.addSegment(addSegmentReq{
			segType:     datapb.SegmentType_Flushed,
			segID:       100,
			collID:      s.collID,
			partitionID: s.partID,
			statsBinLogs: []*datapb.FieldBinlog{
				{
					FieldID: 106,
					Binlogs: []*datapb.Binlog{
						{LogPath: "rootPath/stats/1/0/100/10001"},
					},
				},
			},
		})

		s.NoError(err)

		seg, ok := s.getSegmentByID(100)
		s.Require().True(ok)
		s.True(seg.isLoadingLazy())
		s.True(seg.isPKExist(&storage.Int64PrimaryKey{Value: 100}))

		close(ch)

		// cannot wait here, since running will not reduce immediately
		/*
			s.Eventually(func() bool {
				//			s.T().Log(s.channel.workerpool)
				return s.channel.workerPool.Running() == 0
			}, time.Second, time.Millisecond*100)*/

		// still return false positive
		s.True(seg.isLoadingLazy())
		s.True(seg.isPKExist(&storage.Int64PrimaryKey{Value: 100}))
	})

	s.Run("fail_corrupted", func() {
		defer s.SetupTest()
		ch := make(chan struct{})
		s.cm.EXPECT().MultiRead(mock.Anything, []string{"rootPath/stats/1/0/100/10001"}).
			Run(func(_ context.Context, _ []string) {
				<-ch
			}).Return([][]byte{
			[]byte("ABC"),
		}, nil)

		err := s.channel.addSegment(addSegmentReq{
			segType:     datapb.SegmentType_Flushed,
			segID:       100,
			collID:      s.collID,
			partitionID: s.partID,
			statsBinLogs: []*datapb.FieldBinlog{
				{
					FieldID: 106,
					Binlogs: []*datapb.Binlog{
						{LogPath: "rootPath/stats/1/0/100/10001"},
					},
				},
			},
		})

		s.NoError(err)

		seg, ok := s.getSegmentByID(100)
		s.Require().True(ok)
		s.True(seg.isLoadingLazy())
		s.True(seg.isPKExist(&storage.Int64PrimaryKey{Value: 100}))

		close(ch)
		// cannot wait here, since running will not reduce immediately
		/*
			s.Eventually(func() bool {
				// not retryable
				return s.channel.workerPool.Running() == 0
			}, time.Second, time.Millisecond*100)*/

		// still return false positive
		s.True(seg.isLoadingLazy())
		s.True(seg.isPKExist(&storage.Int64PrimaryKey{Value: 100}))
	})
}

func (s *ChannelMetaMockSuite) TestAddSegment_SkipBFLoad2() {
	Params.Save(Params.DataNodeCfg.SkipBFStatsLoad.Key, "true")
	defer func() {
		Params.Save(Params.DataNodeCfg.SkipBFStatsLoad.Key, "false")
	}()

	s.Run("transient_error", func() {
		defer s.SetupTest()
		ch := make(chan struct{})
		done := func() bool {
			select {
			case <-ch:
				return true
			default:
				return false
			}
		}
		s.cm.EXPECT().MultiRead(mock.Anything, []string{"rootPath/stats/1/0/100/10001"}).Call.
			Return(func(_ context.Context, arg []string) [][]byte {
				if !done() {
					return nil
				}
				return [][]byte{}
			}, func(_ context.Context, arg []string) error {
				if !done() {
					return errors.New("transient error")
				}
				return nil
			})

		err := s.channel.addSegment(addSegmentReq{
			segType:     datapb.SegmentType_Flushed,
			segID:       100,
			collID:      s.collID,
			partitionID: s.partID,
			statsBinLogs: []*datapb.FieldBinlog{
				{
					FieldID: 106,
					Binlogs: []*datapb.Binlog{
						{LogPath: "rootPath/stats/1/0/100/10001"},
					},
				},
			},
		})

		s.NoError(err)

		seg, ok := s.getSegmentByID(100)
		s.Require().True(ok)
		s.True(seg.isLoadingLazy())
		s.True(seg.isPKExist(&storage.Int64PrimaryKey{Value: 100}))

		close(ch)
		s.Eventually(func() bool {
			return !seg.isLoadingLazy()
		}, time.Second, time.Millisecond*100)
	})
}

func (s *ChannelMetaMockSuite) getSegmentByID(id UniqueID) (*Segment, bool) {
	s.channel.segMu.RLock()
	defer s.channel.segMu.RUnlock()

	seg, ok := s.channel.segments[id]
	if ok && seg.isValid() {
		return seg, true
	}

	return nil, false
}

func TestChannelMetaMock(t *testing.T) {
	suite.Run(t, new(ChannelMetaMockSuite))
}

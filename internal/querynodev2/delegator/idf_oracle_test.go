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

package delegator

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// bytesFileReader wraps bytes.Reader to implement storage.FileReader.
type bytesFileReader struct {
	*bytes.Reader
}

func (r *bytesFileReader) Close() error         { return nil }
func (r *bytesFileReader) Size() (int64, error) { return int64(r.Reader.Len()), nil }

type IDFOracleSuite struct {
	suite.Suite
	collectionID     int64
	channel          string
	collectionSchema *schemapb.CollectionSchema
	idfOracle        *idfOracle

	targetVersion int64
	snapshot      *snapshot
}

func (suite *IDFOracleSuite) SetupSuite() {
	suite.collectionID = 111
	suite.channel = "test-channel"
	suite.collectionSchema = &schemapb.CollectionSchema{
		Functions: []*schemapb.FunctionSchema{{
			Type:           schemapb.FunctionType_BM25,
			InputFieldIds:  []int64{101},
			OutputFieldIds: []int64{102},
		}},
	}
}

func (suite *IDFOracleSuite) SetupTest() {
	suite.idfOracle = NewIDFOracle(suite.channel, suite.collectionSchema.GetFunctions()).(*idfOracle)
	suite.idfOracle.dirPath = suite.T().TempDir()
	suite.idfOracle.Start()
	suite.snapshot = &snapshot{
		dist: []SnapshotItem{{1, make([]SegmentEntry, 0)}},
	}
	suite.targetVersion = 0
}

func (suite *IDFOracleSuite) TearDownTest() {
	suite.idfOracle.Close()
}

func (s *IDFOracleSuite) waitTargetVersion(targetVersion int64) {
	for {
		if s.idfOracle.TargetVersion() >= targetVersion {
			return
		}
		time.Sleep(time.Millisecond * 100)
	}
}

func (suite *IDFOracleSuite) genStats(start uint32, end uint32) map[int64]*storage.BM25Stats {
	result := make(map[int64]*storage.BM25Stats)
	result[102] = storage.NewBM25Stats()
	for i := start; i < end; i++ {
		row := map[uint32]float32{i: 1}
		result[102].Append(row)
	}
	return result
}

// registerSealed loads BM25 stats via LoadSealed with a mock ChunkManager.
// Returns the disk size written. Idempotent via LoadSealed's internal check.
func (suite *IDFOracleSuite) registerSealed(segID int64, start uint32, end uint32) int64 {
	stats := suite.genStats(start, end)

	// serialize stats to bytes for mock reader
	data, err := stats[102].Serialize()
	suite.Require().NoError(err)

	cm := mocks.NewChunkManager(suite.T())
	remotePath := fmt.Sprintf("bm25stats/seg_%d/field_102/0", segID)
	cm.EXPECT().Reader(mock.Anything, remotePath).Return(
		&bytesFileReader{bytes.NewReader(data)}, nil,
	).Maybe()

	bm25Logs := []*datapb.FieldBinlog{{
		FieldID: 102,
		Binlogs: []*datapb.Binlog{{LogPath: remotePath}},
	}}

	diskBefore := suite.idfOracle.sealedDiskSize.Load()
	err = suite.idfOracle.LoadSealed(context.Background(), segID, &querypb.SegmentLoadInfo{Bm25Logs: bm25Logs}, cm)
	suite.Require().NoError(err)
	return suite.idfOracle.sealedDiskSize.Load() - diskBefore
}

// update test snapshot
func (suite *IDFOracleSuite) updateSnapshot(seals, grows, drops []int64) *snapshot {
	suite.targetVersion++
	snapshot := &snapshot{
		dist:          []SnapshotItem{{1, make([]SegmentEntry, 0)}},
		targetVersion: suite.targetVersion,
	}

	dropSet := typeutil.NewSet[int64]()
	dropSet.Insert(drops...)

	newSeal := []SegmentEntry{}
	for _, seg := range suite.snapshot.dist[0].Segments {
		if !dropSet.Contain(seg.SegmentID) {
			seg.TargetVersion = suite.targetVersion
		}
		newSeal = append(newSeal, seg)
	}
	for _, seg := range seals {
		newSeal = append(newSeal, SegmentEntry{NodeID: 1, SegmentID: seg, TargetVersion: suite.targetVersion})
	}

	newGrow := []SegmentEntry{}
	for _, seg := range suite.snapshot.growing {
		if !dropSet.Contain(seg.SegmentID) {
			seg.TargetVersion = suite.targetVersion
		} else {
			seg.TargetVersion = redundantTargetVersion
		}
		newGrow = append(newGrow, seg)
	}
	for _, seg := range grows {
		newGrow = append(newGrow, SegmentEntry{NodeID: 1, SegmentID: seg, TargetVersion: suite.targetVersion})
	}

	snapshot.dist[0].Segments = newSeal
	snapshot.growing = newGrow
	suite.snapshot = snapshot
	return snapshot
}

func (suite *IDFOracleSuite) TestSealed() {
	// register sealed
	sealedSegs := []int64{1, 2, 3, 4}
	for _, segID := range sealedSegs {
		suite.registerSealed(segID, uint32(segID), uint32(segID)+1)
	}

	// reduplicate register
	for _, segID := range sealedSegs {
		suite.registerSealed(segID, uint32(segID), uint32(segID)+1)
	}

	// some sealed not in target
	invalidSealedSegs := []int64{5, 6}
	for _, segID := range invalidSealedSegs {
		suite.registerSealed(segID, uint32(segID), uint32(segID)+1)
	}

	// register sealed segment and all preload to current
	suite.Equal(int64(len(sealedSegs)+len(invalidSealedSegs)), suite.idfOracle.current.NumRow())

	// update and sync snapshot make all sealed in target activate
	// and invalid sealed segemnt deactivate
	suite.updateSnapshot(sealedSegs, []int64{}, []int64{})
	suite.idfOracle.SetNext(suite.snapshot)
	suite.waitTargetVersion(suite.targetVersion)
	suite.Equal(int64(len(sealedSegs)), suite.idfOracle.current.NumRow())

	releasedSeg := []int64{1, 2, 3}
	suite.updateSnapshot([]int64{}, []int64{}, releasedSeg)
	suite.idfOracle.SetNext(suite.snapshot)
	suite.waitTargetVersion(suite.targetVersion)
	suite.Equal(int64(1), suite.idfOracle.current.NumRow())

	sparse := typeutil.CreateAndSortSparseFloatRow(map[uint32]float32{4: 1})
	bytes, avgdl, err := suite.idfOracle.BuildIDF(102, &schemapb.SparseFloatArray{Contents: [][]byte{sparse}, Dim: 1})
	suite.NoError(err)
	suite.Equal(float64(1), avgdl)
	suite.Equal(map[uint32]float32{4: 0.2876821}, typeutil.SparseFloatBytesToMap(bytes[0]))

	// reload released segment and some sealed segment stats will not found
	// should not happened
	// will warn but not panic
	suite.updateSnapshot(releasedSeg, []int64{}, []int64{})
	suite.idfOracle.SetNext(suite.snapshot)
	suite.waitTargetVersion(suite.targetVersion)
	suite.Equal(int64(1), suite.idfOracle.current.NumRow())
}

func (suite *IDFOracleSuite) TestGrow() {
	// register grow
	growSegs := []int64{1, 2, 3, 4}
	for _, segID := range growSegs {
		suite.idfOracle.RegisterGrowing(segID, suite.genStats(uint32(segID), uint32(segID)+1))
	}
	// reduplicate register
	for _, segID := range growSegs {
		suite.idfOracle.RegisterGrowing(segID, suite.genStats(uint32(segID), uint32(segID)+1))
	}

	// register sealed segment but all deactvate
	suite.Equal(int64(4), suite.idfOracle.current.NumRow())
	suite.updateSnapshot([]int64{}, growSegs, []int64{})

	releasedSeg := []int64{1, 2, 3}
	suite.updateSnapshot([]int64{}, []int64{}, releasedSeg)
	suite.idfOracle.LazyRemoveGrowings(suite.snapshot.targetVersion, releasedSeg...)
	suite.idfOracle.SetNext(suite.snapshot)
	suite.waitTargetVersion(suite.targetVersion)
	suite.Equal(int64(1), suite.idfOracle.current.NumRow())

	suite.idfOracle.UpdateGrowing(4, suite.genStats(5, 6))
	suite.Equal(int64(2), suite.idfOracle.current.NumRow())
}

func (suite *IDFOracleSuite) TestStats() {
	stats := newBm25Stats([]*schemapb.FunctionSchema{{
		Type:           schemapb.FunctionType_BM25,
		InputFieldIds:  []int64{101},
		OutputFieldIds: []int64{102},
	}})

	_, err := stats.GetStats(104)
	suite.Error(err)

	_, err = stats.GetStats(102)
	suite.NoError(err)
}

func (suite *IDFOracleSuite) TestLocalCache() {
	// register sealed (all stats are now always on disk)
	sealedSegs := []int64{1, 2, 3, 4}
	for _, segID := range sealedSegs {
		suite.registerSealed(segID, uint32(segID), uint32(segID)+1)
	}

	// some sealed not in target
	invalidSealedSegs := []int64{5, 6}
	for _, segID := range invalidSealedSegs {
		suite.registerSealed(segID, uint32(segID), uint32(segID)+1)
	}

	// register sealed segment and all preload to current
	suite.Equal(int64(len(sealedSegs)+len(invalidSealedSegs)), suite.idfOracle.current.NumRow())

	// verify all sealed stats have local dir set
	suite.idfOracle.sealed.Range(func(id int64, stats *sealedBm25Stats) bool {
		stats.RLock()
		defer stats.RUnlock()
		suite.NotEmpty(stats.localDir)
		return true
	})

	// update and sync snapshot make all sealed in target activate
	suite.updateSnapshot(sealedSegs, []int64{}, []int64{})
	suite.idfOracle.SetNext(suite.snapshot)
	suite.waitTargetVersion(suite.targetVersion)
	suite.Equal(int64(len(sealedSegs)), suite.idfOracle.current.NumRow())

	// release some segments
	releasedSeg := []int64{1, 2, 3}
	suite.updateSnapshot([]int64{}, []int64{}, releasedSeg)
	suite.idfOracle.SetNext(suite.snapshot)
	suite.waitTargetVersion(suite.targetVersion)
	suite.Equal(int64(1), suite.idfOracle.current.NumRow())
}

func (suite *IDFOracleSuite) TestFetchStatsRemoved() {
	segID := int64(1)
	suite.registerSealed(segID, 1, 5)

	stats, ok := suite.idfOracle.sealed.Get(segID)
	suite.True(ok)

	// remove then fetch — should return error
	stats.Remove()
	_, err := stats.FetchStats()
	suite.Error(err)
	suite.Contains(err.Error(), "already removed")
}

func (suite *IDFOracleSuite) TestDiskSizeTracking() {
	disk1 := suite.registerSealed(1, 1, 2)
	disk2 := suite.registerSealed(2, 2, 3)
	suite.Equal(disk1+disk2, suite.idfOracle.sealedDiskSize.Load())

	// SyncDistribution with only seg 1 in target — seg 2 gets removed
	suite.updateSnapshot([]int64{1}, []int64{}, []int64{})
	suite.idfOracle.SetNext(suite.snapshot)
	suite.waitTargetVersion(suite.targetVersion)
	suite.Equal(disk1, suite.idfOracle.sealedDiskSize.Load())

	// release seg 1
	suite.updateSnapshot([]int64{}, []int64{}, []int64{1})
	suite.idfOracle.SetNext(suite.snapshot)
	suite.waitTargetVersion(suite.targetVersion)
	suite.Equal(int64(0), suite.idfOracle.sealedDiskSize.Load())
}

func (suite *IDFOracleSuite) TestDiskSizeTrackingSyncDistribution() {
	sealedSegs := []int64{1, 2, 3}
	var totalDisk int64
	for _, segID := range sealedSegs {
		totalDisk += suite.registerSealed(segID, uint32(segID), uint32(segID)+1)
	}
	suite.Equal(totalDisk, suite.idfOracle.sealedDiskSize.Load())

	// activate only seg 1,2 via SyncDistribution — seg 3 gets removed
	suite.updateSnapshot([]int64{1, 2}, []int64{}, []int64{})
	suite.idfOracle.SetNext(suite.snapshot)
	suite.waitTargetVersion(suite.targetVersion)

	suite.Equal(2, suite.idfOracle.sealed.Len())
	suite.Less(suite.idfOracle.sealedDiskSize.Load(), totalDisk)
}

func (suite *IDFOracleSuite) TestMemorySize() {
	// initial state — empty current stats
	suite.Greater(suite.idfOracle.MemorySize(), int64(0)) // current has fixed overhead

	// add growing segments — memory should increase
	sizeBefore := suite.idfOracle.MemorySize()
	suite.idfOracle.RegisterGrowing(1, suite.genStats(1, 100))
	sizeAfter := suite.idfOracle.MemorySize()
	suite.Greater(sizeAfter, sizeBefore)
}

func (suite *IDFOracleSuite) TestUpdateGrowingCheckMemory() {
	suite.idfOracle.RegisterGrowing(1, suite.genStats(1, 2))

	// repeated updates grow the stats
	for i := uint32(2); i < 200; i++ {
		suite.idfOracle.UpdateGrowing(1, suite.genStats(i, i+1))
	}
	suite.Equal(int64(199), suite.idfOracle.current.NumRow())
}

func (suite *IDFOracleSuite) TestLoadSealedIdempotent() {
	suite.registerSealed(1, 1, 5)
	suite.Equal(int64(4), suite.idfOracle.current.NumRow())
	diskSize := suite.idfOracle.sealedDiskSize.Load()

	// duplicate load — should be skipped
	suite.registerSealed(1, 1, 5)
	suite.Equal(int64(4), suite.idfOracle.current.NumRow())
	suite.Equal(diskSize, suite.idfOracle.sealedDiskSize.Load())
}

func (suite *IDFOracleSuite) TestLoadSealedEmptyBm25Logs() {
	cm := mocks.NewChunkManager(suite.T())
	// nil bm25Logs
	err := suite.idfOracle.LoadSealed(context.Background(), 1, &querypb.SegmentLoadInfo{}, cm)
	suite.NoError(err)
	suite.False(suite.idfOracle.sealed.Contain(1))

	// empty bm25Logs
	err = suite.idfOracle.LoadSealed(context.Background(), 2, &querypb.SegmentLoadInfo{Bm25Logs: []*datapb.FieldBinlog{}}, cm)
	suite.NoError(err)
	suite.False(suite.idfOracle.sealed.Contain(2))
}

func (suite *IDFOracleSuite) TestLoadSealedNoParse() {
	// set targetVersion > 0 so needParse = false
	suite.idfOracle.targetVersion.Store(1)

	stats := suite.genStats(1, 5)
	data, err := stats[102].Serialize()
	suite.Require().NoError(err)

	cm := mocks.NewChunkManager(suite.T())
	remotePath := "bm25stats/seg_1/field_102/0"
	cm.EXPECT().Reader(mock.Anything, remotePath).Return(
		&bytesFileReader{bytes.NewReader(data)}, nil,
	)

	bm25Logs := []*datapb.FieldBinlog{{
		FieldID: 102,
		Binlogs: []*datapb.Binlog{{LogPath: remotePath}},
	}}

	err = suite.idfOracle.LoadSealed(context.Background(), 1, &querypb.SegmentLoadInfo{Bm25Logs: bm25Logs}, cm)
	suite.NoError(err)

	// segment registered but NOT preloaded (current stays 0)
	suite.True(suite.idfOracle.sealed.Contain(1))
	suite.Equal(int64(0), suite.idfOracle.current.NumRow())

	// disk file should exist
	segDir := path.Join(suite.idfOracle.dirPath, "1", "102")
	entries, err := os.ReadDir(segDir)
	suite.NoError(err)
	suite.NotEmpty(entries)

	// FetchStats should work (reads from disk)
	sealedStats, ok := suite.idfOracle.sealed.Get(1)
	suite.True(ok)
	fetched, err := sealedStats.FetchStats()
	suite.NoError(err)
	suite.Equal(int64(4), fetched[102].NumRow())
}

func (suite *IDFOracleSuite) TestLoadSealedFailureCleanup() {
	cm := mocks.NewChunkManager(suite.T())
	remotePath := "bm25stats/seg_1/field_102/0"
	cm.EXPECT().Reader(mock.Anything, remotePath).Return(
		nil, errors.New("remote read failed"),
	)

	bm25Logs := []*datapb.FieldBinlog{{
		FieldID: 102,
		Binlogs: []*datapb.Binlog{{LogPath: remotePath}},
	}}

	err := suite.idfOracle.LoadSealed(context.Background(), 1, &querypb.SegmentLoadInfo{Bm25Logs: bm25Logs}, cm)
	suite.Error(err)

	// segment should NOT be registered
	suite.False(suite.idfOracle.sealed.Contain(1))
	suite.Equal(int64(0), suite.idfOracle.sealedDiskSize.Load())

	// disk directory should be cleaned up
	segDir := path.Join(suite.idfOracle.dirPath, "1")
	_, statErr := os.Stat(segDir)
	suite.True(os.IsNotExist(statErr))
}

func (suite *IDFOracleSuite) TestFilterBM25Logs() {
	// normal binlogs
	result := filterBM25Logs([]*datapb.FieldBinlog{{
		FieldID: 101,
		Binlogs: []*datapb.Binlog{
			{LogPath: "root/0"},
			{LogPath: "root/2"},
		},
	}})
	suite.Equal(map[int64][]string{101: {"root/0", "root/2"}}, result)

	// compound stats — should pick only compound and stop
	result = filterBM25Logs([]*datapb.FieldBinlog{{
		FieldID: 101,
		Binlogs: []*datapb.Binlog{
			{LogPath: "root/0"},
			{LogPath: "root/" + storage.CompoundStatsType.LogIdx()},
			{LogPath: "root/2"},
		},
	}})
	suite.Equal(map[int64][]string{101: {"root/" + storage.CompoundStatsType.LogIdx()}}, result)

	// nil input
	result = filterBM25Logs(nil)
	suite.Empty(result)

	// empty binlogs for a field — still creates entry
	result = filterBM25Logs([]*datapb.FieldBinlog{{
		FieldID: 101,
		Binlogs: []*datapb.Binlog{},
	}})
	suite.Contains(result, int64(101))
	suite.Empty(result[101])
}

func TestIDFOracle(t *testing.T) {
	suite.Run(t, new(IDFOracleSuite))
}

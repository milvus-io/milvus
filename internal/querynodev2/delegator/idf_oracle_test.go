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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// bytesFileReader wraps bytes.Reader to implement storage.FileReader.
type bytesFileReader struct {
	*bytes.Reader
}

func (r *bytesFileReader) Close() error         { return nil }
func (r *bytesFileReader) Size() (int64, error) { return int64(r.Len()), nil }

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
	return suite.genStatsForField(102, start, end)
}

func (suite *IDFOracleSuite) genStatsForField(fieldID int64, start uint32, end uint32) map[int64]*storage.BM25Stats {
	return genBM25StatsForField(fieldID, start, end)
}

func genBM25StatsForField(fieldID int64, start uint32, end uint32) map[int64]*storage.BM25Stats {
	result := make(map[int64]*storage.BM25Stats)
	result[fieldID] = storage.NewBM25Stats()
	for i := start; i < end; i++ {
		row := map[uint32]float32{i: 1}
		result[fieldID].Append(row)
	}
	return result
}

func (suite *IDFOracleSuite) setIDFRemoteFetchOnly(enabled bool) {
	setIDFRemoteFetchOnly(suite.T(), enabled)
}

func setIDFRemoteFetchOnly(t testing.TB, enabled bool) {
	t.Helper()
	item := &paramtable.Get().QueryNodeCfg.IDFRemoteFetchOnly
	old := item.GetValue()
	require.NoError(t, paramtable.Get().Save(item.Key, fmt.Sprintf("%t", enabled)))
	t.Cleanup(func() {
		_ = paramtable.Get().Save(item.Key, old)
	})
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

	bm25Logs := bm25LogsForField(102, remotePath)

	diskBefore := suite.idfOracle.sealedDiskSize.Load()
	err = suite.idfOracle.LoadSealed(context.Background(), segID, &querypb.SegmentLoadInfo{Bm25Logs: bm25Logs}, cm)
	suite.Require().NoError(err)
	return suite.idfOracle.sealedDiskSize.Load() - diskBefore
}

func bm25LogsForField(fieldID int64, paths ...string) []*datapb.FieldBinlog {
	binlogs := make([]*datapb.Binlog, 0, len(paths))
	for _, logPath := range paths {
		binlogs = append(binlogs, &datapb.Binlog{LogPath: logPath})
	}
	return []*datapb.FieldBinlog{{
		FieldID: fieldID,
		Binlogs: binlogs,
	}}
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

func (suite *IDFOracleSuite) TestRegisterGrowingClonesStats() {
	stats := suite.genStats(1, 2)

	suite.idfOracle.RegisterGrowing(1, stats)
	stats[102].Append(map[uint32]float32{2: 1})

	registered, ok := suite.idfOracle.growing[1]
	suite.True(ok)
	suite.Equal(int64(1), registered.bm25Stats[102].NumRow())
	suite.Equal(int64(1), suite.idfOracle.current.NumRow())
}

func (suite *IDFOracleSuite) TestUpdateGrowingAfterEmptyRegistration() {
	suite.idfOracle.RegisterGrowing(1, bm25Stats{})

	registered, ok := suite.idfOracle.growing[1]
	suite.True(ok)
	suite.NotNil(registered.bm25Stats)

	suite.idfOracle.UpdateGrowing(1, suite.genStats(1, 2))
	suite.Equal(int64(1), registered.bm25Stats[102].NumRow())
	suite.Equal(int64(1), suite.idfOracle.current.NumRow())
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

func (suite *IDFOracleSuite) TestFetchStatsUsesExplicitRemoteFetchOnlyMode() {
	stats := &sealedBm25Stats{
		activate:  atomic.NewBool(false),
		segmentID: 1,
		fieldList: []int64{102},
	}

	_, err := stats.FetchStats()
	suite.Error(err)
	suite.Contains(err.Error(), "read local dir")
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

func (suite *IDFOracleSuite) TestLoadSealedRemoteFetchOnlyPreload() {
	suite.setIDFRemoteFetchOnly(true)

	stats := suite.genStats(1, 5)
	data, err := stats[102].Serialize()
	suite.Require().NoError(err)

	cm := mocks.NewChunkManager(suite.T())
	remotePath := "bm25stats/seg_1/field_102/0"
	cm.EXPECT().Reader(mock.Anything, remotePath).RunAndReturn(
		func(context.Context, string) (storage.FileReader, error) {
			return &bytesFileReader{bytes.NewReader(data)}, nil
		},
	).Once()

	bm25Logs := []*datapb.FieldBinlog{{
		FieldID: 102,
		Binlogs: []*datapb.Binlog{{LogPath: remotePath}},
	}}

	err = suite.idfOracle.LoadSealed(context.Background(), 1, &querypb.SegmentLoadInfo{Bm25Logs: bm25Logs}, cm)
	suite.NoError(err)
	suite.Equal(int64(4), suite.idfOracle.current.NumRow())
	suite.Equal(int64(0), suite.idfOracle.sealedDiskSize.Load())

	sealedStats, ok := suite.idfOracle.sealed.Get(1)
	suite.True(ok)
	sealedStats.RLock()
	suite.Empty(sealedStats.localDir)
	suite.Equal([]string{remotePath}, sealedStats.remotePaths[102])
	sealedStats.RUnlock()

	_, statErr := os.Stat(path.Join(suite.idfOracle.dirPath, "1"))
	suite.True(os.IsNotExist(statErr))
}

func (suite *IDFOracleSuite) TestLoadSealedRemoteFetchOnlySyncDistribution() {
	suite.setIDFRemoteFetchOnly(true)
	suite.targetVersion = 1
	suite.idfOracle.targetVersion.Store(1)

	stats := suite.genStats(1, 5)
	data, err := stats[102].Serialize()
	suite.Require().NoError(err)

	cm := mocks.NewChunkManager(suite.T())
	remotePath := "bm25stats/seg_1/field_102/0"
	cm.EXPECT().Reader(mock.Anything, remotePath).RunAndReturn(
		func(context.Context, string) (storage.FileReader, error) {
			return &bytesFileReader{bytes.NewReader(data)}, nil
		},
	).Twice()

	bm25Logs := []*datapb.FieldBinlog{{
		FieldID: 102,
		Binlogs: []*datapb.Binlog{{LogPath: remotePath}},
	}}

	err = suite.idfOracle.LoadSealed(context.Background(), 1, &querypb.SegmentLoadInfo{Bm25Logs: bm25Logs}, cm)
	suite.NoError(err)
	suite.True(suite.idfOracle.sealed.Contain(1))
	suite.Equal(int64(0), suite.idfOracle.current.NumRow())
	suite.Equal(int64(0), suite.idfOracle.sealedDiskSize.Load())

	suite.updateSnapshot([]int64{1}, []int64{}, []int64{})
	suite.idfOracle.SetNext(suite.snapshot)
	suite.waitTargetVersion(suite.targetVersion)
	suite.Equal(int64(4), suite.idfOracle.current.NumRow())

	suite.updateSnapshot([]int64{}, []int64{}, []int64{1})
	suite.idfOracle.SetNext(suite.snapshot)
	suite.waitTargetVersion(suite.targetVersion)
	suite.Equal(int64(0), suite.idfOracle.current.NumRow())
	suite.Equal(int64(0), suite.idfOracle.sealedDiskSize.Load())
}

func (suite *IDFOracleSuite) TestLoadSealedFailureCleanup() {
	cm := mocks.NewChunkManager(suite.T())
	remotePath := "bm25stats/seg_1/field_102/0"
	cm.EXPECT().Reader(mock.Anything, remotePath).Return(
		nil, errors.New("remote read failed"),
	)

	bm25Logs := bm25LogsForField(102, remotePath)

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

func TestIDFOracle(t *testing.T) {
	suite.Run(t, new(IDFOracleSuite))
}

func TestLoadSealedForReopenLoadsOnlyMissingFields(t *testing.T) {
	idfOracle := NewIDFOracle("test-channel", []*schemapb.FunctionSchema{{
		Type:           schemapb.FunctionType_BM25,
		InputFieldIds:  []int64{101},
		OutputFieldIds: []int64{102},
	}}).(*idfOracle)
	idfOracle.dirPath = t.TempDir()
	idfOracle.Start()
	defer idfOracle.Close()

	oldStats := genBM25StatsForField(102, 1, 3)
	oldData, err := oldStats[102].Serialize()
	require.NoError(t, err)
	oldPath := "bm25stats/seg_1/field_102/0"
	cm := mocks.NewChunkManager(t)
	cm.EXPECT().Reader(mock.Anything, oldPath).Return(
		&bytesFileReader{bytes.NewReader(oldData)}, nil,
	).Once()
	err = idfOracle.LoadSealed(context.Background(), 1, &querypb.SegmentLoadInfo{Bm25Logs: bm25LogsForField(102, oldPath)}, cm)
	require.NoError(t, err)
	diskSize := idfOracle.sealedDiskSize.Load()

	newStats := genBM25StatsForField(104, 10, 13)
	newData, err := newStats[104].Serialize()
	require.NoError(t, err)
	newPath := "bm25stats/seg_1/field_104/0"
	cm.EXPECT().Reader(mock.Anything, newPath).Return(
		&bytesFileReader{bytes.NewReader(newData)}, nil,
	).Once()

	reopenInfo := &querypb.SegmentLoadInfo{
		Bm25Logs: append(
			bm25LogsForField(102, oldPath),
			bm25LogsForField(104, newPath)...,
		),
	}
	err = idfOracle.LoadSealedForReopen(context.Background(), 1, reopenInfo, cm, false)
	require.NoError(t, err)

	sealedStats, ok := idfOracle.sealed.Get(1)
	require.True(t, ok)
	assert.ElementsMatch(t, []int64{102, 104}, sealedStats.FieldList())
	assert.Greater(t, idfOracle.sealedDiskSize.Load(), diskSize)
	fetched, err := sealedStats.FetchStats()
	require.NoError(t, err)
	assert.Equal(t, int64(2), fetched[102].NumRow())
	assert.Equal(t, int64(3), fetched[104].NumRow())
}

func TestLoadSealedForReopenIdempotentAfterSuccess(t *testing.T) {
	idfOracle := NewIDFOracle("test-channel", []*schemapb.FunctionSchema{{
		Type:           schemapb.FunctionType_BM25,
		InputFieldIds:  []int64{101},
		OutputFieldIds: []int64{102},
	}}).(*idfOracle)
	idfOracle.dirPath = t.TempDir()
	idfOracle.Start()
	defer idfOracle.Close()

	sealedStats := &sealedBm25Stats{
		ts:        time.Now(),
		activate:  atomic.NewBool(false),
		segmentID: 1,
		localDir:  path.Join(idfOracle.dirPath, "1"),
		fieldList: []int64{102},
		diskSize:  7,
	}
	idfOracle.sealed.Insert(1, sealedStats)
	idfOracle.sealedDiskSize.Store(7)

	newStats := genBM25StatsForField(104, 10, 13)
	data, err := newStats[104].Serialize()
	require.NoError(t, err)

	cm := mocks.NewChunkManager(t)
	remotePath := "bm25stats/seg_1/field_104/0"
	cm.EXPECT().Reader(mock.Anything, remotePath).Return(
		&bytesFileReader{bytes.NewReader(data)}, nil,
	).Once()

	reopenInfo := &querypb.SegmentLoadInfo{Bm25Logs: bm25LogsForField(104, remotePath)}
	err = idfOracle.LoadSealedForReopen(context.Background(), 1, reopenInfo, cm, false)
	require.NoError(t, err)
	diskSize := idfOracle.sealedDiskSize.Load()

	err = idfOracle.LoadSealedForReopen(context.Background(), 1, reopenInfo, cm, false)
	require.NoError(t, err)
	assert.Equal(t, diskSize, idfOracle.sealedDiskSize.Load())
}

func TestLoadSealedForReopenActivatesExistingInactiveSegment(t *testing.T) {
	idfOracle := NewIDFOracle("test-channel", []*schemapb.FunctionSchema{{
		Type:           schemapb.FunctionType_BM25,
		InputFieldIds:  []int64{103},
		OutputFieldIds: []int64{104},
	}}).(*idfOracle)
	idfOracle.dirPath = t.TempDir()
	idfOracle.Start()
	defer idfOracle.Close()

	newStats := genBM25StatsForField(104, 10, 13)
	data, err := newStats[104].Serialize()
	require.NoError(t, err)

	cm := mocks.NewChunkManager(t)
	remotePath := "bm25stats/seg_1/field_104/0"
	cm.EXPECT().Reader(mock.Anything, remotePath).Return(
		&bytesFileReader{bytes.NewReader(data)}, nil,
	).Once()
	reopenInfo := &querypb.SegmentLoadInfo{Bm25Logs: bm25LogsForField(104, remotePath)}

	err = idfOracle.LoadSealedForReopen(context.Background(), 1, reopenInfo, cm, false)
	require.NoError(t, err)
	sealedStats, ok := idfOracle.sealed.Get(1)
	require.True(t, ok)
	require.False(t, sealedStats.activate.Load())
	current, err := idfOracle.current.GetStats(104)
	require.NoError(t, err)
	require.Equal(t, int64(0), current.NumRow())

	err = idfOracle.LoadSealedForReopen(context.Background(), 1, reopenInfo, cm, true)
	require.NoError(t, err)
	assert.True(t, sealedStats.activate.Load())
	current, err = idfOracle.current.GetStats(104)
	require.NoError(t, err)
	assert.Equal(t, int64(3), current.NumRow())
}

func TestLoadSealedForReopenCreatesMissingSegmentEntry(t *testing.T) {
	idfOracle := NewIDFOracle("test-channel", []*schemapb.FunctionSchema{{
		Type:           schemapb.FunctionType_BM25,
		InputFieldIds:  []int64{101},
		OutputFieldIds: []int64{104},
	}}).(*idfOracle)
	idfOracle.dirPath = t.TempDir()
	idfOracle.Start()
	defer idfOracle.Close()

	newStats := genBM25StatsForField(104, 10, 13)
	data, err := newStats[104].Serialize()
	require.NoError(t, err)

	cm := mocks.NewChunkManager(t)
	remotePath := "bm25stats/seg_1/field_104/0"
	cm.EXPECT().Reader(mock.Anything, remotePath).Return(
		&bytesFileReader{bytes.NewReader(data)}, nil,
	).Once()

	err = idfOracle.LoadSealedForReopen(context.Background(), 1, &querypb.SegmentLoadInfo{Bm25Logs: bm25LogsForField(104, remotePath)}, cm, false)
	require.NoError(t, err)

	sealedStats, ok := idfOracle.sealed.Get(1)
	require.True(t, ok)
	assert.ElementsMatch(t, []int64{104}, sealedStats.FieldList())
	fetched, err := sealedStats.FetchStats()
	require.NoError(t, err)
	assert.Equal(t, int64(3), fetched[104].NumRow())
}

func TestLoadSealedForReopenFailureCleanupPreservesExistingFields(t *testing.T) {
	idfOracle := NewIDFOracle("test-channel", []*schemapb.FunctionSchema{{
		Type:           schemapb.FunctionType_BM25,
		InputFieldIds:  []int64{101},
		OutputFieldIds: []int64{102},
	}}).(*idfOracle)
	idfOracle.dirPath = t.TempDir()
	idfOracle.Start()
	defer idfOracle.Close()

	oldFieldDir := path.Join(idfOracle.dirPath, "1", "102")
	require.NoError(t, os.MkdirAll(oldFieldDir, os.ModePerm))
	require.NoError(t, os.WriteFile(path.Join(oldFieldDir, "0.data"), []byte("keep"), 0o600))
	sealedStats := &sealedBm25Stats{
		ts:        time.Now(),
		activate:  atomic.NewBool(false),
		segmentID: 1,
		localDir:  path.Join(idfOracle.dirPath, "1"),
		fieldList: []int64{102},
		diskSize:  4,
	}
	idfOracle.sealed.Insert(1, sealedStats)
	idfOracle.sealedDiskSize.Store(4)

	cm := mocks.NewChunkManager(t)
	remotePath := "bm25stats/seg_1/field_104/0"
	cm.EXPECT().Reader(mock.Anything, remotePath).Return(
		nil, errors.New("remote read failed"),
	).Once()

	err := idfOracle.LoadSealedForReopen(context.Background(), 1, &querypb.SegmentLoadInfo{Bm25Logs: bm25LogsForField(104, remotePath)}, cm, false)
	require.Error(t, err)

	sealedStats, ok := idfOracle.sealed.Get(1)
	require.True(t, ok)
	assert.ElementsMatch(t, []int64{102}, sealedStats.FieldList())
	assert.Equal(t, int64(4), idfOracle.sealedDiskSize.Load())
	_, err = os.Stat(oldFieldDir)
	assert.NoError(t, err)
	_, err = os.Stat(path.Join(idfOracle.dirPath, "1", "104"))
	assert.True(t, os.IsNotExist(err))
}

func TestActiveReopenBM25MergesNewFieldStats(t *testing.T) {
	idfOracle := NewIDFOracle("test-channel", []*schemapb.FunctionSchema{{
		Type:           schemapb.FunctionType_BM25,
		InputFieldIds:  []int64{101},
		OutputFieldIds: []int64{102},
	}, {
		Type:           schemapb.FunctionType_BM25,
		InputFieldIds:  []int64{103},
		OutputFieldIds: []int64{104},
	}}).(*idfOracle)
	idfOracle.dirPath = t.TempDir()
	idfOracle.Start()
	defer idfOracle.Close()

	sealedStats := &sealedBm25Stats{
		ts:        time.Now(),
		activate:  atomic.NewBool(true),
		segmentID: 1,
		localDir:  path.Join(idfOracle.dirPath, "1"),
		fieldList: []int64{102},
		diskSize:  7,
	}
	idfOracle.sealed.Insert(1, sealedStats)
	idfOracle.sealedDiskSize.Store(7)

	newStats := genBM25StatsForField(104, 10, 13)
	data, err := newStats[104].Serialize()
	require.NoError(t, err)

	cm := mocks.NewChunkManager(t)
	remotePath := "bm25stats/seg_1/field_104/0"
	cm.EXPECT().Reader(mock.Anything, remotePath).Return(
		&bytesFileReader{bytes.NewReader(data)}, nil,
	).Once()

	err = idfOracle.LoadSealedForReopen(context.Background(), 1, &querypb.SegmentLoadInfo{Bm25Logs: bm25LogsForField(104, remotePath)}, cm, false)
	require.NoError(t, err)

	current, err := idfOracle.current.GetStats(104)
	require.NoError(t, err)
	assert.Equal(t, int64(3), current.NumRow())
	assert.True(t, sealedStats.activate.Load())
}

func TestReadableReopenBM25MergesNewFieldStats(t *testing.T) {
	idfOracle := NewIDFOracle("test-channel", []*schemapb.FunctionSchema{{
		Type:           schemapb.FunctionType_BM25,
		InputFieldIds:  []int64{103},
		OutputFieldIds: []int64{104},
	}}).(*idfOracle)
	idfOracle.dirPath = t.TempDir()
	idfOracle.Start()
	defer idfOracle.Close()

	newStats := genBM25StatsForField(104, 10, 13)
	data, err := newStats[104].Serialize()
	require.NoError(t, err)

	cm := mocks.NewChunkManager(t)
	remotePath := "bm25stats/seg_1/field_104/0"
	cm.EXPECT().Reader(mock.Anything, remotePath).Return(
		&bytesFileReader{bytes.NewReader(data)}, nil,
	).Once()

	err = idfOracle.LoadSealedForReopen(context.Background(), 1, &querypb.SegmentLoadInfo{Bm25Logs: bm25LogsForField(104, remotePath)}, cm, true)
	require.NoError(t, err)

	sealedStats, ok := idfOracle.sealed.Get(1)
	require.True(t, ok)
	assert.True(t, sealedStats.activate.Load())
	current, err := idfOracle.current.GetStats(104)
	require.NoError(t, err)
	assert.Equal(t, int64(3), current.NumRow())
}

func TestReadableReopenBM25ActivatesExistingInactiveSegment(t *testing.T) {
	idfOracle := NewIDFOracle("test-channel", []*schemapb.FunctionSchema{{
		Type:           schemapb.FunctionType_BM25,
		InputFieldIds:  []int64{101},
		OutputFieldIds: []int64{102},
	}, {
		Type:           schemapb.FunctionType_BM25,
		InputFieldIds:  []int64{103},
		OutputFieldIds: []int64{104},
	}}).(*idfOracle)
	idfOracle.dirPath = t.TempDir()
	// Keep the initial load inactive so this test exercises reopen activation.
	idfOracle.targetVersion.Store(1)
	idfOracle.Start()
	defer idfOracle.Close()

	oldStats := genBM25StatsForField(102, 1, 3)
	oldData, err := oldStats[102].Serialize()
	require.NoError(t, err)
	oldPath := "bm25stats/seg_1/field_102/0"

	newStats := genBM25StatsForField(104, 10, 13)
	newData, err := newStats[104].Serialize()
	require.NoError(t, err)
	newPath := "bm25stats/seg_1/field_104/0"

	cm := mocks.NewChunkManager(t)
	cm.EXPECT().Reader(mock.Anything, oldPath).Return(
		&bytesFileReader{bytes.NewReader(oldData)}, nil,
	).Once()
	cm.EXPECT().Reader(mock.Anything, newPath).Return(
		&bytesFileReader{bytes.NewReader(newData)}, nil,
	).Once()

	err = idfOracle.LoadSealed(context.Background(), 1, &querypb.SegmentLoadInfo{Bm25Logs: bm25LogsForField(102, oldPath)}, cm)
	require.NoError(t, err)
	sealedStats, ok := idfOracle.sealed.Get(1)
	require.True(t, ok)
	require.False(t, sealedStats.activate.Load())
	current, err := idfOracle.current.GetStats(102)
	require.NoError(t, err)
	require.Equal(t, int64(0), current.NumRow())

	reopenInfo := &querypb.SegmentLoadInfo{
		Bm25Logs: append(
			bm25LogsForField(102, oldPath),
			bm25LogsForField(104, newPath)...,
		),
	}
	err = idfOracle.LoadSealedForReopen(context.Background(), 1, reopenInfo, cm, true)
	require.NoError(t, err)

	assert.True(t, sealedStats.activate.Load())
	current, err = idfOracle.current.GetStats(102)
	require.NoError(t, err)
	assert.Equal(t, int64(2), current.NumRow())
	current, err = idfOracle.current.GetStats(104)
	require.NoError(t, err)
	assert.Equal(t, int64(3), current.NumRow())
}

func TestReadableReopenBM25RetryDoesNotDoubleMerge(t *testing.T) {
	idfOracle := NewIDFOracle("test-channel", []*schemapb.FunctionSchema{{
		Type:           schemapb.FunctionType_BM25,
		InputFieldIds:  []int64{103},
		OutputFieldIds: []int64{104},
	}}).(*idfOracle)
	idfOracle.dirPath = t.TempDir()
	idfOracle.Start()
	defer idfOracle.Close()

	newStats := genBM25StatsForField(104, 10, 13)
	data, err := newStats[104].Serialize()
	require.NoError(t, err)

	cm := mocks.NewChunkManager(t)
	remotePath := "bm25stats/seg_1/field_104/0"
	cm.EXPECT().Reader(mock.Anything, remotePath).Return(
		&bytesFileReader{bytes.NewReader(data)}, nil,
	).Once()
	reopenInfo := &querypb.SegmentLoadInfo{Bm25Logs: bm25LogsForField(104, remotePath)}

	err = idfOracle.LoadSealedForReopen(context.Background(), 1, reopenInfo, cm, true)
	require.NoError(t, err)
	err = idfOracle.LoadSealedForReopen(context.Background(), 1, reopenInfo, cm, true)
	require.NoError(t, err)

	current, err := idfOracle.current.GetStats(104)
	require.NoError(t, err)
	assert.Equal(t, int64(3), current.NumRow())
}

func TestSyncDistributionReopenBM25InactiveThenActivate(t *testing.T) {
	idfOracle := NewIDFOracle("test-channel", []*schemapb.FunctionSchema{{
		Type:           schemapb.FunctionType_BM25,
		InputFieldIds:  []int64{103},
		OutputFieldIds: []int64{104},
	}}).(*idfOracle)
	idfOracle.dirPath = t.TempDir()
	idfOracle.Start()
	defer idfOracle.Close()

	newStats := genBM25StatsForField(104, 10, 13)
	data, err := newStats[104].Serialize()
	require.NoError(t, err)

	cm := mocks.NewChunkManager(t)
	remotePath := "bm25stats/seg_1/field_104/0"
	cm.EXPECT().Reader(mock.Anything, remotePath).Return(
		&bytesFileReader{bytes.NewReader(data)}, nil,
	).Once()

	err = idfOracle.LoadSealedForReopen(context.Background(), 1, &querypb.SegmentLoadInfo{Bm25Logs: bm25LogsForField(104, remotePath)}, cm, false)
	require.NoError(t, err)
	current, err := idfOracle.current.GetStats(104)
	require.NoError(t, err)
	assert.Equal(t, int64(0), current.NumRow())

	idfOracle.SetNext(&snapshot{
		dist: []SnapshotItem{{
			NodeID:   1,
			Segments: []SegmentEntry{{NodeID: 1, SegmentID: 1, TargetVersion: 1}},
		}},
		targetVersion: 1,
	})
	require.Eventually(t, func() bool {
		return idfOracle.TargetVersion() >= 1
	}, time.Second, 10*time.Millisecond)

	current, err = idfOracle.current.GetStats(104)
	require.NoError(t, err)
	assert.Equal(t, int64(3), current.NumRow())
}

func TestRemoteFetchOnlyReopenMergesRemoteMetadataForMissingFields(t *testing.T) {
	setIDFRemoteFetchOnly(t, true)

	idfOracle := NewIDFOracle("test-channel", []*schemapb.FunctionSchema{{
		Type:           schemapb.FunctionType_BM25,
		InputFieldIds:  []int64{101},
		OutputFieldIds: []int64{102},
	}, {
		Type:           schemapb.FunctionType_BM25,
		InputFieldIds:  []int64{103},
		OutputFieldIds: []int64{104},
	}}).(*idfOracle)
	idfOracle.dirPath = t.TempDir()
	idfOracle.targetVersion.Store(1)
	idfOracle.Start()
	defer idfOracle.Close()

	oldStats := genBM25StatsForField(102, 1, 3)
	oldData, err := oldStats[102].Serialize()
	require.NoError(t, err)
	oldPath := "bm25stats/seg_1/field_102/0"

	newStats := genBM25StatsForField(104, 10, 13)
	newData, err := newStats[104].Serialize()
	require.NoError(t, err)
	newPath := "bm25stats/seg_1/field_104/0"

	cm := mocks.NewChunkManager(t)
	cm.EXPECT().Reader(mock.Anything, oldPath).RunAndReturn(
		func(context.Context, string) (storage.FileReader, error) {
			return &bytesFileReader{bytes.NewReader(oldData)}, nil
		},
	).Once()
	cm.EXPECT().Reader(mock.Anything, newPath).RunAndReturn(
		func(context.Context, string) (storage.FileReader, error) {
			return &bytesFileReader{bytes.NewReader(newData)}, nil
		},
	).Twice()

	err = idfOracle.LoadSealed(context.Background(), 1, &querypb.SegmentLoadInfo{Bm25Logs: bm25LogsForField(102, oldPath)}, cm)
	require.NoError(t, err)

	reopenInfo := &querypb.SegmentLoadInfo{
		Bm25Logs: append(
			bm25LogsForField(102, oldPath),
			bm25LogsForField(104, newPath)...,
		),
	}
	err = idfOracle.LoadSealedForReopen(context.Background(), 1, reopenInfo, cm, false)
	require.NoError(t, err)

	sealedStats, ok := idfOracle.sealed.Get(1)
	require.True(t, ok)
	fetched, err := sealedStats.FetchStats()
	require.NoError(t, err)
	assert.Equal(t, int64(2), fetched[102].NumRow())
	assert.Equal(t, int64(3), fetched[104].NumRow())
}

func TestRemoteFetchOnlyReopenCreatedSegmentActivatesFromRemote(t *testing.T) {
	setIDFRemoteFetchOnly(t, true)

	idfOracle := NewIDFOracle("test-channel", []*schemapb.FunctionSchema{{
		Type:           schemapb.FunctionType_BM25,
		InputFieldIds:  []int64{103},
		OutputFieldIds: []int64{104},
	}}).(*idfOracle)
	idfOracle.dirPath = t.TempDir()
	defer idfOracle.Close()

	newStats := genBM25StatsForField(104, 10, 13)
	data, err := newStats[104].Serialize()
	require.NoError(t, err)

	cm := mocks.NewChunkManager(t)
	remotePath := "bm25stats/seg_1/field_104/0"
	cm.EXPECT().Reader(mock.Anything, remotePath).RunAndReturn(
		func(context.Context, string) (storage.FileReader, error) {
			return &bytesFileReader{bytes.NewReader(data)}, nil
		},
	).Twice()

	err = idfOracle.LoadSealedForReopen(context.Background(), 1, &querypb.SegmentLoadInfo{Bm25Logs: bm25LogsForField(104, remotePath)}, cm, false)
	require.NoError(t, err)
	current, err := idfOracle.current.GetStats(104)
	require.NoError(t, err)
	assert.Equal(t, int64(0), current.NumRow())

	idfOracle.next.SetSnapshot(&snapshot{
		dist: []SnapshotItem{{
			NodeID:   1,
			Segments: []SegmentEntry{{NodeID: 1, SegmentID: 1, TargetVersion: 1}},
		}},
		targetVersion: 1,
	})
	require.NoError(t, idfOracle.SyncDistribution())

	current, err = idfOracle.current.GetStats(104)
	require.NoError(t, err)
	assert.Equal(t, int64(3), current.NumRow())
}

func TestSealedBM25StatsFieldTracking(t *testing.T) {
	stats := &sealedBm25Stats{
		activate:  atomic.NewBool(false),
		segmentID: 1,
		fieldList: []int64{102},
	}

	assert.True(t, stats.HasField(102))
	assert.False(t, stats.HasField(104))
	assert.ElementsMatch(t, []int64{102}, stats.FieldList())

	listOnlyStats := &sealedBm25Stats{fieldList: []int64{201}}
	assert.True(t, listOnlyStats.HasField(201))
	assert.False(t, listOnlyStats.HasField(202))

	stats.AddFields([]int64{104, 102})
	assert.True(t, stats.HasField(102))
	assert.True(t, stats.HasField(104))
	assert.ElementsMatch(t, []int64{102, 104}, stats.FieldList())
}

func TestIDFSyncFunctionsAddsNewBM25Field(t *testing.T) {
	idfOracle := NewIDFOracle("test-channel", []*schemapb.FunctionSchema{{
		Type:           schemapb.FunctionType_BM25,
		InputFieldIds:  []int64{101},
		OutputFieldIds: []int64{102},
	}}).(*idfOracle)
	idfOracle.dirPath = t.TempDir()
	idfOracle.Start()
	defer idfOracle.Close()

	oldStats := storage.NewBM25Stats()
	oldStats.Append(map[uint32]float32{1: 1})
	idfOracle.current[102] = oldStats

	err := idfOracle.SyncFunctions([]*schemapb.FunctionSchema{
		{
			Type:           schemapb.FunctionType_BM25,
			InputFieldIds:  []int64{101},
			OutputFieldIds: []int64{102},
		},
		{
			Type:           schemapb.FunctionType_BM25,
			InputFieldIds:  []int64{103},
			OutputFieldIds: []int64{104},
		},
	})
	require.NoError(t, err)

	existing, err := idfOracle.current.GetStats(102)
	require.NoError(t, err)
	assert.Same(t, oldStats, existing)
	added, err := idfOracle.current.GetStats(104)
	require.NoError(t, err)
	require.NotNil(t, added)
	assert.Equal(t, int64(0), added.NumRow())

	sparse := typeutil.CreateAndSortSparseFloatRow(map[uint32]float32{7: 1})
	_, avgdl, err := idfOracle.BuildIDF(104, &schemapb.SparseFloatArray{Contents: [][]byte{sparse}, Dim: 1})
	require.NoError(t, err)
	assert.Equal(t, float64(0), avgdl)
}

func TestBuildIDFNewFieldAfterSyncFunctions(t *testing.T) {
	idfOracle := NewIDFOracle("test-channel", []*schemapb.FunctionSchema{{
		Type:           schemapb.FunctionType_BM25,
		InputFieldIds:  []int64{101},
		OutputFieldIds: []int64{102},
	}}).(*idfOracle)
	idfOracle.dirPath = t.TempDir()
	idfOracle.Start()
	defer idfOracle.Close()

	sparse := typeutil.CreateAndSortSparseFloatRow(map[uint32]float32{7: 1})
	_, _, err := idfOracle.BuildIDF(104, &schemapb.SparseFloatArray{Contents: [][]byte{sparse}, Dim: 1})
	require.Error(t, err)

	err = idfOracle.SyncFunctions([]*schemapb.FunctionSchema{
		{
			Type:           schemapb.FunctionType_BM25,
			InputFieldIds:  []int64{101},
			OutputFieldIds: []int64{102},
		},
		{
			Type:           schemapb.FunctionType_BM25,
			InputFieldIds:  []int64{103},
			OutputFieldIds: []int64{104},
		},
	})
	require.NoError(t, err)

	_, avgdl, err := idfOracle.BuildIDF(104, &schemapb.SparseFloatArray{Contents: [][]byte{sparse}, Dim: 1})
	require.NoError(t, err)
	assert.Equal(t, float64(0), avgdl)
}

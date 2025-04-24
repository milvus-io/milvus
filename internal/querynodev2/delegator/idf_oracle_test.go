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
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type IDFOracleSuite struct {
	suite.Suite
	collectionID     int64
	collectionSchema *schemapb.CollectionSchema
	idfOracle        *idfOracle

	targetVersion int64
	snapshot      *snapshot
}

func (suite *IDFOracleSuite) SetupSuite() {
	suite.collectionID = 111
	suite.collectionSchema = &schemapb.CollectionSchema{
		Functions: []*schemapb.FunctionSchema{{
			Type:           schemapb.FunctionType_BM25,
			InputFieldIds:  []int64{101},
			OutputFieldIds: []int64{102},
		}},
	}
}

func (suite *IDFOracleSuite) SetupTest() {
	suite.idfOracle = NewIDFOracle(suite.collectionSchema.GetFunctions()).(*idfOracle)
	suite.snapshot = &snapshot{
		dist: []SnapshotItem{{1, make([]SegmentEntry, 0)}},
	}
	suite.targetVersion = 0
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
		suite.idfOracle.Register(segID, suite.genStats(uint32(segID), uint32(segID)+1), commonpb.SegmentState_Sealed)
	}

	// reduplicate register
	for _, segID := range sealedSegs {
		suite.idfOracle.Register(segID, suite.genStats(uint32(segID), uint32(segID)+1), commonpb.SegmentState_Sealed)
	}

	// register sealed segment but all deactvate
	suite.Zero(suite.idfOracle.current.NumRow())

	// update and sync snapshot make all sealed activate
	suite.updateSnapshot(sealedSegs, []int64{}, []int64{})
	suite.idfOracle.SyncDistribution(suite.snapshot)
	suite.Equal(int64(4), suite.idfOracle.current.NumRow())

	releasedSeg := []int64{1, 2, 3}
	suite.updateSnapshot([]int64{}, []int64{}, releasedSeg)
	suite.idfOracle.SyncDistribution(suite.snapshot)
	suite.Equal(int64(1), suite.idfOracle.current.NumRow())

	sparse := typeutil.CreateAndSortSparseFloatRow(map[uint32]float32{4: 1})
	bytes, avgdl, err := suite.idfOracle.BuildIDF(102, &schemapb.SparseFloatArray{Contents: [][]byte{sparse}, Dim: 1})
	suite.NoError(err)
	suite.Equal(float64(1), avgdl)
	suite.Equal(map[uint32]float32{4: 0.2876821}, typeutil.SparseFloatBytesToMap(bytes[0]))
}

func (suite *IDFOracleSuite) TestGrow() {
	// register grow
	growSegs := []int64{1, 2, 3, 4}
	for _, segID := range growSegs {
		suite.idfOracle.Register(segID, suite.genStats(uint32(segID), uint32(segID)+1), commonpb.SegmentState_Growing)
	}
	// reduplicate register
	for _, segID := range growSegs {
		suite.idfOracle.Register(segID, suite.genStats(uint32(segID), uint32(segID)+1), commonpb.SegmentState_Growing)
	}

	// register sealed segment but all deactvate
	suite.Equal(int64(4), suite.idfOracle.current.NumRow())
	suite.updateSnapshot([]int64{}, growSegs, []int64{})

	releasedSeg := []int64{1, 2, 3}
	suite.updateSnapshot([]int64{}, []int64{}, releasedSeg)
	suite.idfOracle.SyncDistribution(suite.snapshot)
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

	suite.NotPanics(func() {
		stats.Merge(map[int64]*storage.BM25Stats{103: storage.NewBM25Stats()})
	})

	suite.Panics(func() {
		stats.Minus(map[int64]*storage.BM25Stats{104: storage.NewBM25Stats()})
	})

	_, err := stats.GetStats(104)
	suite.Error(err)

	_, err = stats.GetStats(102)
	suite.NoError(err)
}

func TestIDFOracle(t *testing.T) {
	suite.Run(t, new(IDFOracleSuite))
}

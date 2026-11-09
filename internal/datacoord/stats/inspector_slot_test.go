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

package stats

import (
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	ctask "github.com/milvus-io/milvus/internal/datacoord/compaction/task"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	taskcommon "github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func (s *statsInspectorSuite) TestReloadFromMetaEstimatesPerSubJobType() {
	// A recovered manifest-backed V3 segment carries no FieldBinlog arrays
	// (the catalog does not persist them), so recovered stats tasks keep the
	// Stats-derived whole segment size — doubled for the json key index job —
	// and recovery must not resolve collections through the handler
	// (rootcoord).
	const (
		collectionID = UniqueID(5)
		segmentID    = UniqueID(50)
		pkFieldID    = int64(500)
		jsonFieldID  = int64(501)
		numRows      = int64(1000)
		segmentSize  = int64(2 * 1024 * 1024 * 1024)
	)

	s.mt.AddCollection(&meta.CollectionInfo{
		ID: collectionID,
		Schema: &schemapb.CollectionSchema{
			ExternalSource: "s3://external",
			ExternalSpec:   `{"format":"parquet"}`,
			Fields: []*schemapb.FieldSchema{
				{FieldID: pkFieldID, Name: "pk", DataType: schemapb.DataType_Int64, Nullable: true, ExternalField: "pk_col"},
				{FieldID: jsonFieldID, Name: "json", DataType: schemapb.DataType_JSON, Nullable: true, ExternalField: "json_col"},
			},
		},
	})
	// no GetCollection expectation: any handler resolution fails the test
	s.inspector.handler = meta.NewNMockHandler(s.T())

	segment := &meta.SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             segmentID,
			CollectionID:   collectionID,
			PartitionID:    3,
			State:          commonpb.SegmentState_Flushed,
			NumOfRows:      numRows,
			StorageVersion: storage.StorageV3,
			ManifestPath:   packed.MarshalManifestPath("files/insert_log/5/3/50", 1),
			Stats:          &datapb.Statistics{InsertBinlogSize: segmentSize},
		},
	}
	s.mt.Segments.SetSegment(segmentID, segment)

	for taskID, subJobType := range map[UniqueID]indexpb.StatsSubJob{
		2001: indexpb.StatsSubJob_JsonKeyIndexJob,
		2002: indexpb.StatsSubJob_Sort,
	} {
		s.stm.tasks.Insert(taskID, &indexpb.StatsTask{
			CollectionID: collectionID,
			TaskID:       taskID,
			SegmentID:    segmentID,
			SubJobType:   subJobType,
			State:        indexpb.JobState_JobStateInProgress,
		})
	}

	slots := make(map[indexpb.StatsSubJob]int64)
	scheduler := task.NewMockGlobalScheduler(s.T())
	scheduler.EXPECT().Enqueue(mock.Anything).Run(func(t task.Task) {
		st := t.(*statsTask)
		slots[st.GetSubJobType()] = t.GetTaskSlot()
	}).Return()
	s.inspector.scheduler = scheduler

	s.inspector.ReloadFromMeta()

	// without binlogs to attribute, the json key index task keeps the whole
	// segment size doubled, and the sort task the whole segment size
	s.Equal(ctask.CalculateStatsTaskSlot(segmentSize*2), slots[indexpb.StatsSubJob_JsonKeyIndexJob])
	s.Equal(ctask.CalculateStatsTaskSlot(segmentSize), slots[indexpb.StatsSubJob_Sort])
}

func (s *statsInspectorSuite) TestStatsTaskFieldIDs() {
	// collection 2 declares one json field (202) and one match enabled varchar
	// (201) among a pk.
	coll := s.mt.GetCollection(2)
	s.Equal([]int64{202}, statsTaskFieldIDs(coll.Schema, indexpb.StatsSubJob_JsonKeyIndexJob))
	s.Equal([]int64{201}, statsTaskFieldIDs(coll.Schema, indexpb.StatsSubJob_TextIndexJob))
	// a sort task reads every column
	s.Nil(statsTaskFieldIDs(coll.Schema, indexpb.StatsSubJob_Sort))

	// collection 1 has no json field at all.
	coll = s.mt.GetCollection(1)
	s.Empty(statsTaskFieldIDs(coll.Schema, indexpb.StatsSubJob_JsonKeyIndexJob))
}

func (s *statsInspectorSuite) TestGetCollectionWithoutMeta() {
	inspector := &Inspector{}
	s.Nil(inspector.getCollection(1))
	s.False(inspector.isExternalCollection(1))
}

const (
	slotTestCollectionID = UniqueID(4)
	slotTestNumRows      = int64(1000)
	slotTestPKFieldID    = int64(300)
	slotTestTextFieldID  = int64(301)
	slotTestJSONFieldID  = int64(302)
	slotTestGroupSize    = int64(100 * 1024 * 1024)
)

// slotTestInternalSchema is an internal storage v3 collection whose short
// columns share one column group: the non-nullable pk is exact and deductible
// from the group budget.
func slotTestInternalSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: slotTestPKFieldID, Name: "pk", DataType: schemapb.DataType_Int64},
			{
				FieldID:  slotTestTextFieldID,
				Name:     "var",
				DataType: schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.MaxLengthKey, Value: "1024"},
					{Key: "enable_match", Value: "true"},
					{Key: "enable_analyzer", Value: "true"},
				},
			},
			{FieldID: slotTestJSONFieldID, Name: "json", DataType: schemapb.DataType_JSON},
		},
	}
}

func newSlotTestSegment(segmentID, collID UniqueID) *meta.SegmentInfo {
	return &meta.SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             segmentID,
			CollectionID:   collID,
			PartitionID:    2,
			State:          commonpb.SegmentState_Flushed,
			IsSorted:       true,
			Level:          datapb.SegmentLevel_L1,
			NumOfRows:      slotTestNumRows,
			StorageVersion: storage.StorageV3,
			ManifestPath:   "manifest.json",
			// one column group holding every column at once, like the
			// synthetic group external segments report
			Binlogs: []*datapb.FieldBinlog{
				{
					FieldID:     0,
					ChildFields: []int64{slotTestPKFieldID, slotTestTextFieldID, slotTestJSONFieldID},
					Binlogs: []*datapb.Binlog{
						{EntriesNum: slotTestNumRows, LogSize: slotTestGroupSize, MemorySize: slotTestGroupSize},
					},
				},
			},
			Stats: &datapb.Statistics{InsertBinlogSize: slotTestGroupSize},
		},
	}
}

func (s *statsInspectorSuite) TestEstimateStatsTaskSize() {
	schema := slotTestInternalSchema()
	s.mt.AddCollection(&meta.CollectionInfo{ID: slotTestCollectionID, Schema: schema})

	perRecord := func(field *schemapb.FieldSchema) int64 {
		size, err := typeutil.EstimateSizePerRecord(&schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{field},
		})
		s.NoError(err)
		return int64(size)
	}
	pkSize := perRecord(schema.Fields[0])
	// both variable width columns of the group, only used to assert the
	// estimation is not the raw schema guess
	schemaGuess := perRecord(schema.Fields[1]) + perRecord(schema.Fields[2])
	// bytes the two variable width columns really take in total
	residualBytes := slotTestGroupSize - pkSize*slotTestNumRows
	s.Greater(residualBytes, schemaGuess*slotTestNumRows)

	newSegment := func(collID UniqueID) *meta.SegmentInfo {
		return newSlotTestSegment(30, collID)
	}

	s.Run("sort job reads the whole segment", func() {
		segment := newSegment(slotTestCollectionID)
		s.Equal(segment.GetSegmentSize(), s.inspector.estimateStatsTaskSize(s.mt.GetCollection(segment.GetCollectionID()), segment, indexpb.StatsSubJob_Sort))
	})

	s.Run("json job drops the fixed width columns of the group", func() {
		// the json column is charged the whole measured residual of the group,
		// i.e. everything but the pk, doubled for the index it writes
		segment := newSegment(slotTestCollectionID)
		expected := residualBytes * 2
		s.Equal(expected, s.inspector.estimateStatsTaskSize(s.mt.GetCollection(segment.GetCollectionID()), segment, indexpb.StatsSubJob_JsonKeyIndexJob))
		s.Less(expected, segment.GetSegmentSize()*2)
	})

	s.Run("text job is not doubled", func() {
		segment := newSegment(slotTestCollectionID)
		expected := residualBytes
		s.Equal(expected, s.inspector.estimateStatsTaskSize(s.mt.GetCollection(segment.GetCollectionID()), segment, indexpb.StatsSubJob_TextIndexJob))
		s.Less(expected, segment.GetSegmentSize())
	})

	s.Run("storage v2 falls back to the whole segment", func() {
		segment := newSegment(slotTestCollectionID)
		segment.StorageVersion = storage.StorageV2
		segment.ManifestPath = ""
		s.Equal(segment.GetSegmentSize()*2, s.inspector.estimateStatsTaskSize(
			s.mt.GetCollection(segment.GetCollectionID()), segment, indexpb.StatsSubJob_JsonKeyIndexJob))
	})

	s.Run("collection without the indexed column falls back to segment size", func() {
		segment := newSegment(1)
		s.Equal(segment.GetSegmentSize()*2, s.inspector.estimateStatsTaskSize(s.mt.GetCollection(segment.GetCollectionID()), segment, indexpb.StatsSubJob_JsonKeyIndexJob))
	})

	s.Run("unresolvable collection falls back to segment size", func() {
		segment := newSegment(999)
		s.Equal(segment.GetSegmentSize()*2, s.inspector.estimateStatsTaskSize(s.mt.GetCollection(segment.GetCollectionID()), segment, indexpb.StatsSubJob_JsonKeyIndexJob))
	})

	s.Run("collection without schema falls back to segment size", func() {
		segment := newSegment(slotTestCollectionID)
		s.Equal(segment.GetSegmentSize()*2, s.inspector.estimateStatsTaskSize(&meta.CollectionInfo{ID: slotTestCollectionID}, segment, indexpb.StatsSubJob_JsonKeyIndexJob))
	})

	s.Run("recovered segment without binlogs falls back to segment size", func() {
		segment := newSegment(slotTestCollectionID)
		segment.Binlogs = nil
		s.Equal(segment.GetSegmentSize()*2, s.inspector.estimateStatsTaskSize(s.mt.GetCollection(segment.GetCollectionID()), segment, indexpb.StatsSubJob_JsonKeyIndexJob))
	})

	s.Run("segment without the field binlog falls back to segment size", func() {
		segment := newSegment(slotTestCollectionID)
		segment.Binlogs[0].ChildFields = []int64{slotTestPKFieldID}
		s.Equal(segment.GetSegmentSize()*2, s.inspector.estimateStatsTaskSize(s.mt.GetCollection(segment.GetCollectionID()), segment, indexpb.StatsSubJob_JsonKeyIndexJob))
	})

	s.Run("non milvus-table external collection keeps the whole segment", func() {
		// The only schema shape production can produce for a non milvus-table
		// external collection: source and spec both set, every user field
		// nullable (Pass 2 of NormalizeAndValidateExternalCollectionSchema).
		// With nothing exact to deduct, a variable length target column is
		// charged the whole synthetic group, i.e. the pre-attribution size.
		const externalCollID = UniqueID(7)
		externalSchema := &schemapb.CollectionSchema{
			ExternalSource: "s3://external",
			ExternalSpec:   `{"format":"parquet"}`,
			Fields: []*schemapb.FieldSchema{
				{FieldID: slotTestPKFieldID, Name: "pk", DataType: schemapb.DataType_Int64, Nullable: true, ExternalField: "pk_col"},
				{
					FieldID:       slotTestTextFieldID,
					Name:          "var",
					DataType:      schemapb.DataType_VarChar,
					Nullable:      true,
					ExternalField: "var_col",
					TypeParams: []*commonpb.KeyValuePair{
						{Key: common.MaxLengthKey, Value: "1024"},
						{Key: "enable_match", Value: "true"},
						{Key: "enable_analyzer", Value: "true"},
					},
				},
				{FieldID: slotTestJSONFieldID, Name: "json", DataType: schemapb.DataType_JSON, Nullable: true, ExternalField: "json_col"},
			},
		}
		s.mt.AddCollection(&meta.CollectionInfo{ID: externalCollID, Schema: externalSchema})

		segment := newSegment(externalCollID)
		s.Equal(slotTestGroupSize, s.inspector.estimateStatsTaskSize(
			s.mt.GetCollection(segment.GetCollectionID()), segment, indexpb.StatsSubJob_TextIndexJob))
		s.Equal(slotTestGroupSize*2, s.inspector.estimateStatsTaskSize(
			s.mt.GetCollection(segment.GetCollectionID()), segment, indexpb.StatsSubJob_JsonKeyIndexJob))
	})
}

func (s *statsInspectorSuite) TestSubmitStatsTaskUsesFieldEstimate() {
	// SubmitStatsTask sizes the task by the projected json columns of the
	// segment instead of the whole segment.
	const segmentID = UniqueID(40)
	s.mt.AddCollection(&meta.CollectionInfo{ID: slotTestCollectionID, Schema: slotTestInternalSchema()})
	segment := newSlotTestSegment(segmentID, slotTestCollectionID)
	s.mt.Segments.SetSegment(segmentID, segment)

	var enqueuedSlot int64
	scheduler := task.NewMockGlobalScheduler(s.T())
	scheduler.EXPECT().GetPendingTaskCount(taskcommon.Stats).Return(0).Once()
	scheduler.EXPECT().Enqueue(mock.Anything).Run(func(t task.Task) {
		enqueuedSlot = t.GetTaskSlot()
	}).Return().Once()
	s.inspector.scheduler = scheduler

	s.NoError(s.inspector.SubmitStatsTask(segmentID, segmentID, indexpb.StatsSubJob_JsonKeyIndexJob, true, nil))

	expected := ctask.CalculateStatsTaskSlot(s.inspector.estimateStatsTaskSize(
		s.mt.GetCollection(slotTestCollectionID), segment, indexpb.StatsSubJob_JsonKeyIndexJob))
	s.Equal(expected, enqueuedSlot)
	s.Less(s.inspector.estimateStatsTaskSize(s.mt.GetCollection(slotTestCollectionID), segment, indexpb.StatsSubJob_JsonKeyIndexJob),
		segment.GetSegmentSize()*2)
}

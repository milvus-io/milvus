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

package cmek

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/tests/integration/cmek/inspector"
)

type propagationAnalyzeSuite struct {
	rawDataSuite
	fixture    propagationFixture
	faultStage string
}

func (s *propagationAnalyzeSuite) SetupSuite() {
	s.fixture.dir = s.T().TempDir()
	s.WithMilvusConfig("MILVUS_CMEK_FIXTURE_CONTROL", s.fixture.dir)
	s.configurePropagationCompaction()
	// Preserve the existing three-retry clustering budget, while leaving a
	// dispatch interval in which to replace the node after Analyze completes.
	s.WithMilvusConfig("indexCoord.scheduler.interval", "5000")
	s.WithMilvusConfig("common.enableVectorClusteringKey", "true")
	s.WithMilvusConfig("dataCoord.compaction.clustering.minCentroidsNum", "2")
	s.WithMilvusConfig("dataCoord.compaction.clustering.maxSegmentSizeRatio", "0.0625")
	s.WithMilvusConfig("dataCoord.compaction.clustering.preferSegmentSizeRatio", "0.0625")
	s.physicalIndexThreshold = 8192
	s.setupRawData(3)
	s.Cluster.MustModifyMilvusConfig(map[string]string{"dataCoord.enableCompaction": "false"})
}

func (s *propagationAnalyzeSuite) analyzeTasks(ctx context.Context, collectionID int64) map[int64]*indexpb.AnalyzeTask {
	tasks := make(map[int64]*indexpb.AnalyzeTask)
	for _, task := range readPropagationRecords(ctx, &s.rawDataSuite, "datacoord-meta/analyze-task", func() *indexpb.AnalyzeTask { return &indexpb.AnalyzeTask{} }) {
		if task.GetCollectionID() == collectionID {
			tasks[task.GetTaskID()] = task
		}
	}
	return tasks
}

func (s *propagationAnalyzeSuite) TestVectorClustering() {
	ctx, cancel := context.WithTimeout(s.Cluster.GetContext(), 9*time.Minute)
	defer cancel()
	defer s.fixture.restore(s.T())
	campaign := consumerCampaign("analyze")
	for _, field := range campaign.schema.GetFields() {
		if field.GetName() == "float_vector" {
			field.IsClusteringKey = true
		}
	}
	description := s.prepareRawDataCollection(ctx, campaign)
	collection, collectionID := description.GetCollectionName(), description.GetCollectionID()
	fields, expected := consumerBatch(0, 4096, 1)
	s.insertRawDataBatch(ctx, description, fields, len(expected))
	s.flushConsumerCollection(ctx, description)
	inputs := s.sortPropagationInputs(ctx, collection, 1)
	s.WaitForIndexBuiltWithDB(ctx, s.dbName, collection, "float_vector")
	s.inspectPropagationRaw(ctx, inputs, collectionID)
	// Segment size must be an integral MiB. A 1/16 clustering size ratio gives
	// 4096 * 8 * sizeof(float) / 64 KiB = two actual centroids, avoiding the
	// production "fake finished" Analyze branch for small collections.
	revert := s.Cluster.MustModifyMilvusConfig(map[string]string{"dataCoord.segment.maxSize": "1"})
	defer revert()
	analyzeNode := s.coldDataNode(s.fixture)
	analyzeFault := keyFaultControl{Role: "mixcoord", EZID: s.ezID, CollectionID: collectionID, Operation: "unsafe", Caller: "(*analyzeTask).CreateTaskOnWorker"}
	compactFault := keyFaultControl{Role: "mixcoord", EZID: s.ezID, CollectionID: collectionID, Operation: "unsafe", Caller: "(*clusteringCompactionTask).BuildCompactionRequest"}
	initialFault := compactFault
	if s.faultStage == "analyze" {
		initialFault = analyzeFault
	}
	s.fixture.setFault(s.T(), initialFault)
	prefixes := []string{fmt.Sprintf("insert_log/%d", collectionID), fmt.Sprintf("stats_log/%d", collectionID), "analyze_stats", "part_stats"}
	before := s.objectSnapshot(ctx, prefixes...)
	s.Require().NotEmpty(before, "snapshot must include the existing raw input objects")
	response, err := s.Cluster.MilvusClient.ManualCompaction(ctx, &milvuspb.ManualCompactionRequest{CollectionID: collectionID, MajorCompaction: true})
	s.Require().NoError(merr.CheckRPCCall(response, err))
	s.Require().Positive(response.GetCompactionID())
	var blocked map[int64]*indexpb.AnalyzeTask
	if s.faultStage == "analyze" {
		s.fixture.waitForRetry(s.T(), analyzeFault, func() {
			s.Require().True(maps.Equal(before, s.objectSnapshot(ctx, prefixes...)))
			s.fixture.assertNotRegistered(s.T(), analyzeNode, s.ezID)
			blocked = s.analyzeTasks(ctx, collectionID)
			for _, task := range blocked {
				s.Require().NotEqual(indexpb.JobState_JobStateFinished, task.GetState())
				s.Require().Empty(task.GetCentroidsFile())
			}
		})
		s.Require().NotEmpty(blocked)
		// Restore Analyze while holding the independently dispatched writer.
		s.fixture.setFault(s.T(), compactFault)
	}
	var analyzed *indexpb.AnalyzeTask
	s.Require().Eventually(func() bool {
		for _, task := range s.analyzeTasks(ctx, collectionID) {
			s.Require().NotEqual(indexpb.JobState_JobStateFailed, task.GetState(), task.GetFailReason())
			if task.GetState() == indexpb.JobState_JobStateFinished {
				analyzed = task
				return true
			}
		}
		return false
	}, 2*time.Minute, 100*time.Millisecond)
	s.Require().NotNil(analyzed)
	if blocked != nil {
		s.Require().Contains(blocked, analyzed.GetTaskID())
	}
	s.Require().NotEmpty(analyzed.GetCentroidsFile(), "Analyze skipped input instead of training")
	centroids, err := s.Cluster.ChunkManager.Read(ctx, analyzed.GetCentroidsFile())
	s.Require().NoError(err)
	s.Require().NotEmpty(centroids)
	s.Require().Equal([]int64{inputs[0].GetID()}, analyzed.GetSegmentIDs())
	s.fixture.assertRegistration(s.T(), analyzeNode, s.ezID, collectionID)
	s.T().Logf("stage=analyze task=%d version=%d node=%d centroid=%s", analyzed.GetTaskID(), analyzed.GetVersion(), analyzeNode, analyzed.GetCentroidsFile())
	s.Require().Eventually(func() bool { return len(s.fixture.faultHits(s.T(), compactFault)) > 0 }, time.Minute, 100*time.Millisecond)
	// No target writer has run. The replacement cannot inherit Analyze's key.
	before = s.objectSnapshot(ctx, prefixes...)
	compactNode := s.coldDataNode(s.fixture)
	s.fixture.assertNotRegistered(s.T(), compactNode, s.ezID)
	if s.faultStage != "clustering" {
		s.fixture.restore(s.T())
	}
	var plan *datapb.CompactionTask
	s.Require().Eventually(func() bool {
		for _, candidate := range s.propagationPlans(ctx, collectionID, datapb.CompactionType_ClusteringCompaction) {
			if candidate.GetTriggerID() != response.GetCompactionID() {
				continue
			}
			if s.faultStage == "clustering" {
				s.Require().True(maps.Equal(before, s.objectSnapshot(ctx, prefixes...)))
				s.fixture.assertNotRegistered(s.T(), compactNode, s.ezID)
				if candidate.GetFailReason() != "" {
					plan = candidate
					return true
				}
			} else {
				s.Require().Empty(candidate.GetFailReason())
				if candidate.GetState() == datapb.CompactionTaskState_completed || candidate.GetState() == datapb.CompactionTaskState_cleaned {
					plan = candidate
					return true
				}
			}
		}
		return false
	}, 3*time.Minute, 100*time.Millisecond)
	s.Require().NotNil(plan)
	s.Require().Equal(analyzed.GetTaskID(), plan.GetAnalyzeTaskID())
	s.T().Logf("stage=clustering plan=%d analyze=%d node=%d state=%s output=%v", plan.GetPlanID(), plan.GetAnalyzeTaskID(), plan.GetNodeID(), plan.GetState(), plan.GetResultSegments())
	if s.faultStage == "clustering" {
		s.Require().Empty(plan.GetResultSegments())
		s.Require().Contains(plan.GetFailReason(), "cannot get ez key")
		return // Exhausting the existing retry budget may fail this task.
	}
	s.Require().Equal(compactNode, plan.GetNodeID())
	outputs := s.propagationSegments(ctx, collection)
	var ids []int64
	var rows int64
	for _, segment := range outputs {
		ids = append(ids, segment.GetID())
		rows += segment.GetNumOfRows()
		s.Require().NotEqual(inputs[0].GetID(), segment.GetID())
	}
	s.Require().ElementsMatch(plan.GetResultSegments(), ids)
	s.Require().Equal(int64(len(expected)), rows)
	s.Require().True(slices.Contains(plan.GetInputSegments(), inputs[0].GetID()))
	s.inspectPropagationRaw(ctx, outputs, collectionID)
	s.fixture.assertRegistration(s.T(), compactNode, s.ezID, collectionID)
	s.reloadPropagation(ctx, description, outputs)
	s.assertConsumerRows(ctx, collection, expected)
}

type propagationCompactionSuite struct {
	rawDataSuite
	fixture propagationFixture
	version int64
	kind    datapb.CompactionType
	fault   bool
}

func (s *propagationCompactionSuite) SetupSuite() {
	s.fixture.dir = s.T().TempDir()
	s.WithMilvusConfig("MILVUS_CMEK_FIXTURE_CONTROL", s.fixture.dir)
	s.configurePropagationCompaction()
	s.WithMilvusConfig("dataCoord.segment.maxSize", "64")
	s.setupRawData(s.version)
	s.Cluster.MustModifyMilvusConfig(map[string]string{"dataCoord.enableCompaction": "false"})
}

func (s *rawDataSuite) inspectPropagationText(ctx context.Context, segment *datapb.SegmentInfo, fieldID, buildID int64) {
	stats := segment.GetTextStatsLogs()[fieldID]
	s.Require().NotNil(stats)
	s.Require().Equal(buildID, stats.GetBuildID())
	s.Require().NotEmpty(stats.GetFiles())
	objects, err := inspector.LocateTextLog(s.Cluster.RootPath(), []*datapb.SegmentInfo{segment}, fieldID, stats.GetCurrentScalarIndexVersion())
	s.Require().NoError(err)
	paths := make([]string, 0, len(objects))
	for _, object := range objects {
		paths = append(paths, object.Path)
		raw, err := s.Cluster.ChunkManager.Read(ctx, object.Path)
		s.Require().NoError(err, object.Path)
		if stats.GetCurrentScalarIndexVersion() >= 3 {
			err = inspector.InspectV3(raw, s.ezID)
		} else {
			err = inspector.InspectV2(raw, s.ezID)
		}
		s.Require().NoError(err, "Text Stats build=%d field=%d object=%s", buildID, fieldID, object.Path)
	}
	if segment.GetStorageVersion() == 3 {
		manifestStats, err := packed.GetManifestStats(segment.GetManifestPath(), packed.CreateStorageConfig())
		s.Require().NoError(err)
		key := fmt.Sprintf("text_index.%d", fieldID)
		s.Require().Contains(manifestStats, key)
		entry := manifestStats[key]
		s.Require().Equal(strconv.FormatInt(buildID, 10), entry.Metadata["build_id"])
		s.Require().Equal(strconv.FormatInt(stats.GetVersion(), 10), entry.Metadata["version"])
		s.Require().ElementsMatch(paths, entry.Paths)
	}
	s.T().Logf("stage=text-stats segment=%d field=%d build=%d files=%v", segment.GetID(), fieldID, buildID, paths)
}

func (s *propagationCompactionSuite) TestRewrite() {
	ctx, cancel := context.WithTimeout(s.Cluster.GetContext(), 7*time.Minute)
	defer cancel()
	defer s.fixture.restore(s.T())
	campaign := consumerCampaign("compaction")
	for _, field := range campaign.schema.GetFields() {
		if field.GetName() == "payload" {
			field.TypeParams = append(field.TypeParams, &commonpb.KeyValuePair{Key: "enable_analyzer", Value: "true"}, &commonpb.KeyValuePair{Key: "enable_match", Value: "true"})
		}
	}
	description := s.prepareRawDataCollection(ctx, campaign)
	collection, collectionID := description.GetCollectionName(), description.GetCollectionID()
	textID := requestedFieldIDs(description.GetSchema(), []string{"payload"})[0]
	batches := 1
	if s.kind == datapb.CompactionType_MixCompaction {
		batches = 2
	}
	expected := make(map[int64]consumerRow)
	var inputs []*datapb.SegmentInfo
	for batch := 0; batch < batches; batch++ {
		fields, rows := consumerBatch(batch*128, 128, int64(batch+1))
		maps.Copy(expected, rows)
		s.insertRawDataBatch(ctx, description, fields, len(rows))
		inputs = append(inputs, s.flushConsumerCollection(ctx, description)...)
	}
	if s.kind == datapb.CompactionType_MixCompaction {
		inputs = s.sortPropagationInputs(ctx, collection, 2)
	}
	s.WaitForIndexBuiltWithDB(ctx, s.dbName, collection, "float_vector")
	s.inspectPropagationRaw(ctx, inputs, collectionID)
	inputIDs := make([]int64, 0, len(inputs))
	for _, segment := range inputs {
		inputIDs = append(inputIDs, segment.GetID())
	}
	baseline := s.propagationPlans(ctx, collectionID, s.kind)
	nodeID := s.coldDataNode(s.fixture)
	prefixes := []string{fmt.Sprintf("insert_log/%d", collectionID), fmt.Sprintf("stats_log/%d", collectionID), "text_log"}
	before := s.objectSnapshot(ctx, prefixes...)
	s.Require().NotEmpty(before, "snapshot must include the existing raw input objects")
	fault := keyFaultControl{Role: "mixcoord", EZID: s.ezID, CollectionID: collectionID, Operation: "unsafe", Caller: "(*mixCompactionTask).BuildCompactionRequest"}
	if s.fault {
		s.fixture.setFault(s.T(), fault)
	}
	if s.kind == datapb.CompactionType_SortCompaction {
		revert := s.Cluster.MustModifyMilvusConfig(map[string]string{"dataCoord.enableCompaction": "true", "dataCoord.compaction.enableAutoCompaction": "true"})
		defer revert()
	} else {
		response, err := s.Cluster.MilvusClient.ManualCompaction(ctx, &milvuspb.ManualCompactionRequest{CollectionID: collectionID, TargetSize: 64})
		s.Require().NoError(merr.CheckRPCCall(response, err))
		s.Require().Positive(response.GetCompactionID())
	}
	var plan *datapb.CompactionTask
	s.Require().Eventually(func() bool {
		for id, current := range s.propagationPlans(ctx, collectionID, s.kind) {
			if _, old := baseline[id]; old {
				continue
			}
			if len(current.GetInputSegments()) == 0 || !slices.Contains(inputIDs, current.GetInputSegments()[0]) {
				continue
			}
			if s.fault {
				if current.GetState() == datapb.CompactionTaskState_failed || current.GetFailReason() != "" {
					plan = current
					return true
				}
			} else {
				s.Require().Empty(current.GetFailReason())
				if current.GetState() == datapb.CompactionTaskState_completed || current.GetState() == datapb.CompactionTaskState_cleaned {
					plan = current
					return true
				}
			}
		}
		return false
	}, 2*time.Minute, 100*time.Millisecond)
	s.Require().NotNil(plan)
	s.Require().ElementsMatch(inputIDs, plan.GetInputSegments())
	s.T().Logf("stage=compaction type=%s plan=%d node=%d input=%v output=%v state=%s", s.kind, plan.GetPlanID(), plan.GetNodeID(), plan.GetInputSegments(), plan.GetResultSegments(), plan.GetState())
	if s.fault {
		s.Require().NotEmpty(s.fixture.faultHits(s.T(), fault))
		s.Require().Empty(plan.GetResultSegments())
		s.Require().True(maps.Equal(before, s.objectSnapshot(ctx, prefixes...)))
		for _, event := range s.fixture.observations(s.T(), "go") {
			s.Require().False(event.NodeID == nodeID && event.EZID == s.ezID && event.Operation == "register")
		}
		return
	}
	s.Require().Equal(nodeID, plan.GetNodeID())
	outputs := s.propagationSegments(ctx, collection)
	s.Require().Len(outputs, 1)
	s.Require().ElementsMatch(plan.GetResultSegments(), []int64{outputs[0].GetID()})
	s.Require().NotContains(inputIDs, outputs[0].GetID())
	s.Require().Equal(int64(len(expected)), outputs[0].GetNumOfRows())
	s.inspectPropagationRaw(ctx, outputs, collectionID)
	s.inspectPropagationText(ctx, outputs[0], textID, plan.GetPlanID())
	s.fixture.assertRegistration(s.T(), nodeID, s.ezID, collectionID)
	s.reloadPropagation(ctx, description, outputs)
	s.assertConsumerRows(ctx, collection, expected)
	ids := make([]int64, 0, len(expected))
	for id := range expected {
		ids = append(ids, id)
	}
	s.assertPropagationQuery(ctx, collection, "TEXT_MATCH(payload, 'batch')", ids)
	s.T().Logf("storage=%s collection=%d rows=%d", strconv.FormatInt(s.version, 10), collectionID, len(expected))
}

func propagationTextField(name string) *schemapb.FieldSchema {
	return &schemapb.FieldSchema{
		Name: name, DataType: schemapb.DataType_VarChar, Nullable: true,
		DefaultValue: &schemapb.ValueField{Data: &schemapb.ValueField_StringData{StringData: "propagationtoken"}},
		TypeParams:   []*commonpb.KeyValuePair{{Key: "max_length", Value: "128"}, {Key: "enable_analyzer", Value: "true"}, {Key: "enable_match", Value: "true"}},
	}
}

type propagationSchemaSuite struct {
	rawDataSuite
	fixture    propagationFixture
	rewrite    bool
	faultStage string
}

func (s *propagationSchemaSuite) SetupSuite() {
	s.fixture.dir = s.T().TempDir()
	s.WithMilvusConfig("MILVUS_CMEK_FIXTURE_CONTROL", s.fixture.dir)
	s.configurePropagationCompaction()
	s.setupRawData(3)
	s.Cluster.MustModifyMilvusConfig(map[string]string{"dataCoord.enableCompaction": "false"})
}

func (s *rawDataSuite) propagationColumns(ctx context.Context, segments []*datapb.SegmentInfo, collectionID int64) map[string][]string {
	refs, err := inspector.LocateManifestsV3(segments, collectionID)
	s.Require().NoError(err)
	columns := make(map[string][]string)
	for _, ref := range refs {
		raw, err := s.Cluster.ChunkManager.Read(ctx, ref.Locator.ObjectPath())
		s.Require().NoError(err)
		objects, err := inspector.ParseParquetObjectsV3(raw, ref.Locator.BasePath)
		s.Require().NoError(err)
		for _, object := range objects {
			columns[object.Path] = object.Columns
		}
	}
	return columns
}

func (s *propagationSchemaSuite) TestBackfill() {
	ctx, cancel := context.WithTimeout(s.Cluster.GetContext(), 8*time.Minute)
	defer cancel()
	defer s.fixture.restore(s.T())
	campaign := consumerCampaign("schema")
	campaign.schema.Fields = append(campaign.schema.Fields, &schemapb.FieldSchema{Name: "obsolete", DataType: schemapb.DataType_Int64})
	description := s.prepareRawDataCollection(ctx, campaign)
	collection, collectionID := description.GetCollectionName(), description.GetCollectionID()
	oldFieldID := requestedFieldIDs(description.GetSchema(), []string{"obsolete"})[0]
	fields, expected := consumerBatch(0, 128, 1)
	oldValues := make([]int64, 128)
	for i := range oldValues {
		oldValues[i] = int64(i + 1000)
	}
	fields = append(fields, &schemapb.FieldData{FieldName: "obsolete", Type: schemapb.DataType_Int64, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: oldValues}}}}})
	s.insertRawDataBatch(ctx, description, fields, len(expected))
	s.flushConsumerCollection(ctx, description)
	inputs := s.sortPropagationInputs(ctx, collection, 1)
	s.WaitForIndexBuiltWithDB(ctx, s.dbName, collection, "float_vector")
	s.inspectPropagationRaw(ctx, inputs, collectionID)
	oldColumns := s.propagationColumns(ctx, inputs, collectionID)
	oldHasB := false
	for _, columns := range oldColumns {
		oldHasB = oldHasB || slices.Contains(columns, strconv.FormatInt(oldFieldID, 10))
	}
	s.Require().True(oldHasB, "rewrite input must physically contain the removed field")
	baseline := s.propagationPlans(ctx, collectionID, datapb.CompactionType_BumpSchemaVersionCompaction)
	if s.rewrite {
		response, err := s.Cluster.MilvusClient.AlterCollectionSchema(ctx, &milvuspb.AlterCollectionSchemaRequest{
			DbName: s.dbName, CollectionName: collection,
			Action: &milvuspb.AlterCollectionSchemaRequest_Action{Op: &milvuspb.AlterCollectionSchemaRequest_Action_DropRequest{
				DropRequest: &milvuspb.AlterCollectionSchemaRequest_DropRequest{Identifier: &milvuspb.AlterCollectionSchemaRequest_DropRequest_FieldName{FieldName: "obsolete"}},
			}},
		})
		s.Require().NoError(err)
		s.Require().NoError(merr.Error(response.GetAlterStatus()))
	}
	field := propagationTextField("backfilled")
	if s.rewrite {
		field = &schemapb.FieldSchema{Name: "backfilled", DataType: schemapb.DataType_Int64, Nullable: true, DefaultValue: &schemapb.ValueField{Data: &schemapb.ValueField_LongData{LongData: 77}}}
	}
	encoded, err := proto.Marshal(field)
	s.Require().NoError(err)
	status, err := s.Cluster.MilvusClient.AddCollectionField(ctx, &milvuspb.AddCollectionFieldRequest{DbName: s.dbName, CollectionName: collection, Schema: encoded})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	description = s.propagationDescription(ctx, collection)
	newFieldID := requestedFieldIDs(description.GetSchema(), []string{"backfilled"})[0]
	nodeID := s.coldDataNode(s.fixture)
	prefixes := []string{fmt.Sprintf("insert_log/%d", collectionID), fmt.Sprintf("stats_log/%d", collectionID), "text_log"}
	before := s.objectSnapshot(ctx, prefixes...)
	s.Require().NotEmpty(before, "snapshot must include the existing raw input objects")
	fault := keyFaultControl{Role: "mixcoord", EZID: s.ezID, CollectionID: collectionID, Operation: "unsafe", Caller: "(*bumpSchemaVersionTask).BuildCompactionRequest"}
	if s.faultStage == "schema" {
		s.fixture.setFault(s.T(), fault)
	}
	revert := s.Cluster.MustModifyMilvusConfig(map[string]string{"dataCoord.compaction.bumpSchemaVersion.enabled": "true"})
	defer revert()
	var plan *datapb.CompactionTask
	s.Require().Eventually(func() bool {
		for id, candidate := range s.propagationPlans(ctx, collectionID, datapb.CompactionType_BumpSchemaVersionCompaction) {
			if _, old := baseline[id]; old {
				continue
			}
			if s.faultStage == "schema" {
				if candidate.GetFailReason() != "" {
					plan = candidate
					return true
				}
			} else {
				s.Require().Empty(candidate.GetFailReason())
				if candidate.GetState() == datapb.CompactionTaskState_completed || candidate.GetState() == datapb.CompactionTaskState_cleaned {
					plan = candidate
					return true
				}
			}
		}
		return false
	}, 2*time.Minute, 100*time.Millisecond)
	s.Require().NotNil(plan)
	s.Require().Equal([]int64{inputs[0].GetID()}, plan.GetInputSegments())
	s.T().Logf("stage=schema rewrite=%t plan=%d node=%d inputs=%v outputs=%v state=%s", s.rewrite, plan.GetPlanID(), plan.GetNodeID(), plan.GetInputSegments(), plan.GetResultSegments(), plan.GetState())
	if s.faultStage == "schema" {
		s.Require().NotEmpty(s.fixture.faultHits(s.T(), fault))
		s.Require().Empty(plan.GetResultSegments())
		s.Require().True(maps.Equal(before, s.objectSnapshot(ctx, prefixes...)))
		s.fixture.assertNotRegistered(s.T(), nodeID, s.ezID)
		return
	}
	s.Require().Equal(nodeID, plan.GetNodeID())
	outputs := s.propagationSegments(ctx, collection)
	s.Require().Len(outputs, 1)
	s.Require().Equal(int64(len(expected)), outputs[0].GetNumOfRows())
	s.Require().Equal(description.GetSchema().GetVersion(), outputs[0].GetSchemaVersion())
	s.inspectPropagationRaw(ctx, outputs, collectionID)
	columns := s.propagationColumns(ctx, outputs, collectionID)
	newHasC := false
	for object, fields := range columns {
		newHasC = newHasC || slices.Contains(fields, strconv.FormatInt(newFieldID, 10))
		if s.rewrite {
			s.Require().NotContains(fields, strconv.FormatInt(oldFieldID, 10))
			s.Require().NotContains(oldColumns, object, "full rewrite retained an old physical data file")
		}
	}
	s.Require().True(newHasC, "backfill did not materialize the new column")
	if s.rewrite {
		s.Require().NotEqual(inputs[0].GetID(), outputs[0].GetID())
	} else {
		s.Require().Equal(inputs[0].GetID(), outputs[0].GetID())
		for object, fields := range oldColumns {
			s.Require().Equal(fields, columns[object], "append must retain old column groups")
		}
	}
	s.fixture.assertRegistration(s.T(), nodeID, s.ezID, collectionID)
	// Disable further schema work before proving the independent Stats entry.
	s.Cluster.MustModifyMilvusConfig(map[string]string{"dataCoord.compaction.bumpSchemaVersion.enabled": "false"})
	if !s.rewrite {
		s.Require().Nil(outputs[0].GetTextStatsLogs()[newFieldID])
		nodeID = s.coldDataNode(s.fixture)
		tasks := s.runPropagationStats(ctx, s.fixture, collectionID, nodeID, indexpb.StatsSubJob_TextIndexJob, s.faultStage == "text")
		outputs = s.propagationSegments(ctx, collection)
		s.Require().Len(outputs, 1)
		text := outputs[0].GetTextStatsLogs()[newFieldID]
		s.Require().NotNil(text)
		s.Require().Contains(tasks, text.GetBuildID())
		s.Require().Equal(tasks[text.GetBuildID()].GetVersion(), text.GetVersion())
		s.Require().NotEqual(plan.GetPlanID(), text.GetBuildID(), "Text Stats must come from its independent task")
		s.inspectPropagationText(ctx, outputs[0], newFieldID, text.GetBuildID())
	}
	s.reloadPropagation(ctx, description, outputs)
	s.assertConsumerRows(ctx, collection, expected)
	ids := make([]int64, 0, len(expected))
	for id := range expected {
		ids = append(ids, id)
	}
	if s.rewrite {
		s.assertPropagationQuery(ctx, collection, "backfilled == 77", ids)
	} else {
		s.assertPropagationQuery(ctx, collection, `backfilled == "propagationtoken"`, ids)
		s.assertPropagationQuery(ctx, collection, `TEXT_MATCH(backfilled, 'propagationtoken')`, ids)
		s.assertPropagationQuery(ctx, collection, `TEXT_MATCH(backfilled, 'absenttoken')`, nil)
	}
}

// Keep public schema refreshes explicit after schema broadcasts.
func (s *rawDataSuite) propagationDescription(ctx context.Context, collection string) *milvuspb.DescribeCollectionResponse {
	description, err := s.Cluster.MilvusClient.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{DbName: s.dbName, CollectionName: collection})
	s.Require().NoError(merr.CheckRPCCall(description, err))
	return description
}

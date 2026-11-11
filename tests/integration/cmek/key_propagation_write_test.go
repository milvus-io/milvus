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
	"path"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/importv3pb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration/cmek/inspector"
)

type propagationFlushSuite struct {
	rawDataSuite
	fixture propagationFixture
	version int64
	fault   bool
}

func (s *propagationFlushSuite) SetupSuite() {
	s.fixture.dir = s.T().TempDir()
	s.WithMilvusConfig("MILVUS_CMEK_FIXTURE_CONTROL", s.fixture.dir)
	s.setupRawData(s.version)
}

func (s *propagationFlushSuite) TestFlush() {
	defer s.fixture.restore(s.T())
	ctx, cancel := context.WithTimeout(s.Cluster.GetContext(), 5*time.Minute)
	defer cancel()
	campaign := consumerCampaign("propagation_flush")
	description := s.prepareRawDataCollection(ctx, campaign)
	collection, collectionID := description.GetCollectionName(), description.GetCollectionID()
	prefixes := []string{
		fmt.Sprintf("insert_log/%d", collectionID),
		fmt.Sprintf("stats_log/%d", collectionID),
		fmt.Sprintf("delta_log/%d", collectionID),
	}
	before := s.objectSnapshot(ctx, prefixes...)
	s.Require().Empty(before)
	fault := keyFaultControl{
		Role: "streamingnode", EZID: s.ezID, CollectionID: collectionID,
		Operation: "unsafe", Caller: "(*BulkPackWriterV2).getPluginContext",
	}
	if s.fault {
		s.fixture.setFault(s.T(), fault)
	}
	fields, expected := consumerBatch(0, 128, 1)
	insert := s.insertRawDataBatch(ctx, description, fields, len(expected))
	type flushResult struct {
		response *milvuspb.FlushResponse
		err      error
	}
	flushed := make(chan flushResult, 1)
	// Flush now waits for consuming-side L1/L0 completion. Observe the fault
	// while that same request is in flight, then let it finish after recovery.
	go func() {
		response, err := s.Cluster.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{
			DbName: s.dbName, CollectionNames: []string{collection},
		})
		flushed <- flushResult{response, err}
	}()
	if s.fault {
		s.fixture.waitForRetry(s.T(), fault, func() {
			s.Require().True(maps.Equal(before, s.objectSnapshot(ctx, prefixes...)),
				"key-unavailable flush created or overwrote target objects, including unpublished files")
			select {
			case result := <-flushed:
				s.T().Fatalf("original Flush returned while writer key was unavailable: response=%v err=%v", result.response, result.err)
			default:
			}
			state, err := s.Cluster.MixCoordClient.GetFlushState(ctx, &datapb.GetFlushStateRequest{
				FlushTs: insert.GetTimestamp(), CollectionID: collectionID,
			})
			s.Require().NoError(merr.CheckRPCCall(state, err))
			s.Require().False(state.GetFlushed(), "original batch completed while writer key was unavailable")
			segments, err := s.Cluster.ShowSegmentsWithDB(s.dbName, collection)
			s.Require().NoError(err)
			ids := make([]int64, 0, len(segments))
			for _, segment := range segments {
				ids = append(ids, segment.GetID())
			}
			if len(ids) == 0 {
				return // Segment allocation may not yet have reached DataCoord.
			}
			info, err := s.Cluster.MixCoordClient.GetSegmentInfo(ctx, &datapb.GetSegmentInfoRequest{
				SegmentIDs: ids, IncludeUnHealthy: true,
			})
			if err = merr.CheckRPCCall(info, err); err != nil {
				// A newly allocated segment may not yet have been reported.
				s.Require().ErrorIs(err, merr.ErrSegmentNotFound)
				return
			}
			for _, position := range info.GetChannelCheckpoint() {
				if position != nil {
					s.Require().Less(position.GetTimestamp(), insert.GetTimestamp(),
						"flush checkpoint passed the blocked input batch")
				}
			}
			for _, segment := range info.GetInfos() {
				s.Require().NotEqual(commonpb.SegmentState_Flushed, segment.GetState())
			}
		})
		// No new insert or flush request: recovery must finish this exact batch.
		s.fixture.restore(s.T())
	}
	var flush *milvuspb.FlushResponse
	select {
	case result := <-flushed:
		s.Require().NoError(merr.CheckRPCCall(result.response, result.err))
		flush = result.response
	case <-ctx.Done():
		s.T().Fatalf("original Flush did not finish after key recovery: %v", ctx.Err())
	}
	ids := flush.GetFlushCollSegIDs()[collection].GetData()
	s.Require().NotEmpty(ids)
	flushTs := flush.GetCollFlushTs()[collection]
	s.WaitForFlush(ctx, ids, flushTs, s.dbName, collection)
	segments := s.rawFlushedSegments(collection, ids)
	var rows int64
	for _, segment := range segments {
		rows += segment.GetNumOfRows()
		s.Require().Equal(s.version, segment.GetStorageVersion())
	}
	s.Require().Equal(int64(len(expected)), rows)
	var manifests map[int64]inspector.ManifestLocatorV3
	if s.version == 3 {
		manifests, _ = s.inspectRawDataV3(ctx, segments, collectionID, "")
	} else {
		objects, err := inspector.LocateRawDataV2(s.Cluster.RootPath(), segments)
		s.Require().NoError(err)
		s.Require().NotEmpty(objects)
		for _, object := range objects {
			raw, err := s.Cluster.ChunkManager.Read(ctx, object.Path)
			s.Require().NoError(err)
			_, err = inspector.InspectEncryptedParquet(raw, s.ezID, collectionID)
			s.Require().NoError(err, "object=%s", object.Path)
		}
	}
	status, err := s.Cluster.MilvusClient.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{
		DbName: s.dbName, CollectionName: collection,
	})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.CheckCollectionCacheReleased(collectionID)
	status, err = s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName: s.dbName, CollectionName: collection, ReplicaNumber: 1, LoadFields: campaign.loadFields,
	})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.WaitForLoadWithDB(ctx, s.dbName, collection)
	if s.version == 3 {
		snapshot := s.parquetReadSnapshot(ctx, collectionID)
		s.Require().True(snapshot.matches(manifests, s.manifestIdentity(s.currentParquetSegments(ctx, collectionID))))
	} else {
		s.assertRawLoadedSegments(ctx, collectionID, segments)
	}
	s.assertConsumerRows(ctx, collection, expected)
	s.T().Logf("stage=flush-recovered storage=%d ez=%d collection=%d segments=%v rows=%d fault=%t",
		s.version, s.ezID, collectionID, ids, rows, s.fault)
}

type propagationImportSuite struct {
	rawDataSuite
	fixture                          propagationFixture
	protocol                         int
	sourceEncrypted, targetEncrypted bool
	faultStage                       string
}

func (s *propagationImportSuite) SetupSuite() {
	s.fixture.dir = s.T().TempDir()
	s.WithMilvusConfig("MILVUS_CMEK_FIXTURE_CONTROL", s.fixture.dir)
	s.WithMilvusConfig("dataCoord.import.enableImportV3", strconv.FormatBool(s.protocol == 3))
	s.WithMilvusConfig("dataCoord.compaction.bumpSchemaVersion.enabled", "true")
	s.WithMilvusConfig("dataCoord.import.enableIDRangeMsg", "true")
	// Leave time to replace DN between independently dispatched stages. The
	// test checks task metadata, so an unexpected early dispatch fails it.
	s.WithMilvusConfig("dataCoord.import.checkIntervalHigh", "20")
	s.WithMilvusConfig("dataCoord.import.scheduleInterval", "0.1")
	s.WithMilvusConfig("dataCoord.taskCheckInterval", "1")
	s.setupRawData(2) // backup input is a real packed-binlog collection
}

func (s *rawDataSuite) propagationDatabase(ctx context.Context, encrypted bool) (string, int64) {
	name := "propagation_" + funcutil.GenRandomStr()
	var properties []*commonpb.KeyValuePair
	if encrypted {
		properties = []*commonpb.KeyValuePair{
			{Key: common.EncryptionEnabledKey, Value: "true"},
			{Key: common.EncryptionRootKeyKey, Value: "fixture-root-key"},
		}
	}
	status, err := s.Cluster.MilvusClient.CreateDatabase(ctx, &milvuspb.CreateDatabaseRequest{DbName: name, Properties: properties})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.T().Cleanup(func() {
		status, err := s.Cluster.MilvusClient.DropDatabase(context.Background(), &milvuspb.DropDatabaseRequest{DbName: name})
		s.NoError(merr.CheckRPCCall(status, err))
	})
	info, err := s.Cluster.MilvusClient.DescribeDatabase(ctx, &milvuspb.DescribeDatabaseRequest{DbName: name})
	s.Require().NoError(merr.CheckRPCCall(info, err))
	if encrypted {
		return name, info.GetDbID()
	}
	s.Require().Empty(propertyValue(info.GetProperties(), common.EncryptionEzIDKey))
	return name, 0
}

type propagationImportTask struct {
	id, nodeID, rows int64
	state            datapb.ImportTaskStateV2
	reason           string
}

func (s *propagationImportSuite) stageTasks(ctx context.Context, jobID int64, stage string) map[int64]propagationImportTask {
	tasks := make(map[int64]propagationImportTask)
	add := func(job, id, node, rows int64, state datapb.ImportTaskStateV2, reason string) {
		if job == jobID {
			tasks[id] = propagationImportTask{id, node, rows, state, reason}
		}
	}
	switch {
	case stage == "preimport" && s.protocol == 2:
		for _, task := range readPropagationRecords(ctx, &s.rawDataSuite, "datacoord-meta/preimport-task", func() *datapb.PreImportTask { return &datapb.PreImportTask{} }) {
			var rows int64
			for _, stats := range task.GetFileStats() {
				rows += stats.GetTotalRows()
			}
			add(task.GetJobID(), task.GetTaskID(), task.GetNodeID(), rows, task.GetState(), task.GetReason())
		}
	case stage == "preimport":
		for _, task := range readPropagationRecords(ctx, &s.rawDataSuite, "datacoord-meta/preimport-v3-task", func() *datapb.PreImportTaskV3 { return &datapb.PreImportTaskV3{} }) {
			var rows int64
			for _, stats := range task.GetFileStats() {
				rows += stats.GetTotalRows()
			}
			add(task.GetJobId(), task.GetTaskId(), task.GetNodeId(), rows, task.GetState(), task.GetReason())
		}
	case stage == "reshard":
		for _, task := range readPropagationRecords(ctx, &s.rawDataSuite, "datacoord-meta/reshard-task", func() *datapb.ReshardTask { return &datapb.ReshardTask{} }) {
			add(task.GetJobId(), task.GetTaskId(), task.GetNodeId(), 0, task.GetState(), task.GetReason())
		}
	case s.protocol == 2:
		for _, task := range readPropagationRecords(ctx, &s.rawDataSuite, "datacoord-meta/import-task", func() *datapb.ImportTaskV2 { return &datapb.ImportTaskV2{} }) {
			add(task.GetJobID(), task.GetTaskID(), task.GetNodeID(), 0, task.GetState(), task.GetReason())
		}
	default:
		for _, task := range readPropagationRecords(ctx, &s.rawDataSuite, "datacoord-meta/import-task-v3", func() *datapb.ImportTaskV3 { return &datapb.ImportTaskV3{} }) {
			add(task.GetJobId(), task.GetTaskId(), task.GetNodeId(), task.GetRows(), task.GetState(), task.GetReason())
		}
	}
	return tasks
}

func (s *propagationImportSuite) TestBackup() {
	ctx, cancel := context.WithTimeout(s.Cluster.GetContext(), 9*time.Minute)
	defer cancel()
	defer s.fixture.restore(s.T())
	originalDB, originalEZ := s.dbName, s.ezID
	defer func() { s.dbName, s.ezID = originalDB, originalEZ }()
	s.dbName, s.ezID = s.propagationDatabase(ctx, s.sourceEncrypted)
	sourceDB, sourceEZ := s.dbName, s.ezID
	source := s.prepareRawDataCollection(ctx, consumerCampaign("source"))
	fields, expected := consumerBatch(0, 128, 1)
	s.insertRawDataBatch(ctx, source, fields, len(expected))
	sourceSegments := s.flushConsumerCollection(ctx, source)
	s.WaitForIndexBuiltWithDB(ctx, sourceDB, source.GetCollectionName(), "float_vector")
	s.inspectPropagationV2(ctx, sourceSegments, source.GetCollectionID(), sourceEZ)
	options := []*commonpb.KeyValuePair{{Key: "backup", Value: "true"}, {Key: importutilv2.StorageVersion, Value: "2"}}
	if s.sourceEncrypted {
		backup, err := s.Cluster.MixCoordClient.BackupEzk(ctx, &internalpb.BackupEzkRequest{DbName: sourceDB})
		s.Require().NoError(merr.CheckRPCCall(backup, err))
		s.Require().NotEmpty(backup.GetEzk())
		options = append(options, &commonpb.KeyValuePair{Key: importutilv2.EZK, Value: backup.GetEzk()})
	}
	s.dbName, s.ezID = s.propagationDatabase(ctx, s.targetEncrypted)
	target := s.prepareRawDataCollection(ctx, consumerCampaign("target"))
	collectionID := target.GetCollectionID()
	files := make([]*internalpb.ImportFile, 0, len(sourceSegments))
	for _, segment := range sourceSegments {
		files = append(files, &internalpb.ImportFile{Paths: []string{path.Join(s.Cluster.RootPath(), "insert_log",
			strconv.FormatInt(source.GetCollectionID(), 10), strconv.FormatInt(segment.GetPartitionID(), 10), strconv.FormatInt(segment.GetID(), 10))}})
	}
	nodeID := s.coldDataNode(s.fixture)
	callers := map[string]string{"preimport": "assemblePreImportRequest", "import": "AssembleImportRequest", "reshard": "(*reshardTask).CreateTaskOnWorker"}
	if s.protocol == 3 {
		callers["import"] = "(*importTaskV3).CreateTaskOnWorker"
	}
	fault := keyFaultControl{Role: "mixcoord", EZID: s.ezID, CollectionID: collectionID, Operation: "unsafe", Caller: callers[s.faultStage]}
	if s.faultStage != "none" {
		s.fixture.setFault(s.T(), fault)
	}
	response, err := s.Cluster.ProxyClient.ImportV2(ctx, &internalpb.ImportRequest{
		DbName: s.dbName, CollectionName: target.GetCollectionName(),
		PartitionName: paramtable.Get().CommonCfg.DefaultPartitionName.GetValue(),
		Files:         files, Options: options,
	})
	s.Require().NoError(merr.CheckRPCCall(response, err))
	jobID, err := strconv.ParseInt(response.GetJobID(), 10, 64)
	s.Require().NoError(err)
	prefixes := []string{fmt.Sprintf("insert_log/%d", collectionID), fmt.Sprintf("stats_log/%d", collectionID), fmt.Sprintf("import_v3/%d", jobID)}
	before := s.objectSnapshot(ctx, prefixes...)
	if s.faultStage == "preimport" {
		s.Require().Eventually(func() bool {
			progress, err := s.Cluster.ProxyClient.GetImportProgress(ctx, &internalpb.GetImportProgressRequest{DbName: s.dbName, JobID: response.GetJobID()})
			s.Require().NoError(merr.CheckRPCCall(progress, err))
			return progress.GetState() == internalpb.ImportJobState_Failed
		}, 90*time.Second, 200*time.Millisecond)
		s.Require().NotEmpty(s.fixture.faultHits(s.T(), fault))
		s.Require().True(maps.Equal(before, s.objectSnapshot(ctx, prefixes...)))
		s.fixture.assertNotRegistered(s.T(), nodeID, s.ezID)
		for _, task := range s.stageTasks(ctx, jobID, "preimport") {
			s.Require().LessOrEqual(task.nodeID, int64(0))
		}
		s.T().Logf("job=%d stage=preimport outcome=failed-before-dispatch", jobID)
		return // Existing PreImport assembly failures are terminal.
	}
	stages := []string{"preimport", "import"}
	if s.protocol == 3 {
		stages = []string{"preimport", "reshard", "import"}
	}
	for i, stage := range stages {
		if i > 0 {
			for _, task := range s.stageTasks(ctx, jobID, stage) {
				s.Require().LessOrEqual(task.nodeID, int64(0), "stage %s was dispatched before cold-node replacement", stage)
			}
			nodeID = s.coldDataNode(s.fixture)
		}
		var blocked map[int64]propagationImportTask
		if stage == s.faultStage {
			before = s.objectSnapshot(ctx, prefixes...)
			s.fixture.waitForRetry(s.T(), fault, func() {
				s.Require().True(maps.Equal(before, s.objectSnapshot(ctx, prefixes...)), "unavailable key produced target output")
				s.fixture.assertNotRegistered(s.T(), nodeID, s.ezID)
				blocked = s.stageTasks(ctx, jobID, stage)
				for _, task := range blocked {
					s.Require().LessOrEqual(task.nodeID, int64(0))
					s.Require().NotEqual(datapb.ImportTaskStateV2_Completed, task.state)
				}
			})
			s.Require().NotEmpty(blocked)
			s.fixture.restore(s.T())
		}
		var completed map[int64]propagationImportTask
		s.Require().Eventually(func() bool {
			completed = s.stageTasks(ctx, jobID, stage)
			if len(completed) == 0 {
				return false
			}
			for _, task := range completed {
				s.Require().NotEqual(datapb.ImportTaskStateV2_Failed, task.state, "stage=%s reason=%s", stage, task.reason)
				if task.state != datapb.ImportTaskStateV2_Completed {
					return false
				}
			}
			return true
		}, 2*time.Minute, 100*time.Millisecond, "job=%d stage=%s", jobID, stage)
		for id, task := range completed {
			// Completed task cleanup may clear NodeID. Registration still
			// identifies the only eligible process for this isolated stage.
			s.Require().True(task.nodeID <= 0 || task.nodeID == nodeID)
			if blocked != nil {
				s.Require().Contains(blocked, id, "recovery replaced the original task")
			}
			s.T().Logf("job=%d stage=%s task=%d node=%d rows=%d", jobID, stage, id, nodeID, task.rows)
		}
		if stage == "preimport" {
			var rows int64
			for _, task := range completed {
				rows += task.rows
			}
			// Import V3 backup preimport is size-only; the reshard stage reads
			// the source and establishes its row count.
			if s.protocol == 2 {
				s.Require().Equal(int64(len(expected)), rows)
			}
		}
		if s.sourceEncrypted && (stage == "preimport" && s.protocol == 2 || stage == "reshard" || stage == "import" && s.protocol == 2) {
			s.fixture.assertRegistration(s.T(), nodeID, sourceEZ, 0)
		}
		if s.targetEncrypted && stage != "preimport" {
			s.fixture.assertRegistration(s.T(), nodeID, s.ezID, collectionID)
		}
		if stage == "reshard" {
			s.inspectReshardFragments(ctx, jobID, collectionID, int64(len(expected)), completed)
		}
	}
	s.Require().Eventually(func() bool {
		progress, err := s.Cluster.ProxyClient.GetImportProgress(ctx, &internalpb.GetImportProgressRequest{DbName: s.dbName, JobID: response.GetJobID()})
		s.Require().NoError(merr.CheckRPCCall(progress, err))
		s.Require().NotEqual(internalpb.ImportJobState_Failed, progress.GetState(), progress.GetReason())
		return progress.GetState() == internalpb.ImportJobState_Completed
	}, 2*time.Minute, 200*time.Millisecond)
	segments, err := s.Cluster.ShowSegmentsWithDB(s.dbName, target.GetCollectionName())
	s.Require().NoError(err)
	var outputs []*datapb.SegmentInfo
	var rows int64
	for _, segment := range segments {
		if segment.GetState() == commonpb.SegmentState_Flushed && segment.GetNumOfRows() > 0 {
			outputs = append(outputs, segment)
			rows += segment.GetNumOfRows()
		}
	}
	s.Require().Equal(int64(len(expected)), rows)
	s.inspectPropagationV2(ctx, outputs, collectionID, s.ezID)
	s.WaitForIndexBuiltWithDB(ctx, s.dbName, target.GetCollectionName(), "float_vector")
	status, err := s.Cluster.MilvusClient.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{DbName: s.dbName, CollectionName: target.GetCollectionName()})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.CheckCollectionCacheReleased(collectionID)
	status, err = s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{DbName: s.dbName, CollectionName: target.GetCollectionName()})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.WaitForLoadWithDB(ctx, s.dbName, target.GetCollectionName())
	s.assertRawLoadedSegments(ctx, collectionID, outputs)
	s.assertConsumerRows(ctx, target.GetCollectionName(), expected)
}

func (s *rawDataSuite) inspectPropagationV2(ctx context.Context, segments []*datapb.SegmentInfo, collectionID, ezID int64) {
	objects, err := inspector.LocateRawDataV2(s.Cluster.RootPath(), segments)
	s.Require().NoError(err)
	s.Require().NotEmpty(objects)
	for _, object := range objects {
		s.inspectPropagationParquet(ctx, object.Path, collectionID, ezID)
	}
}

func (s *rawDataSuite) inspectPropagationParquet(ctx context.Context, object string, collectionID, ezID int64) {
	raw, err := s.Cluster.ChunkManager.Read(ctx, object)
	s.Require().NoError(err)
	if ezID != 0 {
		_, err = inspector.InspectEncryptedParquet(raw, ezID, collectionID)
		s.Require().NoError(err, object)
	} else {
		table, err := inspector.ReadEncryptedParquet(raw, nil)
		s.Require().NoError(err, "plaintext target must be readable without any key: %s", object)
		s.Require().Positive(table.NumRows())
		table.Release()
	}
}

// The completed task's manifest is authoritative for the full fragment set;
// storage enumeration also catches files that were never published there.
func (s *propagationImportSuite) inspectReshardFragments(ctx context.Context, jobID, collectionID, expectedRows int64, completed map[int64]propagationImportTask) {
	objects := s.objectSnapshot(ctx, metautil.BuildImportV3JobPath(jobID))
	s.Require().NotEmpty(objects, "snapshot must include reshard outputs in the cluster's storage root")
	fragments := make(map[string]struct{})
	var rows int64
	for _, task := range readPropagationRecords(ctx, &s.rawDataSuite, "datacoord-meta/reshard-task", func() *datapb.ReshardTask { return &datapb.ReshardTask{} }) {
		if task.GetJobId() != jobID {
			continue
		}
		s.Require().Contains(completed, task.GetTaskId())
		s.Require().Positive(task.GetRunId())
		manifestPath := path.Join(s.Cluster.RootPath(), metautil.BuildImportReshardResultPath(jobID, task.GetTaskId(), task.GetRunId()))
		s.Require().Contains(objects, manifestPath)
		data, err := s.Cluster.ChunkManager.Read(ctx, manifestPath)
		s.Require().NoError(err)
		manifest := &importv3pb.ReshardManifest{}
		s.Require().NoError(proto.Unmarshal(data, manifest))
		for _, fragment := range manifest.GetFragments() {
			s.Require().NotContains(fragments, fragment.GetPath())
			s.Require().Contains(objects, fragment.GetPath())
			s.Require().Positive(fragment.GetRows())
			fragments[fragment.GetPath()] = struct{}{}
			rows += fragment.GetRows()
			s.inspectPropagationParquet(ctx, fragment.GetPath(), collectionID, s.ezID)
		}
	}
	s.Require().NotEmpty(fragments)
	s.Require().Equal(expectedRows, rows)
	for object := range objects {
		if strings.HasSuffix(object, ".parquet") {
			s.Require().Contains(fragments, object, "unpublished reshard fragment")
		}
	}
	s.T().Logf("stage=reshard-objects job=%d fragments=%d rows=%d", jobID, len(fragments), rows)
}

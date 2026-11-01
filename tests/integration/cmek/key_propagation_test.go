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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestKeyPropagationAnalyzeAndClustering(t *testing.T) {
	for _, stage := range []string{"analyze", "clustering"} {
		t.Run(stage, func(t *testing.T) { suite.Run(t, &propagationAnalyzeSuite{faultStage: stage}) })
	}
}

func TestKeyPropagationCompaction(t *testing.T) {
	for _, version := range []int64{2, 3} {
		for _, kind := range []datapb.CompactionType{datapb.CompactionType_SortCompaction, datapb.CompactionType_MixCompaction} {
			for _, fault := range []bool{false, true} {
				t.Run(fmt.Sprintf("v%d/%s/key_unavailable=%t", version, kind, fault), func(t *testing.T) {
					suite.Run(t, &propagationCompactionSuite{version: version, kind: kind, fault: fault})
				})
			}
		}
	}
}

func TestKeyPropagationFlush(t *testing.T) {
	for _, version := range []int64{2, 3} {
		for _, fault := range []bool{false, true} {
			t.Run(fmt.Sprintf("v%d/key_unavailable=%t", version, fault), func(t *testing.T) {
				suite.Run(t, &propagationFlushSuite{version: version, fault: fault})
			})
		}
	}
}

func TestKeyPropagationImport(t *testing.T) {
	for _, protocol := range []int{2, 3} {
		for _, source := range []bool{false, true} {
			for _, target := range []bool{false, true} {
				stages := []string{"none"}
				if target {
					stages = append(stages, "preimport", "import")
					if protocol == 3 {
						stages = append(stages, "reshard")
					}
				}
				for _, stage := range stages {
					t.Run(fmt.Sprintf("protocol%d/source=%t/target=%t/fault=%s", protocol, source, target, stage), func(t *testing.T) {
						suite.Run(t, &propagationImportSuite{protocol: protocol, sourceEncrypted: source, targetEncrypted: target, faultStage: stage})
					})
				}
			}
		}
	}
}

func TestKeyPropagationIndex(t *testing.T) {
	for _, version := range []int64{2, 3} {
		for _, kind := range []string{"vector", "scalar", "default"} {
			if version == 2 && kind == "default" {
				continue // The explicit default-value adapter case uses V3.
			}
			for _, fault := range []bool{false, true} {
				t.Run(fmt.Sprintf("v%d/%s/key_unavailable=%t", version, kind, fault), func(t *testing.T) {
					suite.Run(t, &propagationIndexSuite{version: version, kind: kind, fault: fault})
				})
			}
		}
	}
}

func TestKeyPropagationSchema(t *testing.T) {
	for _, rewrite := range []bool{false, true} {
		stages := []string{"none", "schema"}
		if !rewrite {
			stages = append(stages, "text")
		}
		for _, stage := range stages {
			t.Run(fmt.Sprintf("rewrite=%t/fault=%s", rewrite, stage), func(t *testing.T) { suite.Run(t, &propagationSchemaSuite{rewrite: rewrite, faultStage: stage}) })
		}
	}
}

func TestKeyPropagationJSONStats(t *testing.T) {
	for _, fault := range []bool{false, true} {
		t.Run(fmt.Sprintf("key_unavailable=%t", fault), func(t *testing.T) { suite.Run(t, &propagationJSONSuite{fault: fault}) })
	}
}

type keyFaultControl struct {
	Role         string `json:"role"`
	EZID         int64  `json:"ez_id"`
	CollectionID int64  `json:"collection_id"`
	Operation    string `json:"operation"`
	Caller       string `json:"caller,omitempty"`
}

type keyObservation struct {
	PID          int      `json:"pid"`
	NodeID       int64    `json:"node_id"`
	Role         string   `json:"role"`
	EZID         int64    `json:"ez_id"`
	CollectionID int64    `json:"collection_id"`
	Operation    string   `json:"operation"`
	Outcome      string   `json:"outcome"`
	Stack        []string `json:"stack"`
}

type propagationFixture struct {
	dir string
}

func (f propagationFixture) setFault(t *testing.T, fault keyFaultControl) {
	t.Helper()
	data, err := json.Marshal(fault)
	require.NoError(t, err)
	temporary := filepath.Join(f.dir, "fault.pending")
	require.NoError(t, os.WriteFile(temporary, data, 0o600))
	require.NoError(t, os.Rename(temporary, filepath.Join(f.dir, "fault.json")))
	t.Cleanup(func() {
		err := os.Remove(filepath.Join(f.dir, "fault.json"))
		require.True(t, err == nil || os.IsNotExist(err), "%v", err)
	})
}

func (f propagationFixture) restore(t *testing.T) {
	t.Helper()
	err := os.Remove(filepath.Join(f.dir, "fault.json"))
	require.True(t, err == nil || os.IsNotExist(err), "%v", err)
}

func (f propagationFixture) observations(t *testing.T, language string) []keyObservation {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(f.dir, language+"-*.jsonl"))
	require.NoError(t, err)
	var events []keyObservation
	for _, file := range files {
		data, err := os.ReadFile(file)
		require.NoError(t, err)
		lines := bytes.Split(data, []byte{'\n'})
		// A writer may be appending the last line. Only complete records count.
		for _, line := range lines[:len(lines)-1] {
			var event keyObservation
			require.NoError(t, json.Unmarshal(line, &event))
			events = append(events, event)
		}
	}
	return events
}

func (f propagationFixture) faultHits(t *testing.T, fault keyFaultControl) []keyObservation {
	t.Helper()
	var hits []keyObservation
	for _, event := range f.observations(t, "go") {
		if event.Role == fault.Role && event.EZID == fault.EZID && event.CollectionID == fault.CollectionID &&
			event.Operation == fault.Operation && event.Outcome == "unavailable" {
			if fault.Caller != "" {
				found := false
				for _, frame := range event.Stack {
					found = found || strings.Contains(frame, fault.Caller)
				}
				if !found {
					continue
				}
			}
			hits = append(hits, event)
		}
	}
	return hits
}

func (f propagationFixture) waitForRetry(t *testing.T, fault keyFaultControl, assertBlocked func()) {
	t.Helper()
	require.Eventually(t, func() bool {
		assertBlocked()
		return len(f.faultHits(t, fault)) >= 3
	}, 90*time.Second, 250*time.Millisecond, "writer key fault did not cause repeated attempts: %+v", fault)
	assertBlocked()
	hits := f.faultHits(t, fault)
	t.Logf("stage=writer-key-blocked role=%s ez=%d collection=%d caller=%s attempts=%d pid=%d node=%d",
		fault.Role, fault.EZID, fault.CollectionID, fault.Caller, len(hits), hits[0].PID, hits[0].NodeID)
}

func (f propagationFixture) assertRegistration(t *testing.T, nodeID, ezID, collectionID int64) {
	t.Helper()
	var goPID int
	for _, event := range f.observations(t, "go") {
		if event.NodeID == nodeID && event.Role == "datanode" && event.EZID == ezID && event.Operation == "register" {
			goPID = event.PID
		}
	}
	require.Positive(t, goPID, "cold DataNode did not receive EZ %d", ezID)
	var cppPID int
	for _, event := range f.observations(t, "cpp") {
		if event.NodeID == nodeID && event.EZID == ezID && event.CollectionID == collectionID && event.Operation == "register" {
			cppPID = event.PID
		}
	}
	require.Equal(t, goPID, cppPID, "C++ context must be registered in the executing DataNode process")
	t.Logf("stage=registered node=%d pid=%d ez=%d collection=%d", nodeID, cppPID, ezID, collectionID)
}

func (f propagationFixture) assertNotRegistered(t *testing.T, nodeID, ezID int64) {
	t.Helper()
	for _, event := range f.observations(t, "go") {
		require.False(t, event.NodeID == nodeID && event.EZID == ezID && event.Operation == "register",
			"cold DataNode received the blocked task's EZ")
	}
}

func (s *rawDataSuite) coldDataNode(f propagationFixture) int64 {
	for _, node := range s.Cluster.GetAllDataNodes() {
		s.Require().NoError(node.Stop())
	}
	nodeID := s.Cluster.AddDataNode().GetNodeID()
	for _, event := range f.observations(s.T(), "go") {
		s.Require().False(event.NodeID == nodeID && event.EZID == s.ezID && event.Operation == "register",
			"new DataNode was warmed before the target task")
	}
	s.T().Logf("stage=cold-datanode node=%d ez=%d", nodeID, s.ezID)
	return nodeID
}

// Enumerate storage itself, including objects not published in task metadata.
// Hash existing bytes as well: a failed writer must not overwrite source data.
func (s *rawDataSuite) objectSnapshot(ctx context.Context, relativePrefixes ...string) map[string][sha256.Size]byte {
	objects := make(map[string][sha256.Size]byte)
	for _, prefix := range relativePrefixes {
		err := s.Cluster.ChunkManager.WalkWithPrefix(ctx, path.Join(s.Cluster.RootPath(), prefix)+"/", true,
			func(object *storage.ChunkObjectInfo) bool {
				data, err := s.Cluster.ChunkManager.Read(ctx, object.FilePath)
				s.Require().NoError(err)
				objects[object.FilePath] = sha256.Sum256(data)
				return true
			})
		s.Require().NoError(err)
	}
	return objects
}

func (s *rawDataSuite) configurePropagationCompaction() {
	s.enableCompaction = true
	s.WithMilvusConfig("dataCoord.compaction.enableAutoCompaction", "false")
	// This setting is forbidden from runtime updates. Pause compaction after
	// startup instead, once the real trigger loops have been initialized.
	s.WithMilvusConfig("dataCoord.sortCompaction.enable", "true")
	s.WithMilvusConfig("dataCoord.compaction.mix.triggerInterval", "1")
	s.WithMilvusConfig("dataCoord.compaction.bumpSchemaVersion.triggerInterval", "1")
	s.WithMilvusConfig("dataCoord.compaction.bumpSchemaVersion.enabled", "false")
	s.WithMilvusConfig("dataCoord.taskCheckInterval", "1")
	s.WithMilvusConfig("indexCoord.scheduler.interval", "100")
	s.WithMilvusConfig("dataCoord.statsTaskPendingLimit", "-1")
	s.WithMilvusConfig("common.enabledJSONShredding", "false")
}

func (s *rawDataSuite) propagationPlans(ctx context.Context, collectionID int64, kind datapb.CompactionType) map[int64]*datapb.CompactionTask {
	plans := make(map[int64]*datapb.CompactionTask)
	for _, plan := range readPropagationRecords(ctx, s, "datacoord-meta/compaction-task", func() *datapb.CompactionTask { return &datapb.CompactionTask{} }) {
		if plan.GetCollectionID() == collectionID && plan.GetType() == kind {
			plans[plan.GetPlanID()] = plan
		}
	}
	return plans
}

func (s *rawDataSuite) propagationSegments(ctx context.Context, collection string) []*datapb.SegmentInfo {
	all, err := s.Cluster.ShowSegmentsWithDB(s.dbName, collection)
	s.Require().NoError(err)
	var live []*datapb.SegmentInfo
	for _, segment := range all {
		if segment.GetState() == commonpb.SegmentState_Flushed && segment.GetNumOfRows() > 0 && !segment.GetIsInvisible() {
			live = append(live, segment)
		}
	}
	return live
}

// Sort preparation is observed through authoritative segment metadata. Small
// fixtures cannot satisfy the automatic merge fill-rate gate.
func (s *rawDataSuite) sortPropagationInputs(ctx context.Context, collection string, count int) []*datapb.SegmentInfo {
	revert := s.Cluster.MustModifyMilvusConfig(map[string]string{
		"dataCoord.enableCompaction": "true", "dataCoord.compaction.enableAutoCompaction": "true",
	})
	defer revert()
	var segments []*datapb.SegmentInfo
	s.Require().Eventually(func() bool {
		segments = s.propagationSegments(ctx, collection)
		if len(segments) != count {
			return false
		}
		for _, segment := range segments {
			if !segment.GetIsSorted() {
				return false
			}
		}
		return true
	}, 2*time.Minute, 100*time.Millisecond)
	return segments
}

func (s *rawDataSuite) inspectPropagationRaw(ctx context.Context, segments []*datapb.SegmentInfo, collectionID int64) {
	s.Require().NotEmpty(segments)
	if segments[0].GetStorageVersion() == 3 {
		s.inspectRawDataV3(ctx, segments, collectionID, "")
	} else {
		s.inspectRawObjects(ctx, segments, collectionID)
	}
}

func (s *rawDataSuite) reloadPropagation(ctx context.Context, description *milvuspb.DescribeCollectionResponse, segments []*datapb.SegmentInfo) {
	collection := description.GetCollectionName()
	s.WaitForIndexBuiltWithDB(ctx, s.dbName, collection, "float_vector")
	status, err := s.Cluster.MilvusClient.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{DbName: s.dbName, CollectionName: collection})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.CheckCollectionCacheReleased(description.GetCollectionID())
	status, err = s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{DbName: s.dbName, CollectionName: collection})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.WaitForLoadWithDB(ctx, s.dbName, collection)
	s.assertRawLoadedSegments(ctx, description.GetCollectionID(), segments)
	if segments[0].GetStorageVersion() == 3 {
		expected := s.manifestIdentity(segments)
		s.Require().Eventually(func() bool {
			return s.parquetReadSnapshot(ctx, description.GetCollectionID()).matches(expected, s.manifestIdentity(s.currentParquetSegments(ctx, description.GetCollectionID())))
		}, time.Minute, 200*time.Millisecond)
	}
}

func (s *rawDataSuite) assertPropagationQuery(ctx context.Context, collection, expr string, expected []int64) {
	response, err := s.Cluster.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		DbName: s.dbName, CollectionName: collection, Expr: expr, OutputFields: []string{fixturePrimaryKey}, ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
	})
	s.Require().NoError(merr.CheckRPCCall(response, err))
	s.Require().Len(response.GetFieldsData(), 1)
	actual := response.GetFieldsData()[0].GetScalars().GetLongData().GetData()
	s.Require().ElementsMatch(expected, actual)
}

func readPropagationRecords[T proto.Message](ctx context.Context, s *rawDataSuite, prefix string, newRecord func() T) []T {
	response, err := s.Cluster.EtcdCli.Get(ctx, path.Join(s.Cluster.RootPath(), "meta", prefix)+"/", clientv3.WithPrefix())
	s.Require().NoError(err)
	records := make([]T, 0, len(response.Kvs))
	for _, item := range response.Kvs {
		record := newRecord()
		s.Require().NoError(proto.Unmarshal(item.Value, record))
		records = append(records, record)
	}
	return records
}

func (s *rawDataSuite) propagationStatsTasks(ctx context.Context, collectionID int64, kind indexpb.StatsSubJob) map[int64]*indexpb.StatsTask {
	tasks := make(map[int64]*indexpb.StatsTask)
	for _, task := range readPropagationRecords(ctx, s, "datacoord-meta/stats-task", func() *indexpb.StatsTask { return &indexpb.StatsTask{} }) {
		if task.GetCollectionID() == collectionID && task.GetSubJobType() == kind {
			tasks[task.GetTaskID()] = task
		}
	}
	return tasks
}

func (s *rawDataSuite) runPropagationStats(ctx context.Context, fixture propagationFixture, collectionID, nodeID int64, kind indexpb.StatsSubJob, failKey bool) map[int64]*indexpb.StatsTask {
	prefixes := []string{fmt.Sprintf("insert_log/%d", collectionID), "text_log", "json_stats"}
	before := s.objectSnapshot(ctx, prefixes...)
	s.Require().NotEmpty(before, "snapshot must include the existing raw input objects")
	fault := keyFaultControl{Role: "mixcoord", EZID: s.ezID, CollectionID: collectionID, Operation: "unsafe", Caller: "(*statsTask).prepareJobRequest"}
	if failKey {
		fixture.setFault(s.T(), fault)
	}
	configuration := map[string]string{"dataCoord.statsTaskPendingLimit": "100"}
	if kind == indexpb.StatsSubJob_JsonKeyIndexJob {
		configuration["common.enabledJSONShredding"] = "true"
	}
	revert := s.Cluster.MustModifyMilvusConfig(configuration)
	s.T().Cleanup(revert)
	var blocked map[int64]*indexpb.StatsTask
	if failKey {
		fixture.waitForRetry(s.T(), fault, func() {
			s.Require().True(maps.Equal(before, s.objectSnapshot(ctx, prefixes...)))
			blocked = s.propagationStatsTasks(ctx, collectionID, kind)
			for _, task := range blocked {
				s.Require().NotEqual(indexpb.JobState_JobStateFinished, task.GetState())
			}
			fixture.assertNotRegistered(s.T(), nodeID, s.ezID)
		})
		s.Require().NotEmpty(blocked)
		fixture.restore(s.T())
	}
	var finished map[int64]*indexpb.StatsTask
	s.Require().Eventually(func() bool {
		finished = s.propagationStatsTasks(ctx, collectionID, kind)
		if len(finished) == 0 {
			return false
		}
		for _, task := range finished {
			s.Require().NotEqual(indexpb.JobState_JobStateFailed, task.GetState(), task.GetFailReason())
			if task.GetState() != indexpb.JobState_JobStateFinished {
				return false
			}
		}
		return true
	}, 2*time.Minute, 100*time.Millisecond)
	for id, task := range finished {
		if blocked != nil {
			s.Require().Contains(blocked, id)
		}
		s.Require().True(task.GetNodeID() <= 0 || task.GetNodeID() == nodeID)
		s.T().Logf("stage=stats kind=%s task=%d segment=%d node=%d version=%d", kind, id, task.GetSegmentID(), nodeID, task.GetVersion())
	}
	fixture.assertRegistration(s.T(), nodeID, s.ezID, collectionID)
	return finished
}

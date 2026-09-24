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
	"crypto/sha256"
	"fmt"
	"maps"
	"os/exec"
	"path"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/testutils"
	"github.com/milvus-io/milvus/tests/integration"
	"github.com/milvus-io/milvus/tests/integration/cluster/process"
	"github.com/milvus-io/milvus/tests/integration/cmek/inspector"
)

type RawDataV3GrowingSuite struct {
	rawDataSuite
}

func (s *RawDataV3GrowingSuite) SetupSuite() {
	s.growingSource = true
	s.WithMilvusConfig("MILVUS_CMEK_FIXTURE_STRICT_CONTEXT", "true")
	s.setupRawData(3)
}

func TestRawDataV3GrowingSuite(t *testing.T) {
	suite.Run(t, new(RawDataV3GrowingSuite))
}

type growingRow struct {
	batchID int64
	payload string
	vector  []float32
}

func growingVector(pk int64) []float32 {
	vector := make([]float32, rawDataDim)
	vector[0] = float32(pk)
	for i := 1; i < rawDataDim; i++ {
		vector[i] = float32((pk+int64(i))%97) / 100
	}
	return vector
}

func growingCampaign(name string) rawDataCampaign {
	return rawDataCampaign{
		name:      name,
		shardsNum: 1,
		schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
			{Name: fixturePrimaryKey, IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
			{Name: "batch_id", DataType: schemapb.DataType_Int64},
			{Name: "payload", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: "max_length", Value: "128"}}},
			vectorSchema("float_vector", schemapb.DataType_FloatVector, rawDataDim),
		}},
		loadFields: []string{fixturePrimaryKey, "batch_id", "payload", "float_vector"},
	}
}

func growingBatch(start, count int, batchID int64) ([]*schemapb.FieldData, map[int64]growingRow) {
	pks, batches := make([]int64, count), make([]int64, count)
	payloads, vectors := make([]string, count), make([]float32, count*rawDataDim)
	expected := make(map[int64]growingRow, count)
	for i := 0; i < count; i++ {
		pk := int64(start + i)
		pks[i], batches[i] = pk, batchID
		payloads[i] = fmt.Sprintf("batch=%d/pk=%d", batchID, pk)
		vector := growingVector(pk)
		copy(vectors[i*rawDataDim:], vector)
		expected[pk] = growingRow{batchID, payloads[i], vector}
	}
	return []*schemapb.FieldData{
		testutils.NewInt64FieldDataWithValue(fixturePrimaryKey, pks),
		testutils.NewInt64FieldDataWithValue("batch_id", batches),
		testutils.NewVarCharFieldDataWithValue("payload", payloads),
		testutils.NewFloatVectorFieldDataWithValue("float_vector", vectors, rawDataDim),
	}, expected
}

func (s *rawDataSuite) insertGrowingBatch(ctx context.Context, description *milvuspb.DescribeCollectionResponse, start, count int, batchID int64, expected map[int64]growingRow) uint64 {
	fields, rows := growingBatch(start, count, batchID)
	for pk, row := range rows {
		s.Require().NotContains(expected, pk)
		expected[pk] = row
	}
	result := s.insertRawDataBatch(ctx, description, fields, count)
	s.Require().Positive(result.GetTimestamp())
	return result.GetTimestamp()
}

type growingObject struct {
	Columns    string
	Start, End int64
	Digest     [sha256.Size]byte
}

type growingSnapshot struct {
	CollectionID, SegmentID              int64
	State                                streamingpb.SegmentAssignmentState
	ModifiedRows, CommittedRows          int64
	SegmentCheckpoint, ChannelCheckpoint uint64
	Manifest                             inspector.ManifestLocatorV3
	ManifestPath                         string
	Objects                              map[string]growingObject
	GroupPaths                           map[string][]string
}

func (s *rawDataSuite) readGrowingMeta(ctx context.Context, description *milvuspb.DescribeCollectionResponse) (*streamingpb.SegmentAssignmentMeta, *streamingpb.WALCheckpoint, error) {
	channels := description.GetVirtualChannelNames()
	if len(channels) != 1 {
		return nil, nil, merr.WrapErrServiceInternalMsg("expected one vchannel, got %v", channels)
	}
	pchannel := funcutil.ToPhysicalChannel(channels[0])
	base := path.Join(s.Cluster.RootPath(), "meta/streamingnode-meta/wal", pchannel)
	response, err := s.Cluster.EtcdCli.Get(ctx, path.Join(base, "segment-assign")+"/", clientv3.WithPrefix())
	if err != nil {
		return nil, nil, err
	}
	var selected *streamingpb.SegmentAssignmentMeta
	for _, kv := range response.Kvs {
		var meta streamingpb.SegmentAssignmentMeta
		if err := proto.Unmarshal(kv.Value, &meta); err != nil {
			return nil, nil, err
		}
		if meta.GetCollectionId() != description.GetCollectionID() || meta.GetStat().GetLevel() != datapb.SegmentLevel_L1 {
			continue
		}
		if selected != nil {
			return nil, nil, merr.WrapErrServiceInternalMsg("collection %d has multiple L1 growing segments %d and %d", description.GetCollectionID(), selected.GetSegmentId(), meta.GetSegmentId())
		}
		selected = &meta
	}
	if selected == nil {
		return nil, nil, merr.WrapErrServiceInternalMsg("no L1 assignment for collection %d", description.GetCollectionID())
	}
	cpResponse, err := s.Cluster.EtcdCli.Get(ctx, path.Join(base, "consume-checkpoint"))
	if err != nil {
		return nil, nil, err
	}
	if len(cpResponse.Kvs) != 1 {
		return nil, nil, merr.WrapErrServiceInternalMsg("missing channel checkpoint for %s", pchannel)
	}
	checkpoint := new(streamingpb.WALCheckpoint)
	if err := proto.Unmarshal(cpResponse.Kvs[0].Value, checkpoint); err != nil {
		return nil, nil, err
	}
	return selected, checkpoint, nil
}

// DataCoord owns the committed Growing manifest and its DML recovery position.
// The WAL assignment above tracks the live segment and consumed rows, but its
// persisted_storage is only populated by StreamingNode's own storage writer.
func (s *rawDataSuite) readGrowingSegmentInfo(ctx context.Context, assignment *streamingpb.SegmentAssignmentMeta) (*datapb.SegmentInfo, error) {
	key := path.Join(s.Cluster.RootPath(), "meta/datacoord-meta/s",
		strconv.FormatInt(assignment.GetCollectionId(), 10),
		strconv.FormatInt(assignment.GetPartitionId(), 10),
		strconv.FormatInt(assignment.GetSegmentId(), 10))
	response, err := s.Cluster.EtcdCli.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if len(response.Kvs) != 1 {
		return nil, merr.WrapErrServiceInternalMsg("missing DataCoord SegmentInfo for growing segment %d", assignment.GetSegmentId())
	}
	segment := new(datapb.SegmentInfo)
	if err := proto.Unmarshal(response.Kvs[0].Value, segment); err != nil {
		return nil, err
	}
	if segment.GetID() != assignment.GetSegmentId() || segment.GetCollectionID() != assignment.GetCollectionId() ||
		segment.GetPartitionID() != assignment.GetPartitionId() {
		return nil, merr.WrapErrServiceInternalMsg("DataCoord SegmentInfo identity differs from WAL assignment %d", assignment.GetSegmentId())
	}
	return segment, nil
}

func (s *rawDataSuite) readGrowingSnapshot(ctx context.Context, description *milvuspb.DescribeCollectionResponse) (*growingSnapshot, error) {
	meta, checkpoint, err := s.readGrowingMeta(ctx, description)
	if err != nil {
		return nil, err
	}
	if meta.GetState() != streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING || meta.GetStorageVersion() != 3 {
		return nil, merr.WrapErrServiceInternalMsg("segment %d state=%s storage=%d, expected growing V3", meta.GetSegmentId(), meta.GetState(), meta.GetStorageVersion())
	}
	segment, err := s.readGrowingSegmentInfo(ctx, meta)
	if err != nil {
		return nil, err
	}
	if segment.GetState() != commonpb.SegmentState_Growing || segment.GetLevel() != datapb.SegmentLevel_L1 ||
		segment.GetStorageVersion() != 3 || !segment.GetIsCreatedByStreaming() {
		return nil, merr.WrapErrServiceInternalMsg("DataCoord segment %d state=%s level=%s storage=%d streaming=%t, expected Growing L1 V3",
			segment.GetID(), segment.GetState(), segment.GetLevel(), segment.GetStorageVersion(), segment.GetIsCreatedByStreaming())
	}
	manifestPath := segment.GetManifestPath()
	locator, err := inspector.ParseManifestLocatorV3(manifestPath)
	if err != nil {
		return nil, err
	}
	manifest, err := s.Cluster.ChunkManager.Read(ctx, locator.ObjectPath())
	if err != nil {
		return nil, err
	}
	objects, err := inspector.ParseParquetObjectsV3(manifest, locator.BasePath)
	if err != nil {
		return nil, err
	}
	snapshot := &growingSnapshot{
		CollectionID: description.GetCollectionID(), SegmentID: meta.GetSegmentId(), State: meta.GetState(),
		ModifiedRows: int64(meta.GetStat().GetModifiedRows()), CommittedRows: segment.GetNumOfRows(),
		SegmentCheckpoint: segment.GetDmlPosition().GetTimestamp(), ChannelCheckpoint: checkpoint.GetTimeTick(),
		Manifest: locator, ManifestPath: manifestPath, Objects: make(map[string]growingObject),
		GroupPaths: make(map[string][]string),
	}
	groupRows := make(map[string]int64)
	for _, object := range objects {
		raw, err := s.Cluster.ChunkManager.Read(ctx, object.Path)
		if err != nil {
			return nil, err
		}
		if _, err := inspector.InspectEncryptedParquet(raw, s.ezID, description.GetCollectionID()); err != nil {
			return nil, merr.Wrapf(err, "encrypted object %s", object.Path)
		}
		columns := strings.Join(object.Columns, ",")
		snapshot.Objects[object.Path] = growingObject{columns, object.Start, object.End, sha256.Sum256(raw)}
		snapshot.GroupPaths[columns] = append(snapshot.GroupPaths[columns], object.Path)
		groupRows[columns] += object.Rows
	}
	for _, field := range description.GetSchema().GetFields() {
		name := strconv.FormatInt(field.GetFieldID(), 10)
		covered := false
		for _, object := range objects {
			covered = covered || slices.Contains(object.Columns, name)
		}
		if !covered {
			return nil, merr.WrapErrServiceInternalMsg("growing manifest lacks field %s (%s)", field.GetName(), name)
		}
	}
	// Loon's start/end indices are within each file, and a later append can
	// start at zero again. Sum file slices in manifest order within each
	// column group, never across groups.
	for group, rows := range groupRows {
		if rows != snapshot.CommittedRows {
			return nil, merr.WrapErrServiceInternalMsg("column group %s covers %d rows, DataCoord committed %d", group, rows, snapshot.CommittedRows)
		}
	}
	if snapshot.CommittedRows > snapshot.ModifiedRows {
		return nil, merr.WrapErrServiceInternalMsg("committed rows %d exceed modified rows %d", snapshot.CommittedRows, snapshot.ModifiedRows)
	}
	return snapshot, nil
}

func (s *rawDataSuite) waitGrowingSnapshot(ctx context.Context, description *milvuspb.DescribeCollectionResponse, committed int64) *growingSnapshot {
	var last string
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		snapshot, err := s.readGrowingSnapshot(ctx, description)
		if err == nil {
			if snapshot.CommittedRows == committed {
				s.T().Logf("stage=growing-commit collection=%d segment=%d committed=%d modified=%d segmentCheckpoint=%d channelCheckpoint=%d manifest=%+v objects=%d", snapshot.CollectionID, snapshot.SegmentID, snapshot.CommittedRows, snapshot.ModifiedRows, snapshot.SegmentCheckpoint, snapshot.ChannelCheckpoint, snapshot.Manifest, len(snapshot.Objects))
				return snapshot
			}
			last = fmt.Sprintf("committed=%d modified=%d segment=%d manifest=%+v", snapshot.CommittedRows, snapshot.ModifiedRows, snapshot.SegmentID, snapshot.Manifest)
			if snapshot.CommittedRows > committed {
				s.T().Fatalf("unexpected early growing commit: wanted=%d %s", committed, last)
			}
		} else {
			last = err.Error()
		}
		select {
		case <-ctx.Done():
			s.T().Fatalf("waiting for growing commit %d: %s: %v", committed, last, ctx.Err())
		case <-ticker.C:
		}
	}
}

func (s *rawDataSuite) assertGrowingAdvance(previous, current *growingSnapshot, newRows int64) {
	s.Require().Equal(previous.CollectionID, current.CollectionID)
	s.Require().Equal(previous.SegmentID, current.SegmentID)
	s.Require().Equal(previous.CommittedRows+newRows, current.CommittedRows)
	for path, object := range previous.Objects {
		s.Require().Equal(object, current.Objects[path], "prior encrypted object changed: %s", path)
	}
	for group, paths := range previous.GroupPaths {
		currentPaths := current.GroupPaths[group]
		s.Require().GreaterOrEqual(len(currentPaths), len(paths), "column group %s lost prior files", group)
		s.Require().Equal(paths, currentPaths[:len(paths)], "column group %s reordered prior files", group)
	}
	s.Require().Greater(len(current.Objects), len(previous.Objects), "new commit has no new encrypted object")
}

func (s *rawDataSuite) assertGrowingUnchanged(ctx context.Context, description *milvuspb.DescribeCollectionResponse, previous *growingSnapshot) *growingSnapshot {
	current, err := s.readGrowingSnapshot(ctx, description)
	s.Require().NoError(err)
	s.Require().Equal(previous.SegmentID, current.SegmentID)
	s.Require().Equal(previous.CommittedRows, current.CommittedRows)
	s.Require().Equal(previous.Manifest, current.Manifest)
	s.Require().Equal(previous.SegmentCheckpoint, current.SegmentCheckpoint, "DataCoord recovery position changed without a commit")
	s.Require().Equal(previous.GroupPaths, current.GroupPaths, "growing file order changed without an expected commit")
	s.Require().True(maps.Equal(previous.Objects, current.Objects), "growing objects changed without an expected commit")
	return current
}

func (s *rawDataSuite) assertGrowingRows(ctx context.Context, collection string, expected map[int64]growingRow) {
	count, err := s.Cluster.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		DbName: s.dbName, CollectionName: collection, Expr: "", OutputFields: []string{"count(*)"}, ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
	})
	s.Require().NoError(merr.CheckRPCCall(count, err))
	s.Require().Len(count.GetFieldsData(), 1)
	s.Require().Len(count.GetFieldsData()[0].GetScalars().GetLongData().GetData(), 1)
	s.Require().Equal(int64(len(expected)), count.GetFieldsData()[0].GetScalars().GetLongData().GetData()[0])
	ids := make([]int64, 0, len(expected))
	for id := range expected {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for start := 0; start < len(ids); start += 100 {
		end := min(start+100, len(ids))
		selected := ids[start:end]
		values := make([]string, len(selected))
		for i, id := range selected {
			values[i] = strconv.FormatInt(id, 10)
		}
		result, err := s.Cluster.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
			DbName: s.dbName, CollectionName: collection, Expr: fmt.Sprintf("%s in [%s]", fixturePrimaryKey, strings.Join(values, ",")),
			OutputFields: []string{fixturePrimaryKey, "batch_id", "payload", "float_vector"}, ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
		})
		s.Require().NoError(merr.CheckRPCCall(result, err))
		fields := fieldDataByName(result.GetFieldsData())
		s.Require().Len(fields, 4)
		for _, name := range []string{fixturePrimaryKey, "batch_id", "payload", "float_vector"} {
			s.Require().Contains(fields, name)
		}
		actualIDs := fields[fixturePrimaryKey].GetScalars().GetLongData().GetData()
		actualBatch := fields["batch_id"].GetScalars().GetLongData().GetData()
		actualPayload := fields["payload"].GetScalars().GetStringData().GetData()
		actualVectors := fields["float_vector"].GetVectors().GetFloatVector().GetData()
		s.Require().Len(actualIDs, len(selected))
		s.Require().Len(actualBatch, len(selected))
		s.Require().Len(actualPayload, len(selected))
		s.Require().Len(actualVectors, len(selected)*rawDataDim)
		seen := make(map[int64]bool)
		for i, id := range actualIDs {
			s.Require().Contains(selected, id, "query returned an ID outside the requested page")
			want, ok := expected[id]
			s.Require().True(ok, "unexpected pk %d", id)
			s.Require().False(seen[id], "duplicate pk %d", id)
			seen[id] = true
			s.Require().Equal(want.batchID, actualBatch[i], "pk=%d", id)
			s.Require().Equal(want.payload, actualPayload[i], "pk=%d", id)
			s.Require().Equal(want.vector, actualVectors[i*rawDataDim:(i+1)*rawDataDim], "pk=%d", id)
		}
	}
	representatives := make(map[int64]int64)
	for _, id := range ids {
		batch := expected[id].batchID
		if _, exists := representatives[batch]; !exists {
			representatives[batch] = id
		}
	}
	for batch, id := range representatives {
		vector := growingVector(id)
		request := integration.ConstructSearchRequest(s.dbName, collection, "", "float_vector", schemapb.DataType_FloatVector,
			[]string{fixturePrimaryKey}, metric.L2, map[string]any{"ef": 200}, 1, rawDataDim, 1, -1)
		placeholder, err := proto.Marshal(funcutil.Float32VectorsToPlaceholderGroup([][]float32{vector}))
		s.Require().NoError(err)
		request.SearchInput = &milvuspb.SearchRequest_PlaceholderGroup{PlaceholderGroup: placeholder}
		result, err := s.Cluster.MilvusClient.Search(ctx, request)
		s.Require().NoError(merr.CheckRPCCall(result, err), "batch %d", batch)
		s.Require().Equal([]int64{id}, result.GetResults().GetIds().GetIntId().GetData(), "batch %d", batch)
		s.Require().Len(result.GetResults().GetScores(), 1)
		s.Require().InDelta(0, result.GetResults().GetScores()[0], 1e-6)
	}
}

func (s *rawDataSuite) flushGrowingCollection(ctx context.Context, description *milvuspb.DescribeCollectionResponse) []*datapb.SegmentInfo {
	collection := description.GetCollectionName()
	flush, err := s.Cluster.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{DbName: s.dbName, CollectionNames: []string{collection}})
	s.Require().NoError(merr.CheckRPCCall(flush, err))
	ids := flush.GetCollSegIDs()[collection].GetData()
	s.Require().NotEmpty(ids)
	s.WaitForFlush(ctx, ids, flush.GetCollFlushTs()[collection], s.dbName, collection)
	return s.rawFlushedSegments(collection, ids)
}

func (s *RawDataV3GrowingSuite) TestGrowingFlushBatches() {
	for _, tail := range []bool{true, false} {
		name := "emptyTail"
		if tail {
			name = "tail"
		}
		s.Run(name, func() {
			ctx, cancel := context.WithTimeout(s.Cluster.GetContext(), 5*time.Minute)
			defer cancel()
			campaign := growingCampaign(name)
			description := s.prepareRawDataCollection(ctx, campaign)
			expected := make(map[int64]growingRow)
			s.insertGrowingBatch(ctx, description, 0, 256, 1, expected)
			first := s.waitGrowingSnapshot(ctx, description, 256)
			s.insertGrowingBatch(ctx, description, 256, 256, 2, expected)
			second := s.waitGrowingSnapshot(ctx, description, 512)
			s.assertGrowingAdvance(first, second, 256)
			s.assertGrowingRows(ctx, description.GetCollectionName(), expected)
			if tail {
				s.insertGrowingBatch(ctx, description, 512, 64, 3, expected)
				s.assertGrowingRows(ctx, description.GetCollectionName(), expected)
				s.assertGrowingUnchanged(ctx, description, second)
			}
			segments := s.flushGrowingCollection(ctx, description)
			s.Require().Len(segments, 1)
			s.Require().Equal(int64(len(expected)), segments[0].GetNumOfRows())
			if tail {
				s.Require().NotEqual(second.ManifestPath, segments[0].GetManifestPath(), "tail flush must advance the manifest")
			} else {
				s.Require().Equal(second.ManifestPath, segments[0].GetManifestPath(), "empty-tail flush must reuse the committed manifest")
			}
			_, _ = s.inspectRawDataV3(ctx, segments, description.GetCollectionID(), "")
			finalLocator, err := inspector.ParseManifestLocatorV3(segments[0].GetManifestPath())
			s.Require().NoError(err)
			finalRaw, err := s.Cluster.ChunkManager.Read(ctx, finalLocator.ObjectPath())
			s.Require().NoError(err)
			finalObjects, err := inspector.ParseParquetObjectsV3(finalRaw, finalLocator.BasePath)
			s.Require().NoError(err)
			finalByPath := make(map[string]growingObject)
			finalByGroup := make(map[string][]inspector.ParquetObjectV3)
			for _, object := range finalObjects {
				raw, err := s.Cluster.ChunkManager.Read(ctx, object.Path)
				s.Require().NoError(err)
				group := strings.Join(object.Columns, ",")
				finalByPath[object.Path] = growingObject{group, object.Start, object.End, sha256.Sum256(raw)}
				finalByGroup[group] = append(finalByGroup[group], object)
			}
			for group, files := range finalByGroup {
				var rows int64
				paths := make([]string, 0, len(files))
				for _, file := range files {
					rows += file.Rows
					paths = append(paths, file.Path)
				}
				s.Require().Equal(int64(len(expected)), rows, "final manifest group %s has incomplete rows", group)
				prior := second.GroupPaths[group]
				s.Require().GreaterOrEqual(len(paths), len(prior), "final manifest group %s lost prior files", group)
				s.Require().Equal(prior, paths[:len(prior)], "final manifest group %s reordered prior files", group)
			}
			for path, old := range second.Objects {
				s.Require().Equal(old, finalByPath[path], "historical object changed: %s", path)
			}
			if tail {
				s.Require().Greater(len(finalByPath), len(second.Objects))
			} else {
				s.Require().True(maps.Equal(second.Objects, finalByPath))
			}
			s.reloadGrowingRows(ctx, description, campaign, segments, expected)
		})
	}
}

func (s *rawDataSuite) reloadGrowingRows(ctx context.Context, description *milvuspb.DescribeCollectionResponse, campaign rawDataCampaign, segments []*datapb.SegmentInfo, expected map[int64]growingRow) {
	s.assertNoPhysicalVectorIndex(ctx, segments, description.GetSchema())
	collection := description.GetCollectionName()
	release, err := s.Cluster.MilvusClient.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{DbName: s.dbName, CollectionName: collection})
	s.Require().NoError(merr.CheckRPCCall(release, err))
	s.waitParquetReleased(ctx, description.GetCollectionID())
	load, err := s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{DbName: s.dbName, CollectionName: collection, ReplicaNumber: 1, LoadFields: campaign.loadFields})
	s.Require().NoError(merr.CheckRPCCall(load, err))
	s.WaitForLoadWithDB(ctx, s.dbName, collection)
	s.assertLoadedFields(ctx, description.GetCollectionID(), requestedFieldIDs(description.GetSchema(), campaign.loadFields))
	s.assertRawLoadedSegments(ctx, description.GetCollectionID(), segments)
	s.assertGrowingRows(ctx, collection, expected)
	s.assertNoPhysicalVectorIndex(ctx, segments, description.GetSchema())
}

func (s *rawDataSuite) waitGrowingOnNode(ctx context.Context, nodeID, collectionID, segmentID int64) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		for _, node := range s.Cluster.GetAllStreamingNodes() {
			if node.GetNodeID() != nodeID {
				continue
			}
			response, err := node.MustGetClient(ctx).GetDataDistribution(ctx, &querypb.GetDataDistributionRequest{LastUpdateTs: 0, SupportDelta: false})
			if err = merr.CheckRPCCall(response, err); err != nil {
				break
			}
			for _, view := range response.GetLeaderViews() {
				_, growing := view.GetGrowingSegments()[segmentID]
				if view.GetCollection() == collectionID && growing && view.GetStatus().GetServiceable() {
					return
				}
			}
		}
		select {
		case <-ctx.Done():
			s.T().Fatalf("node %d did not serve growing segment %d after recovery: %v", nodeID, segmentID, ctx.Err())
		case <-ticker.C:
		}
	}
}

// stopAndRecoverGrowing preserves the cluster's WAL, metadata and object store.
// In-flight sync may finish during Stop; read the resulting persistence boundary.
func (s *rawDataSuite) stopAndRecoverGrowing(ctx context.Context, description *milvuspb.DescribeCollectionResponse, segmentID, nodeID int64) *growingSnapshot {
	var old *process.StreamingNodeProcess
	for _, node := range s.Cluster.GetAllStreamingNodes() {
		if node.GetNodeID() == nodeID {
			old = node
			break
		}
	}
	s.Require().NotNil(old, "channel owner %d is absent", nodeID)
	if err := old.Stop(30 * time.Second); err != nil {
		// The existing component shutdown deadline exits with code 1 when
		// this single-node fixture cannot migrate its remaining channel.
		var exitErr *exec.ExitError
		s.Require().ErrorAs(err, &exitErr)
		s.Require().Equal(1, exitErr.ExitCode(), "unexpected process exit from Stop")
	}
	after, err := s.readGrowingSnapshot(ctx, description)
	s.Require().NoError(err, "stopped segment must still be Growing for this recovery case")
	s.Require().Equal(segmentID, after.SegmentID)
	s.T().Logf("stage=stopped-growing segment=%d committed=%d modified=%d segmentCheckpoint=%d channelCheckpoint=%d manifest=%s",
		after.SegmentID, after.CommittedRows, after.ModifiedRows, after.SegmentCheckpoint, after.ChannelCheckpoint, after.ManifestPath)
	replacement := s.Cluster.AddStreamingNode()
	s.Require().NotEqual(nodeID, replacement.GetNodeID())
	s.waitGrowingOnNode(ctx, replacement.GetNodeID(), description.GetCollectionID(), segmentID)
	return after
}

func (s *RawDataV3GrowingSuite) TestGrowingFlushRecovery() {
	ctx, cancel := context.WithTimeout(s.Cluster.GetContext(), 5*time.Minute)
	defer cancel()
	campaign := growingCampaign("recovery")
	description := s.prepareRawDataCollection(ctx, campaign)
	expected := make(map[int64]growingRow)
	owner := s.Cluster.DefaultStreamingNode().GetNodeID()
	s.insertGrowingBatch(ctx, description, 0, 256, 1, expected)
	first := s.waitGrowingSnapshot(ctx, description, 256)
	s.insertGrowingBatch(ctx, description, 256, 256, 2, expected)
	second := s.waitGrowingSnapshot(ctx, description, 512)
	s.assertGrowingAdvance(first, second, 256)
	s.insertGrowingBatch(ctx, description, 512, 64, 3, expected)
	s.assertGrowingRows(ctx, description.GetCollectionName(), expected)
	stopped := s.stopAndRecoverGrowing(ctx, description, second.SegmentID, owner)
	s.Require().GreaterOrEqual(stopped.CommittedRows, second.CommittedRows)
	s.Require().LessOrEqual(stopped.CommittedRows, int64(len(expected)))
	for object, before := range second.Objects {
		s.Require().Equal(before, stopped.Objects[object], "historical object changed during Stop: %s", object)
	}
	s.assertGrowingRows(ctx, description.GetCollectionName(), expected)
	s.insertGrowingBatch(ctx, description, 576, 256, 4, expected)
	third := s.waitGrowingSnapshot(ctx, description, int64(len(expected)))
	s.assertGrowingAdvance(stopped, third, int64(len(expected))-stopped.CommittedRows)
	s.assertGrowingRows(ctx, description.GetCollectionName(), expected)
	segments := s.flushGrowingCollection(ctx, description)
	s.Require().Len(segments, 1)
	s.Require().Equal(int64(len(expected)), segments[0].GetNumOfRows())
	_, _ = s.inspectRawDataV3(ctx, segments, description.GetCollectionID(), "")
	s.reloadGrowingRows(ctx, description, campaign, segments, expected)
}

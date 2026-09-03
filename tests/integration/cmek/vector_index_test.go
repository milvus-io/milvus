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
	"encoding/binary"
	"encoding/json"
	"math"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/tests/integration"
	"github.com/milvus-io/milvus/tests/integration/cmek/inspector"
)

const (
	vectorIndexRows       = 512
	vectorIndexDim        = 16
	vectorIndexFieldID    = int64(101)
	vectorIndexName       = "cmek_hnsw"
	vectorIndexType       = "HNSW"
	vectorIndexVersion    = int32(8)
	vectorIndexPathFormat = indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED
)

type VectorIndexV2Suite struct {
	integration.MiniClusterSuite
	dbName string
	ezID   int64
}

func (s *VectorIndexV2Suite) SetupSuite() {
	s.WithOptions(integration.WithoutResetDeploymentWhenTestTearDown())
	s.WithMilvusConfig("common.storage.useLoonFFI", "false")
	s.WithMilvusConfig("dataNode.storage.format", "parquet")
	s.WithMilvusConfig("common.storage.enableGrowingSourceFlush", "false")
	s.WithMilvusConfig("dataCoord.targetVecIndexVersion", strconv.Itoa(int(vectorIndexVersion)))
	s.WithMilvusConfig("dataCoord.forceRebuildSegmentIndex", "true")
	s.WithMilvusConfig("dataCoord.index.storePathVersion", "0")
	s.WithMilvusConfig("indexCoord.segment.minSegmentNumRowsToEnableIndex", "64")
	s.WithMilvusConfig("queryNode.segcore.interimIndex.enableIndex", "false")
	s.WithMilvusConfig("queryNode.segcore.tieredStorage.warmup.vectorIndex", common.WarmupSync)
	s.WithMilvusConfig("queryNode.segcore.tieredStorage.evictionEnabled", "false")
	s.WithMilvusConfig("queryNode.preferFieldDataWhenIndexHasRawData", "false")
	s.WithMilvusConfig("queryNode.enableSegmentPrune", "false")
	s.WithMilvusConfig("queryNode.enableSegmentFilter", "false")
	s.WithMilvusConfig("proxy.partialResultRequiredDataRatio", "1")
	s.MiniClusterSuite.SetupSuite()

	ctx := s.Cluster.GetContext()
	s.dbName = "cmek_vector_index_" + funcutil.GenRandomStr()
	status, err := s.Cluster.MilvusClient.CreateDatabase(ctx, &milvuspb.CreateDatabaseRequest{
		DbName: s.dbName,
		Properties: []*commonpb.KeyValuePair{
			{Key: common.EncryptionEnabledKey, Value: "true"},
			{Key: common.EncryptionRootKeyKey, Value: "fixture-root-key"},
		},
	})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	describe, err := s.Cluster.MilvusClient.DescribeDatabase(ctx, &milvuspb.DescribeDatabaseRequest{DbName: s.dbName})
	s.Require().NoError(merr.CheckRPCCall(describe, err))
	s.ezID = describe.GetDbID()
	s.Require().Positive(s.ezID)
}

func (s *VectorIndexV2Suite) TearDownSuite() {
	if s.Cluster != nil && s.dbName != "" {
		status, err := s.Cluster.MilvusClient.DropDatabase(context.Background(), &milvuspb.DropDatabaseRequest{DbName: s.dbName})
		s.NoError(merr.CheckRPCCall(status, err))
	}
	s.MiniClusterSuite.TearDownSuite()
}

func TestVectorIndexV2Suite(t *testing.T) {
	suite.Run(t, new(VectorIndexV2Suite))
}

func (s *VectorIndexV2Suite) TestVectorIndexData() {
	ctx := s.Cluster.GetContext()
	collection := "cmek_vector_index_" + funcutil.GenRandomStr()
	schema := &schemapb.CollectionSchema{
		Name:       collection,
		Properties: []*commonpb.KeyValuePair{{Key: common.WarmupVectorIndexKey, Value: common.WarmupSync}},
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: fixturePrimaryKey, IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
			{
				FieldID: vectorIndexFieldID, Name: fixtureVectorName, DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: strconv.Itoa(vectorIndexDim)}, {Key: common.WarmupKey, Value: common.WarmupSync}},
			},
		},
	}
	marshaled, err := proto.Marshal(schema)
	s.Require().NoError(err)
	status, err := s.Cluster.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName: s.dbName, CollectionName: collection, Schema: marshaled, ShardsNum: common.DefaultShardsNum,
	})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	defer s.cleanupVectorCollection(collection)
	describeCollection, err := s.Cluster.MilvusClient.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
		DbName: s.dbName, CollectionName: collection,
	})
	s.Require().NoError(merr.CheckRPCCall(describeCollection, err))
	collectionID := describeCollection.GetCollectionID()
	s.Require().Equal(strconv.FormatInt(s.ezID, 10), propertyValue(describeCollection.GetProperties(), common.EncryptionEzIDKey))

	vectors := deterministicFloatVectors(fixtureVectorName, vectorIndexRows, vectorIndexDim)
	insert, err := s.Cluster.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
		DbName: s.dbName, CollectionName: collection,
		FieldsData: []*schemapb.FieldData{newInt64FieldData(fixturePrimaryKey, sequentialIDs(vectorIndexRows)), vectors},
		HashKeys:   integration.GenerateHashKeys(vectorIndexRows), NumRows: vectorIndexRows,
	})
	s.Require().NoError(merr.CheckRPCCall(insert, err))
	flush, err := s.Cluster.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{DbName: s.dbName, CollectionNames: []string{collection}})
	s.Require().NoError(merr.CheckRPCCall(flush, err))
	flushedIDs := flush.GetCollSegIDs()[collection].GetData()
	s.Require().NotEmpty(flushedIDs)
	s.WaitForFlush(ctx, flushedIDs, flush.GetCollFlushTs()[collection], s.dbName, collection)
	segments := s.vectorSealedSegments(collection)
	s.assertVectorFlushProducedSegment(flushedIDs, segments)
	s.assertVectorOnlyColumnGroup(segments)

	indexParams := []*commonpb.KeyValuePair{
		{Key: common.IndexTypeKey, Value: vectorIndexType},
		{Key: common.MetricTypeKey, Value: metric.L2},
		{Key: "M", Value: "30"},
		{Key: "efConstruction", Value: "360"},
		{Key: common.WarmupKey, Value: common.WarmupSync},
	}
	status, err = s.Cluster.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		DbName: s.dbName, CollectionName: collection, FieldName: fixtureVectorName, IndexName: vectorIndexName, ExtraParams: indexParams,
	})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.WaitForIndexBuiltWithDB(ctx, s.dbName, collection, fixtureVectorName)
	indexID := s.assertVectorIndexDescription(ctx, collection)

	sets := s.locateVectorIndex(ctx, segments, indexID)
	s.inspectVectorIndexObjects(ctx, sets)
	// Refresh both segment and index metadata before release so replacement
	// segments/builds cannot enter the cold-load proof unchecked.
	segments = s.vectorSealedSegments(collection)
	s.assertVectorOnlyColumnGroup(segments)
	sets = s.locateVectorIndex(ctx, segments, indexID)
	s.inspectVectorIndexObjects(ctx, sets)

	release, err := s.Cluster.MilvusClient.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{DbName: s.dbName, CollectionName: collection})
	s.Require().NoError(merr.CheckRPCCall(release, err))
	s.CheckCollectionCacheReleased(collectionID)
	load, err := s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName: s.dbName, CollectionName: collection, ReplicaNumber: 1,
		LoadFields: []string{fixturePrimaryKey, fixtureVectorName},
	})
	s.Require().NoError(merr.CheckRPCCall(load, err))
	s.WaitForLoadWithDB(ctx, s.dbName, collection)
	s.assertVectorLoadedFields(ctx, collectionID)
	s.assertLoadedVectorIndexes(ctx, sets)
	s.assertVectorIndexSearch(ctx, collection, firstFloatVector([]*schemapb.FieldData{vectors}, fixtureVectorName, vectorIndexDim))
}

func (s *VectorIndexV2Suite) assertVectorIndexDescription(ctx context.Context, collection string) int64 {
	response, err := s.Cluster.MilvusClient.DescribeIndex(ctx, &milvuspb.DescribeIndexRequest{
		DbName: s.dbName, CollectionName: collection, FieldName: fixtureVectorName, IndexName: vectorIndexName,
	})
	s.Require().NoError(merr.CheckRPCCall(response, err))
	s.Require().Len(response.GetIndexDescriptions(), 1)
	description := response.GetIndexDescriptions()[0]
	s.Require().Equal(vectorIndexName, description.GetIndexName())
	s.Require().Equal(fixtureVectorName, description.GetFieldName())
	s.Require().Positive(description.GetIndexID())
	s.Require().Equal(commonpb.IndexState_Finished, description.GetState())
	s.Require().Equal(vectorIndexVersion, description.GetMinIndexVersion())
	s.Require().Equal(vectorIndexVersion, description.GetMaxIndexVersion())
	s.Require().Equal(vectorIndexType, propertyValue(description.GetParams(), common.IndexTypeKey))
	s.Require().Equal(metric.L2, propertyValue(description.GetParams(), common.MetricTypeKey))
	s.Require().Equal("30", propertyValue(description.GetParams(), "M"))
	s.Require().Equal("360", propertyValue(description.GetParams(), "efConstruction"))
	return description.GetIndexID()
}

func (s *VectorIndexV2Suite) locateVectorIndex(ctx context.Context, segments []*datapb.SegmentInfo, indexID int64) []inspector.VectorIndexSet {
	var sets []inspector.VectorIndexSet
	var locateErr error
	s.Require().Eventually(func() bool {
		requestCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		sets, locateErr = inspector.LocateVectorIndex(requestCtx, s.Cluster.MixCoordClient, segments,
			vectorIndexFieldID, indexID, vectorIndexType, metric.L2, vectorIndexVersion, vectorIndexPathFormat)
		return locateErr == nil && len(sets) == len(segments)
	}, 5*time.Minute, 500*time.Millisecond, "locate vector index: %v", locateErr)
	return sets
}

func (s *VectorIndexV2Suite) inspectVectorIndexObjects(ctx context.Context, sets []inspector.VectorIndexSet) {
	reader := inspector.ObjectReader{ChunkManager: s.Cluster.ChunkManager}
	for _, set := range sets {
		s.Require().Positive(set.IndexVersion)
		for _, path := range set.Paths {
			raw, err := reader.Read(ctx, inspector.Object{Path: path})
			s.Require().NoError(err, "segment=%d field=%d build=%d path=%s", set.SegmentID, set.FieldID, set.BuildID, path)
			s.Require().NotEmpty(raw, path)
			s.Require().NoError(inspector.InspectIndexDataV2(raw, inspector.VectorIndexObject{
				CollectionID: set.CollectionID, PartitionID: set.PartitionID, SegmentID: set.SegmentID,
				FieldID: set.FieldID, BuildID: set.BuildID, EZID: s.ezID,
			}), "segment=%d field=%d build=%d path=%s", set.SegmentID, set.FieldID, set.BuildID, path)
		}
	}
}

func (s *VectorIndexV2Suite) assertVectorOnlyColumnGroup(segments []*datapb.SegmentInfo) {
	for _, segment := range segments {
		s.Require().Equal(storage.StorageV2, segment.GetStorageVersion(), "segment %d", segment.GetID())
		matches := 0
		for _, fieldBinlog := range segment.GetBinlogs() {
			for _, child := range fieldBinlog.GetChildFields() {
				if child == vectorIndexFieldID {
					matches++
					s.Require().Equal([]int64{vectorIndexFieldID}, fieldBinlog.GetChildFields(), "vector field must be the column group's only child")
				}
			}
		}
		s.Require().Equal(1, matches, "segment %d must report exactly one vector column group", segment.GetID())
	}
}

func (s *VectorIndexV2Suite) assertLoadedVectorIndexes(ctx context.Context, sets []inspector.VectorIndexSet) {
	expected := make(map[int64]inspector.VectorIndexSet, len(sets))
	for _, set := range sets {
		expected[set.SegmentID] = set
	}
	s.Require().Eventually(func() bool {
		seen := make(map[int64]int)
		for _, process := range s.Cluster.GetAllQueryNodes() {
			client := process.MustGetClient(ctx)
			request, err := metricsinfo.ConstructGetMetricsRequest(map[string]interface{}{
				metricsinfo.MetricTypeKey:                     metricsinfo.SegmentKey,
				metricsinfo.MetricRequestParamCollectionIDKey: sets[0].CollectionID,
			})
			if err != nil {
				return false
			}
			response, err := client.GetMetrics(ctx, request)
			if err = merr.CheckRPCCall(response, err); err != nil {
				return false
			}
			var segments []*metricsinfo.Segment
			if err := json.Unmarshal([]byte(response.GetResponse()), &segments); err != nil {
				return false
			}
			for _, segment := range segments {
				if segment.CollectionID == sets[0].CollectionID && segment.State != "Sealed" {
					return false
				}
				want, ok := expected[segment.SegmentID]
				if !ok {
					continue
				}
				if segment.CollectionID != want.CollectionID || segment.PartitionID != want.PartitionID || segment.State != "Sealed" {
					return false
				}
				matches := 0
				for _, field := range segment.IndexedFields {
					if field.IndexFieldID == want.FieldID && field.IndexID == want.IndexID {
						matches++
						if field.BuildID != want.BuildID || !field.IsLoaded || !field.HasRawData {
							return false
						}
					}
				}
				if matches != 1 {
					return false
				}
				seen[segment.SegmentID]++
			}
		}
		for segmentID := range expected {
			if seen[segmentID] != 1 {
				return false
			}
		}
		return true
	}, 3*time.Minute, 500*time.Millisecond)
}

func (s *VectorIndexV2Suite) assertVectorLoadedFields(ctx context.Context, collectionID int64) {
	response, err := s.Cluster.MixCoordClient.ShowLoadCollections(ctx, &querypb.ShowCollectionsRequest{CollectionIDs: []int64{collectionID}})
	s.Require().NoError(merr.CheckRPCCall(response, err))
	s.Require().Len(response.GetLoadFields(), 1)
	actual := append([]int64(nil), response.GetLoadFields()[0].GetData()...)
	sort.Slice(actual, func(i, j int) bool { return actual[i] < actual[j] })
	s.Require().Equal([]int64{100, vectorIndexFieldID}, actual)
}

func (s *VectorIndexV2Suite) assertVectorIndexSearch(ctx context.Context, collection string, vector []float32) {
	request := integration.ConstructSearchRequest(s.dbName, collection, "", fixtureVectorName, schemapb.DataType_FloatVector,
		[]string{fixturePrimaryKey}, metric.L2, map[string]any{"ef": vectorIndexRows}, 1, vectorIndexDim, 1, -1)
	value := make([]byte, len(vector)*4)
	for i, item := range vector {
		binary.LittleEndian.PutUint32(value[i*4:], math.Float32bits(item))
	}
	placeholder, err := proto.Marshal(&commonpb.PlaceholderGroup{Placeholders: []*commonpb.PlaceholderValue{{
		Tag: "$0", Type: commonpb.PlaceholderType_FloatVector, Values: [][]byte{value},
	}}})
	s.Require().NoError(err)
	request.SearchInput = &milvuspb.SearchRequest_PlaceholderGroup{PlaceholderGroup: placeholder}
	response, err := s.Cluster.MilvusClient.Search(ctx, request)
	s.Require().NoError(merr.CheckRPCCall(response, err))
	s.Require().Equal([]int64{0}, response.GetResults().GetIds().GetIntId().GetData())
	s.Require().Len(response.GetResults().GetScores(), 1)
	s.Require().InDelta(0, response.GetResults().GetScores()[0], 1e-6)
}

func (s *VectorIndexV2Suite) vectorSealedSegments(collection string) []*datapb.SegmentInfo {
	var segments []*datapb.SegmentInfo
	s.Require().Eventually(func() bool {
		current, err := s.Cluster.ShowSegmentsWithDB(s.dbName, collection)
		if err != nil {
			return false
		}
		segments = segments[:0]
		for _, segment := range current {
			if (segment.GetState() == commonpb.SegmentState_Sealed || segment.GetState() == commonpb.SegmentState_Flushed) &&
				segment.GetNumOfRows() > 0 && !segment.GetCompacted() && !segment.GetIsInvisible() {
				segments = append(segments, segment)
			}
		}
		return len(segments) > 0
	}, 2*time.Minute, 500*time.Millisecond)
	return segments
}

func (s *VectorIndexV2Suite) assertVectorFlushProducedSegment(flushed []int64, segments []*datapb.SegmentInfo) {
	flushedIDs := make(map[int64]struct{}, len(flushed))
	for _, id := range flushed {
		flushedIDs[id] = struct{}{}
	}
	for _, segment := range segments {
		if _, ok := flushedIDs[segment.GetID()]; ok {
			return
		}
		for _, sourceID := range segment.GetCompactionFrom() {
			if _, ok := flushedIDs[sourceID]; ok {
				return
			}
		}
	}
	s.FailNow("no current sealed segment came from this flush", "flush=%v", flushed)
}

func (s *VectorIndexV2Suite) cleanupVectorCollection(collection string) {
	ctx := context.Background()
	_, _ = s.Cluster.MilvusClient.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{DbName: s.dbName, CollectionName: collection})
	status, err := s.Cluster.MilvusClient.DropCollection(ctx, &milvuspb.DropCollectionRequest{DbName: s.dbName, CollectionName: collection})
	s.NoError(merr.CheckRPCCall(status, err))
}

func sequentialIDs(rows int) []int64 {
	ids := make([]int64, rows)
	for i := range ids {
		ids[i] = int64(i)
	}
	return ids
}

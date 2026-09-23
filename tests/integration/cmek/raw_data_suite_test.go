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
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
	"github.com/milvus-io/milvus/tests/integration"
)

// rawDataSuite owns the cluster setup and metadata assertions shared by V2 and V3.
type rawDataSuite struct {
	integration.MiniClusterSuite
	dbName                 string
	ezID                   int64
	growingSource          bool
	captureLogs            bool
	growingLogDir          string
	interimIndex           bool
	growingBufferSize      int
	physicalIndexThreshold int
}

func (s *rawDataSuite) setupRawData(storageVersion int64) {
	s.WithOptions(integration.WithoutResetDeploymentWhenTestTearDown())
	s.WithMilvusConfig("common.storage.useLoonFFI", strconv.FormatBool(storageVersion == 3))
	s.WithMilvusConfig("dataNode.storage.format", "parquet")
	s.WithMilvusConfig("dataCoord.enableCompaction", "false")
	s.WithMilvusConfig("common.storage.enableGrowingSourceFlush", strconv.FormatBool(s.growingSource))
	indexThreshold := s.physicalIndexThreshold
	if indexThreshold == 0 {
		indexThreshold = 1024
	}
	s.WithMilvusConfig("indexCoord.segment.minSegmentNumRowsToEnableIndex", strconv.Itoa(indexThreshold))
	s.WithMilvusConfig("queryNode.segcore.interimIndex.enableIndex", strconv.FormatBool(s.interimIndex))
	if s.growingSource {
		bufferSize := s.growingBufferSize
		if bufferSize == 0 {
			bufferSize = 32768
		}
		s.WithMilvusConfig("dataNode.segment.insertBufSize", strconv.Itoa(bufferSize))
		if s.interimIndex {
			s.WithMilvusConfig("queryNode.segcore.interimIndex.nlist", "4")
			s.WithMilvusConfig("queryNode.segcore.interimIndex.indexBuildRatio", "0")
		}
		s.WithMilvusConfig("dataNode.segment.syncPeriod", "3600")
		s.WithMilvusConfig("dataCoord.segment.maxIdleTime", "3600")
		s.WithMilvusConfig("dataNode.memory.forceSyncEnable", "false")
	}
	s.WithMilvusConfig("queryNode.segcore.tieredStorage.warmup.scalarField", "sync")
	s.WithMilvusConfig("queryNode.segcore.tieredStorage.warmup.vectorField", "sync")
	if s.growingSource || s.captureLogs {
		s.growingLogDir = s.T().TempDir()
		s.WithMilvusConfig("log.file.rootPath", s.growingLogDir)
		s.WithMilvusConfig("log.format", "json")
	}
	s.SetupSuite()

	ctx := s.Cluster.GetContext()
	s.dbName = fmt.Sprintf("cmek_raw_v%d_%s", storageVersion, funcutil.GenRandomStr())
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
	s.Require().Equal(strconv.FormatInt(s.ezID, 10), propertyValue(describe.GetProperties(), common.EncryptionEzIDKey))
}

func (s *rawDataSuite) TearDownSuite() {
	if s.Cluster != nil && s.dbName != "" {
		status, err := s.Cluster.MilvusClient.DropDatabase(context.Background(), &milvuspb.DropDatabaseRequest{DbName: s.dbName})
		s.NoError(merr.CheckRPCCall(status, err))
	}
	s.MiniClusterSuite.TearDownSuite()
}

func (s *rawDataSuite) createRawVectorIndexes(ctx context.Context, collection string, schema *schemapb.CollectionSchema) {
	create := func(fieldName string, field *schemapb.FieldSchema) {
		if !typeutil.IsVectorType(field.GetDataType()) {
			return
		}
		indexType := integration.IndexFaissIDMap
		metricType := metric.L2
		vectorType := field.GetDataType()
		if field.GetDataType() == schemapb.DataType_ArrayOfVector {
			vectorType = field.GetElementType()
			indexType = integration.IndexHNSW
			metricType = metric.MaxSim
		}
		switch vectorType {
		case schemapb.DataType_BinaryVector:
			if field.GetDataType() == schemapb.DataType_ArrayOfVector {
				metricType = metric.MaxSimHamming
			} else {
				indexType = integration.IndexFaissBinIDMap
				metricType = metric.JACCARD
			}
		case schemapb.DataType_SparseFloatVector:
			indexType = integration.IndexSparseInvertedIndex
			metricType = metric.IP
		case schemapb.DataType_Int8Vector:
			indexType = integration.IndexHNSW
		}
		if s.interimIndex && field.GetDataType() == schemapb.DataType_FloatVector {
			// Growing interim indexes skip FLAT logical indexes.
			indexType = integration.IndexFaissIvfFlat
		}
		status, err := s.Cluster.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
			DbName: s.dbName, CollectionName: collection, FieldName: fieldName, IndexName: "raw_" + field.GetName(),
			ExtraParams: integration.ConstructIndexParam(rawDataDim, indexType, metricType),
		})
		s.Require().NoError(merr.CheckRPCCall(status, err), field.GetName())
		s.WaitForIndexBuiltWithDB(ctx, s.dbName, collection, fieldName)
	}
	for _, field := range schema.GetFields() {
		create(field.GetName(), field)
	}
	for _, structField := range schema.GetStructArrayFields() {
		for _, field := range structField.GetFields() {
			create(typeutil.ConcatStructFieldName(structField.GetName(), field.GetName()), field)
		}
	}
}

func (s *rawDataSuite) assertNoPhysicalVectorIndex(ctx context.Context, segments []*datapb.SegmentInfo, schema *schemapb.CollectionSchema) {
	ids := make([]int64, 0, len(segments))
	for _, segment := range segments {
		ids = append(ids, segment.GetID())
	}
	vectorIDs := make(map[int64]struct{})
	fields := append([]*schemapb.FieldSchema(nil), schema.GetFields()...)
	for _, field := range schema.GetStructArrayFields() {
		fields = append(fields, field.GetFields()...)
	}
	for _, field := range fields {
		if typeutil.IsVectorType(field.GetDataType()) {
			vectorIDs[field.GetFieldID()] = struct{}{}
		}
	}
	s.Require().NotEmpty(ids)
	s.Require().NotEmpty(vectorIDs)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		response, err := s.Cluster.MixCoordClient.GetIndexInfos(ctx, &indexpb.GetIndexInfoRequest{CollectionID: segments[0].GetCollectionID(), SegmentIDs: ids})
		s.Require().NoError(merr.CheckRPCCall(response, err))
		if completeVectorIndexMetadata(response, segments, vectorIDs) {
			for _, segment := range segments {
				for _, info := range response.GetSegmentInfo()[segment.GetID()].GetIndexInfos() {
					if _, ok := vectorIDs[info.GetFieldID()]; ok {
						s.Require().Empty(info.GetIndexFilePaths(), "raw vector segment %d has a physical index", segment.GetID())
					}
				}
			}
			return
		}
		select {
		case <-ctx.Done():
			s.T().Fatal(ctx.Err())
		case <-ticker.C:
		}
	}
}

func completeVectorIndexMetadata(response *indexpb.GetIndexInfoResponse, segments []*datapb.SegmentInfo, vectorFieldIDs map[int64]struct{}) bool {
	if response == nil {
		return false
	}
	for _, segment := range segments {
		seen := make(map[int64]int, len(vectorFieldIDs))
		for _, info := range response.GetSegmentInfo()[segment.GetID()].GetIndexInfos() {
			if _, target := vectorFieldIDs[info.GetFieldID()]; target {
				seen[info.GetFieldID()]++
			}
		}
		for fieldID := range vectorFieldIDs {
			if seen[fieldID] != 1 {
				return false
			}
		}
	}
	return true
}

func (s *rawDataSuite) assertLoadedFields(ctx context.Context, collectionID int64, expected []int64) {
	response, err := s.Cluster.MixCoordClient.ShowLoadCollections(ctx, &querypb.ShowCollectionsRequest{CollectionIDs: []int64{collectionID}})
	s.Require().NoError(merr.CheckRPCCall(response, err))
	s.Require().Equal([]int64{collectionID}, response.GetCollectionIDs())
	s.Require().Len(response.GetLoadFields(), 1)
	actual := append([]int64(nil), response.GetLoadFields()[0].GetData()...)
	sort.Slice(actual, func(i, j int) bool { return actual[i] < actual[j] })
	sort.Slice(expected, func(i, j int) bool { return expected[i] < expected[j] })
	s.Require().Equal(expected, actual)
}

func (s *rawDataSuite) assertRawLoadedSegments(ctx context.Context, collectionID int64, expected []*datapb.SegmentInfo) {
	expectedIDs := make(map[int64]struct{}, len(expected))
	for _, segment := range expected {
		expectedIDs[segment.GetID()] = struct{}{}
	}
	s.Require().Eventually(func() bool {
		seen := make(map[int64]int, len(expectedIDs))
		for _, process := range s.Cluster.GetAllQueryNodes() {
			client := process.MustGetClient(ctx)
			request, err := metricsinfo.ConstructGetMetricsRequest(map[string]interface{}{
				metricsinfo.MetricTypeKey: metricsinfo.SegmentKey, metricsinfo.MetricRequestParamCollectionIDKey: collectionID,
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
				if segment.CollectionID != collectionID || segment.State != "Sealed" {
					return false
				}
				if _, ok := expectedIDs[segment.SegmentID]; !ok {
					return false
				}
				seen[segment.SegmentID]++
			}
		}
		for segmentID := range expectedIDs {
			if seen[segmentID] != 1 {
				return false
			}
		}
		return true
	}, 3*time.Minute, 500*time.Millisecond)
}

func (s *rawDataSuite) rawFlushedSegments(collection string, flushed []int64) []*datapb.SegmentInfo {
	var segments []*datapb.SegmentInfo
	s.Require().Eventually(func() bool {
		current, err := s.Cluster.ShowSegmentsWithDB(s.dbName, collection)
		if err != nil {
			return false
		}
		segments = selectFlushSegments(flushed, current)
		return len(segments) > 0
	}, 2*time.Minute, 500*time.Millisecond, "no inspectable Segment was found for flush %v", flushed)
	return segments
}

func selectFlushSegments(flushed []int64, current []*datapb.SegmentInfo) []*datapb.SegmentInfo {
	flushedIDs := make(map[int64]struct{}, len(flushed))
	for _, id := range flushed {
		flushedIDs[id] = struct{}{}
	}
	segments := make([]*datapb.SegmentInfo, 0, len(flushed))
	for _, segment := range current {
		persisted := segment.GetState() == commonpb.SegmentState_Sealed || segment.GetState() == commonpb.SegmentState_Flushed
		compacted := segment.GetState() == commonpb.SegmentState_Dropped && segment.GetCompacted()
		if _, ok := flushedIDs[segment.GetID()]; ok &&
			(persisted || compacted) &&
			segment.GetNumOfRows() > 0 {
			segments = append(segments, segment)
		}
	}
	if len(segments) != len(flushedIDs) {
		return nil
	}
	return segments
}

func (s *rawDataSuite) cleanupRawCollection(collection string) {
	ctx := context.Background()
	_, _ = s.Cluster.MilvusClient.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{DbName: s.dbName, CollectionName: collection})
	status, err := s.Cluster.MilvusClient.DropCollection(ctx, &milvuspb.DropCollectionRequest{DbName: s.dbName, CollectionName: collection})
	s.NoError(merr.CheckRPCCall(status, err))
}

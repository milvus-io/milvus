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
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v3/util/testutils"
	"github.com/milvus-io/milvus/tests/integration"
	"github.com/milvus-io/milvus/tests/integration/cmek/inspector"
)

type RawDataV3ConsumerSuite struct{ rawDataSuite }

func (s *RawDataV3ConsumerSuite) SetupSuite() {
	s.physicalIndexThreshold = 256
	s.setupRawData(3)
}

func TestRawDataV3ConsumerSuite(t *testing.T) {
	suite.Run(t, new(RawDataV3ConsumerSuite))
}

func (s *RawDataV3ConsumerSuite) TestV3EncryptedManifestBuildIndex() {
	s.runV3EncryptedManifestBuildIndex()
}

func (s *rawDataSuite) sealedV3ObjectDigests(ctx context.Context, segments []*datapb.SegmentInfo, collectionID int64) (map[int64]inspector.ManifestLocatorV3, map[string][sha256.Size]byte) {
	locators, _ := s.inspectRawDataV3(ctx, segments, collectionID, "")
	digests := make(map[string][sha256.Size]byte)
	for _, locator := range locators {
		raw, err := s.Cluster.ChunkManager.Read(ctx, locator.ObjectPath())
		s.Require().NoError(err)
		digests[locator.ObjectPath()] = sha256.Sum256(raw)
		objects, err := inspector.ParseParquetObjectsV3(raw, locator.BasePath)
		s.Require().NoError(err)
		for _, object := range objects {
			data, err := s.Cluster.ChunkManager.Read(ctx, object.Path)
			s.Require().NoError(err)
			digests[object.Path] = sha256.Sum256(data)
		}
	}
	return locators, digests
}

func (s *rawDataSuite) waitPhysicalHNSW(ctx context.Context, description *milvuspb.DescribeCollectionResponse,
	segments []*datapb.SegmentInfo, fieldID, indexID int64,
) map[int64]int64 {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	ids := make([]int64, 0, len(segments))
	for _, segment := range segments {
		ids = append(ids, segment.GetID())
	}
	var last string
	for {
		response, err := s.Cluster.MixCoordClient.GetIndexInfos(ctx, &indexpb.GetIndexInfoRequest{
			CollectionID: description.GetCollectionID(), SegmentIDs: ids,
		})
		if err = merr.CheckRPCCall(response, err); err == nil {
			builds := make(map[int64]int64)
			for _, segment := range segments {
				info := response.GetSegmentInfo()[segment.GetID()]
				if info == nil {
					break
				}
				for _, index := range info.GetIndexInfos() {
					if index.GetFieldID() == fieldID && index.GetIndexID() == indexID && index.GetBuildID() > 0 && len(index.GetIndexFilePaths()) > 0 {
						builds[segment.GetID()] = index.GetBuildID()
						for _, object := range index.GetIndexFilePaths() {
							s.Require().NotEmpty(object)
						}
					}
				}
			}
			if len(builds) == len(segments) {
				return builds
			}
			last = fmt.Sprintf("physical builds=%v want=%v", builds, ids)
		} else {
			last = err.Error()
		}
		select {
		case <-ctx.Done():
			s.T().Fatalf("HNSW physical index not built: %s: %v", last, ctx.Err())
		case <-ticker.C:
		}
	}
}

func (s *rawDataSuite) waitLoadedHNSW(ctx context.Context, description *milvuspb.DescribeCollectionResponse, builds map[int64]int64, fieldID, indexID int64) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		loaded := make(map[int64]bool)
		for _, node := range s.Cluster.GetAllQueryNodes() {
			request, err := metricsinfo.ConstructGetMetricsRequest(map[string]interface{}{
				metricsinfo.MetricTypeKey:                     metricsinfo.SegmentKey,
				metricsinfo.MetricRequestParamCollectionIDKey: description.GetCollectionID(),
			})
			if err != nil {
				break
			}
			response, err := node.MustGetClient(ctx).GetMetrics(ctx, request)
			if err = merr.CheckRPCCall(response, err); err != nil {
				break
			}
			var segments []*metricsinfo.Segment
			if err := json.Unmarshal([]byte(response.GetResponse()), &segments); err != nil {
				break
			}
			for _, segment := range segments {
				buildID, target := builds[segment.SegmentID]
				if !target || segment.CollectionID != description.GetCollectionID() || segment.State != "Sealed" {
					continue
				}
				for _, index := range segment.IndexedFields {
					if index.IndexFieldID == fieldID && index.IndexID == indexID && index.BuildID == buildID && index.IsLoaded {
						loaded[segment.SegmentID] = true
					}
				}
			}
		}
		if len(loaded) == len(builds) {
			return
		}
		select {
		case <-ctx.Done():
			s.T().Fatalf("HNSW not loaded for build IDs %v; loaded=%v: %v", builds, loaded, ctx.Err())
		case <-ticker.C:
		}
	}
}

func (s *rawDataSuite) assertHNSWSearch(ctx context.Context, collection string, pk int64) {
	vector := consumerVector(pk)
	request := integration.ConstructSearchRequest(s.dbName, collection, "", "float_vector", schemapb.DataType_FloatVector,
		[]string{fixturePrimaryKey, "batch_id", "payload"}, metric.L2, integration.GetSearchParams(integration.IndexHNSW, metric.L2), 1, rawDataDim, 1, -1)
	placeholder, err := proto.Marshal(funcutil.Float32VectorsToPlaceholderGroup([][]float32{vector}))
	s.Require().NoError(err)
	request.SearchInput = &milvuspb.SearchRequest_PlaceholderGroup{PlaceholderGroup: placeholder}
	result, err := s.Cluster.MilvusClient.Search(ctx, request)
	s.Require().NoError(merr.CheckRPCCall(result, err))
	s.Require().Equal([]int64{pk}, result.GetResults().GetIds().GetIntId().GetData())
	s.Require().Len(result.GetResults().GetScores(), 1)
	s.Require().InDelta(0, result.GetResults().GetScores()[0], 1e-6)
}

func (s *rawDataSuite) runV3EncryptedManifestBuildIndex() {
	ctx, cancel := context.WithTimeout(s.Cluster.GetContext(), 8*time.Minute)
	defer cancel()
	name := "canonical_hnsw"
	total := 512
	campaign := consumerCampaign(name)
	description := s.prepareRawDataCollection(ctx, campaign)
	fields, expected := consumerBatch(0, total, 1)
	campaign.fields = fields
	s.insertRawDataBatch(ctx, description, fields, total)
	segments := s.flushConsumerCollection(ctx, description)
	var indexed *datapb.SegmentInfo
	for _, segment := range segments {
		if segment.GetNumOfRows() > int64(s.physicalIndexThreshold) || (s.physicalIndexThreshold == 0 && segment.GetNumOfRows() > 1024) {
			indexed = segment
		}
	}
	s.Require().NotNil(indexed, "no segment exceeds physical index threshold")
	beforeLocators, beforeDigests := s.sealedV3ObjectDigests(ctx, segments, description.GetCollectionID())
	collection := description.GetCollectionName()
	release, err := s.Cluster.MilvusClient.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{DbName: s.dbName, CollectionName: collection})
	s.Require().NoError(merr.CheckRPCCall(release, err))
	s.waitParquetReleased(ctx, description.GetCollectionID())
	dropped, err := s.Cluster.MilvusClient.DropIndex(ctx, &milvuspb.DropIndexRequest{
		DbName: s.dbName, CollectionName: collection, IndexName: "raw_float_vector",
	})
	s.Require().NoError(merr.CheckRPCCall(dropped, err))
	created, err := s.Cluster.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		DbName: s.dbName, CollectionName: collection, FieldName: "float_vector", IndexName: "cmek_hnsw",
		ExtraParams: integration.ConstructIndexParam(rawDataDim, integration.IndexHNSW, metric.L2),
	})
	s.Require().NoError(merr.CheckRPCCall(created, err))
	s.WaitForIndexBuiltWithDB(ctx, s.dbName, collection, "float_vector")
	indexDescription, err := s.Cluster.MilvusClient.DescribeIndex(ctx, &milvuspb.DescribeIndexRequest{
		DbName: s.dbName, CollectionName: collection, FieldName: "float_vector", IndexName: "cmek_hnsw",
	})
	s.Require().NoError(merr.CheckRPCCall(indexDescription, err))
	s.Require().Len(indexDescription.GetIndexDescriptions(), 1)
	index := indexDescription.GetIndexDescriptions()[0]
	s.Require().Equal(commonpb.IndexState_Finished, index.GetState())
	s.Require().Equal(integration.IndexHNSW, propertyValue(index.GetParams(), common.IndexTypeKey))
	s.Require().Positive(index.GetIndexID())
	fieldIDs := requestedFieldIDs(description.GetSchema(), []string{"float_vector"})
	s.Require().Len(fieldIDs, 1)
	builds := s.waitPhysicalHNSW(ctx, description, segments, fieldIDs[0], index.GetIndexID())
	s.Require().Contains(builds, indexed.GetID(), "above-threshold segment lacks physical HNSW")
	afterSegments := s.currentParquetSegments(ctx, description.GetCollectionID())
	afterLocators, afterDigests := s.sealedV3ObjectDigests(ctx, afterSegments, description.GetCollectionID())
	s.Require().True(maps.Equal(beforeLocators, afterLocators), "source manifest identity changed during HNSW build")
	s.Require().True(maps.Equal(beforeDigests, afterDigests), "encrypted source objects changed during HNSW build")
	load, err := s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName: s.dbName, CollectionName: collection, ReplicaNumber: 1, LoadFields: campaign.loadFields,
	})
	s.Require().NoError(merr.CheckRPCCall(load, err))
	s.WaitForLoadWithDB(ctx, s.dbName, collection)
	s.waitLoadedHNSW(ctx, description, builds, fieldIDs[0], index.GetIndexID())
	s.assertConsumerRows(ctx, collection, expected)
	s.assertHNSWSearch(ctx, collection, 0)
	s.assertHNSWSearch(ctx, collection, int64(total-1))
	s.T().Logf("stage=hnsw-complete source=%s collection=%d index=%d buildIDs=%v rows=%d", name, description.GetCollectionID(), index.GetIndexID(), builds, total)
}

type consumerRow struct {
	batchID int64
	payload string
	vector  []float32
}

func consumerVector(pk int64) []float32 {
	vector := make([]float32, rawDataDim)
	vector[0] = float32(pk)
	for i := 1; i < rawDataDim; i++ {
		vector[i] = float32((pk+int64(i))%97) / 100
	}
	return vector
}

func consumerCampaign(name string) rawDataCampaign {
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

func consumerBatch(start, count int, batchID int64) ([]*schemapb.FieldData, map[int64]consumerRow) {
	pks, batches := make([]int64, count), make([]int64, count)
	payloads, vectors := make([]string, count), make([]float32, count*rawDataDim)
	expected := make(map[int64]consumerRow, count)
	for i := 0; i < count; i++ {
		pk := int64(start + i)
		pks[i], batches[i] = pk, batchID
		payloads[i] = fmt.Sprintf("batch=%d/pk=%d", batchID, pk)
		vector := consumerVector(pk)
		copy(vectors[i*rawDataDim:], vector)
		expected[pk] = consumerRow{batchID, payloads[i], vector}
	}
	return []*schemapb.FieldData{
		testutils.NewInt64FieldDataWithValue(fixturePrimaryKey, pks),
		testutils.NewInt64FieldDataWithValue("batch_id", batches),
		testutils.NewVarCharFieldDataWithValue("payload", payloads),
		testutils.NewFloatVectorFieldDataWithValue("float_vector", vectors, rawDataDim),
	}, expected
}

func (s *rawDataSuite) assertConsumerRows(ctx context.Context, collection string, expected map[int64]consumerRow) {
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
		vector := consumerVector(id)
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

func (s *rawDataSuite) flushConsumerCollection(ctx context.Context, description *milvuspb.DescribeCollectionResponse) []*datapb.SegmentInfo {
	collection := description.GetCollectionName()
	flush, err := s.Cluster.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{DbName: s.dbName, CollectionNames: []string{collection}})
	s.Require().NoError(merr.CheckRPCCall(flush, err))
	ids := flush.GetFlushCollSegIDs()[collection].GetData()
	s.Require().NotEmpty(ids)
	s.WaitForFlush(ctx, ids, flush.GetCollFlushTs()[collection], s.dbName, collection)
	return s.rawFlushedSegments(collection, ids)
}

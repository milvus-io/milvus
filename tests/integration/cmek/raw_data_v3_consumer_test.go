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
	"github.com/milvus-io/milvus/tests/integration"
	"github.com/milvus-io/milvus/tests/integration/cmek/inspector"
)

type RawDataV3ConsumerSuite struct{ rawDataSuite }

func (s *RawDataV3ConsumerSuite) SetupSuite() {
	s.captureLogs = true
	s.WithMilvusConfig("MILVUS_CMEK_FIXTURE_STRICT_CONTEXT", "true")
	s.physicalIndexThreshold = 256
	s.setupRawData(3)
}

func TestRawDataV3ConsumerSuite(t *testing.T) {
	suite.Run(t, new(RawDataV3ConsumerSuite))
}

func (s *RawDataV3ConsumerSuite) TestV3EncryptedManifestBuildIndex() {
	s.runV3EncryptedManifestBuildIndex(false)
}

func (s *RawDataV3GrowingSuite) TestV3EncryptedManifestBuildIndex() {
	s.runV3EncryptedManifestBuildIndex(true)
}

// The raw-data identity is captured before index creation and checked again
// after the actual build. Both paths use the independently parsed manifest.
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

func (s *rawDataSuite) waitHNSWBuildInput(ctx context.Context, description *milvuspb.DescribeCollectionResponse,
	segment *datapb.SegmentInfo, fieldID, indexID, buildID int64,
) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		entries, err := s.recoveryLogs()
		if err == nil {
			for _, entry := range entries {
				if entry.Message == "index build manifest input" && entry.CollectionID == description.GetCollectionID() &&
					entry.SegmentID == segment.GetID() && entry.FieldID == fieldID && entry.IndexID == indexID &&
					entry.BuildID == buildID && entry.SourceRows == segment.GetNumOfRows() && entry.ManifestPath == segment.GetManifestPath() {
					s.T().Logf("stage=hnsw-build-input node=%d build=%d segment=%d field=%d index=%d rows=%d manifest=%s",
						entry.NodeID, buildID, segment.GetID(), fieldID, indexID, entry.SourceRows, entry.ManifestPath)
					return
				}
			}
		}
		select {
		case <-ctx.Done():
			s.T().Fatalf("missing HNSW build input: segment=%d field=%d index=%d build=%d manifest=%s: %v",
				segment.GetID(), fieldID, indexID, buildID, segment.GetManifestPath(), ctx.Err())
		case <-ticker.C:
		}
	}
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
	vector := growingVector(pk)
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

func (s *rawDataSuite) runV3EncryptedManifestBuildIndex(growing bool) {
	ctx, cancel := context.WithTimeout(s.Cluster.GetContext(), 8*time.Minute)
	defer cancel()
	name := "canonical_hnsw"
	total := 512
	if growing {
		name, total = "growing_hnsw", 1536
	}
	campaign := growingCampaign(name)
	description := s.prepareRawDataCollection(ctx, campaign)
	expected := make(map[int64]growingRow)
	if growing {
		s.insertGrowingBatch(ctx, description, 0, 768, 1, expected)
		first := s.waitGrowingSnapshot(ctx, description, 768)
		s.insertGrowingBatch(ctx, description, 768, 768, 2, expected)
		second := s.waitGrowingSnapshot(ctx, description, 1536)
		s.assertGrowingAdvance(first, second, 768)
	} else {
		fields, rows := growingBatch(0, total, 1)
		campaign.fields = fields
		expected = rows
		s.insertRawDataBatch(ctx, description, fields, total)
	}
	segments := s.flushGrowingCollection(ctx, description)
	var indexed *datapb.SegmentInfo
	for _, segment := range segments {
		if segment.GetNumOfRows() > int64(s.physicalIndexThreshold) || (s.physicalIndexThreshold == 0 && segment.GetNumOfRows() > 1024) {
			indexed = segment
		}
	}
	s.Require().NotNil(indexed, "no segment exceeds physical index threshold")
	if growing {
		s.assertGrowingSourceFlush(segments)
	}
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
	for _, segment := range segments {
		s.waitHNSWBuildInput(ctx, description, segment, fieldIDs[0], index.GetIndexID(), builds[segment.GetID()])
	}
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
	s.assertGrowingRows(ctx, collection, expected)
	s.assertHNSWSearch(ctx, collection, 0)
	s.assertHNSWSearch(ctx, collection, int64(total-1))
	s.T().Logf("stage=hnsw-complete source=%s collection=%d index=%d buildIDs=%v rows=%d", name, description.GetCollectionID(), index.GetIndexID(), builds, total)
}

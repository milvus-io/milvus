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
	"path"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/iskorotkov/avro/v2/ocf"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/testutils"
	"github.com/milvus-io/milvus/tests/integration"
	"github.com/milvus-io/milvus/tests/integration/cmek/inspector"
)

type RawDataV3Suite struct {
	rawDataSuite
}

func (s *RawDataV3Suite) SetupSuite() {
	s.setupRawData(3)
}

func TestRawDataV3Suite(t *testing.T) {
	suite.Run(t, new(RawDataV3Suite))
}

// This first exploratory case exercises the non-TEXT DataNode Parquet path.
// It is not yet the complete Storage V3 encryption acceptance campaign.
func (s *RawDataV3Suite) TestParquetFlushAndColdRead() {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{Name: fixturePrimaryKey, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{Name: "value", DataType: schemapb.DataType_Int64},
			vectorSchema("float_vector", schemapb.DataType_FloatVector, rawDataDim),
		},
	}
	fields := []*schemapb.FieldData{
		testutils.NewInt64FieldData(fixturePrimaryKey, rawDataRows),
		testutils.NewInt64FieldData("value", rawDataRows),
		deterministicFloatVectors("float_vector", rawDataRows, rawDataDim),
	}
	s.runParquetCampaign(rawDataCampaign{
		name: "basic", schema: schema, fields: fields,
		loadFields: []string{fixturePrimaryKey, "value", "float_vector"},
		index:      true, search: true,
	})
}

func (s *RawDataV3Suite) TestParquetRawScalar() {
	s.runParquetCampaign(newRawScalarCampaign())
}

func (s *RawDataV3Suite) runParquetCampaign(c rawDataCampaign) {
	ctx, cancel := context.WithTimeout(s.Cluster.GetContext(), 3*time.Minute)
	defer cancel()
	collection := "cmek_raw_v3_parquet_" + c.name + "_" + funcutil.GenRandomStr()
	c.schema.Name = collection
	loadFieldIDs := requestedFieldIDs(c.schema, c.loadFields)
	marshaled, err := proto.Marshal(c.schema)
	s.Require().NoError(err)
	status, err := s.Cluster.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName: s.dbName, CollectionName: collection, Schema: marshaled, ShardsNum: 1,
	})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	defer s.cleanupRawCollection(collection)
	description, err := s.Cluster.MilvusClient.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
		DbName: s.dbName, CollectionName: collection,
	})
	s.Require().NoError(merr.CheckRPCCall(description, err))
	s.Require().Equal(strconv.FormatInt(s.ezID, 10), propertyValue(description.GetProperties(), common.EncryptionEzIDKey))
	s.Require().Equal(strconv.FormatInt(s.ezID, 10), propertyValue(description.GetSchema().GetProperties(), common.EncryptionEzIDKey))
	s.T().Logf("stage=create campaign=%s collection=%d storage_version=3 format=parquet growing_source=false rows=%d fields=%v", c.name, description.GetCollectionID(), rawDataRows, c.loadFields)
	if c.index {
		s.createRawVectorIndexes(ctx, collection, c.schema)
	}
	insert, err := s.Cluster.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
		DbName: s.dbName, CollectionName: collection, FieldsData: c.fields,
		HashKeys: integration.GenerateHashKeys(rawDataRows), NumRows: rawDataRows,
	})
	s.Require().NoError(merr.CheckRPCCall(insert, err))
	s.Require().Equal(int64(rawDataRows), insert.GetInsertCnt())
	s.T().Log("stage=insert complete")
	flush, err := s.Cluster.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{
		DbName: s.dbName, CollectionNames: []string{collection},
	})
	s.Require().NoError(merr.CheckRPCCall(flush, err))
	segmentIDs := flush.GetCollSegIDs()[collection].GetData()
	s.Require().NotEmpty(segmentIDs)
	s.T().Logf("stage=flush submitted segments=%v", segmentIDs)
	s.WaitForFlush(ctx, segmentIDs, flush.GetCollFlushTs()[collection], s.dbName, collection)
	segments := s.rawFlushedSegments(collection, segmentIDs)
	s.inspectParquetSegments(ctx, segments, description.GetCollectionID())
	segments = s.rawSealedSegments(collection)
	s.inspectParquetSegments(ctx, segments, description.GetCollectionID())
	if c.index {
		s.assertNoPhysicalVectorIndex(ctx, segments, description.GetSchema())
	}
	release, err := s.Cluster.MilvusClient.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{
		DbName: s.dbName, CollectionName: collection,
	})
	s.Require().NoError(merr.CheckRPCCall(release, err))
	s.CheckCollectionCacheReleased(description.GetCollectionID())
	segments = s.rawSealedSegments(collection)
	expected := s.inspectParquetSegments(ctx, segments, description.GetCollectionID())
	load, err := s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName: s.dbName, CollectionName: collection, ReplicaNumber: 1, LoadFields: c.loadFields,
	})
	s.Require().NoError(merr.CheckRPCCall(load, err))
	s.waitForParquetLoad(ctx, collection)
	s.assertLoadedFields(ctx, description.GetCollectionID(), loadFieldIDs)
	before := s.loadedParquetSnapshot(ctx, description.GetCollectionID(), expected)
	if c.index {
		s.assertNoPhysicalVectorIndex(ctx, segments, description.GetSchema())
	}
	s.assertRawDataOracle(ctx, collection, c.fields, c.loadFields)
	if c.search {
		s.assertExactFloatSearch(ctx, collection, "float_vector", firstFloatVector(c.fields, "float_vector", rawDataDim), rawDataRows)
	}
	after := s.loadedParquetSnapshot(ctx, description.GetCollectionID(), expected)
	s.Require().Equal(before, after, "loaded identity changed during the exploratory read window")
	current := make(map[int64]string)
	for _, segment := range s.rawSealedSegments(collection) {
		current[segment.GetID()] = segment.GetManifestPath()
	}
	s.Require().Equal(expected, current, "authoritative manifest changed during the exploratory read window")
	s.T().Logf("stage=cold-read complete loaded=%v", after)
}

func (s *RawDataV3Suite) waitForParquetLoad(ctx context.Context, collection string) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		progress, err := s.Cluster.MilvusClient.GetLoadingProgress(ctx, &milvuspb.GetLoadingProgressRequest{
			DbName: s.dbName, CollectionName: collection,
		})
		s.Require().NoError(merr.CheckRPCCall(progress, err), "load encrypted Parquet collection")
		if progress.GetProgress() == 100 {
			return
		}
		select {
		case <-ctx.Done():
			s.Require().NoError(ctx.Err(), "waiting for encrypted Parquet collection to load")
		case <-ticker.C:
		}
	}
}

func (s *RawDataV3Suite) inspectParquetSegments(ctx context.Context, segments []*datapb.SegmentInfo, collectionID int64) map[int64]string {
	expected := make(map[int64]string, len(segments))
	var rows int64
	for _, segment := range segments {
		s.Require().Equal(collectionID, segment.GetCollectionID())
		s.Require().Equal(int64(3), segment.GetStorageVersion(), "segment=%d", segment.GetID())
		var locator struct {
			BasePath string `json:"base_path"`
			Version  int64  `json:"ver"`
		}
		s.Require().NoError(json.Unmarshal([]byte(segment.GetManifestPath()), &locator))
		s.Require().NotEmpty(locator.BasePath)
		s.Require().Positive(locator.Version)
		manifestPath := path.Join(locator.BasePath, "_metadata", fmt.Sprintf("manifest-%d.avro", locator.Version))
		manifest, err := s.Cluster.ChunkManager.Read(ctx, manifestPath)
		s.Require().NoError(err, "segment=%d manifest=%s", segment.GetID(), manifestPath)
		s.inspectParquetManifest(ctx, locator.BasePath, manifest, collectionID, segment.GetID())
		expected[segment.GetID()] = segment.GetManifestPath()
		rows += segment.GetNumOfRows()
		s.T().Logf("stage=manifest segment=%d rows=%d locator=%s bytes=%d", segment.GetID(), segment.GetNumOfRows(), segment.GetManifestPath(), len(manifest))
	}
	s.Require().Equal(int64(rawDataRows), rows)
	return expected
}

type v3LoadedIdentity struct {
	NodeID   int64
	Version  int64
	Manifest string
}

func (s *RawDataV3Suite) loadedParquetSnapshot(ctx context.Context, collectionID int64, expected map[int64]string) map[int64]v3LoadedIdentity {
	loaded := make(map[int64]v3LoadedIdentity)
	serving := make(map[int64]int64)
	for _, client := range s.Cluster.GetAllStreamingAndQueryNodesClient() {
		response, err := client.GetDataDistribution(ctx, &querypb.GetDataDistributionRequest{LastUpdateTs: 0, SupportDelta: false})
		s.Require().NoError(merr.CheckRPCCall(response, err))
		s.Require().False(response.GetIsDelta())
		for _, segment := range response.GetSegments() {
			if segment.GetCollection() != collectionID {
				continue
			}
			s.Require().Contains(expected, segment.GetID())
			s.Require().NotContains(loaded, segment.GetID(), "expected one loaded replica")
			s.Require().Equal(expected[segment.GetID()], segment.GetManifestPath(), "loaded manifest differs from inspected manifest")
			loaded[segment.GetID()] = v3LoadedIdentity{response.GetNodeID(), segment.GetVersion(), segment.GetManifestPath()}
		}
		for _, view := range response.GetLeaderViews() {
			if view.GetCollection() != collectionID {
				continue
			}
			s.Require().Empty(view.GetGrowingSegmentIDs())
			s.Require().Empty(view.GetGrowingSegments())
			for segmentID, distribution := range view.GetSegmentDist() {
				s.Require().NotContains(serving, segmentID)
				serving[segmentID] = distribution.GetNodeID()
			}
		}
	}
	s.Require().Len(loaded, len(expected))
	s.Require().Len(serving, len(expected))
	for segmentID, identity := range loaded {
		s.Require().Equal(identity.NodeID, serving[segmentID], "leader routes to a different node")
	}
	return loaded
}

// Decode the exact manifest independently of the production Loon reader. The
// first non-TEXT case must inspect every referenced Parquet object and emit no
// LOB files. Structural-only validation and writer observation are separate
// requirements still needed for the complete acceptance campaign.
func (s *RawDataV3Suite) inspectParquetManifest(ctx context.Context, basePath string, raw []byte, collectionID, segmentID int64) {
	decoder, err := ocf.NewDecoder(bytes.NewReader(raw))
	s.Require().NoError(err, "segment=%d", segmentID)
	s.Require().True(decoder.HasNext(), "segment=%d has no manifest record", segmentID)
	var manifest struct {
		ColumnGroups []struct {
			Columns []string `avro:"columns"`
			Format  string   `avro:"format"`
			Files   []struct {
				Path  string `avro:"path"`
				Start int64  `avro:"start_index"`
				End   int64  `avro:"end_index"`
			} `avro:"files"`
		} `avro:"column_groups"`
		LOBFiles []struct {
			Path string `avro:"path"`
		} `avro:"lob_files"`
	}
	s.Require().NoError(decoder.Decode(&manifest))
	s.Require().False(decoder.HasNext(), "segment=%d has multiple manifest records", segmentID)
	s.Require().NoError(decoder.Error())
	s.Require().NotEmpty(manifest.ColumnGroups)
	s.Require().Empty(manifest.LOBFiles, "non-TEXT case unexpectedly produced LOB files")
	seen := make(map[string]struct{})
	for _, group := range manifest.ColumnGroups {
		s.Require().Equal("parquet", group.Format)
		s.Require().NotEmpty(group.Columns)
		s.Require().NotEmpty(group.Files)
		for _, file := range group.Files {
			s.Require().NotEmpty(file.Path)
			s.Require().Greater(file.End, file.Start)
			objectPath := file.Path
			if !path.IsAbs(objectPath) && !strings.HasPrefix(objectPath, basePath+"/") {
				objectPath = path.Join(basePath, "_data", objectPath)
			}
			s.Require().NotContains(seen, objectPath, "duplicate column-group object")
			seen[objectPath] = struct{}{}
			object, err := s.Cluster.ChunkManager.Read(ctx, objectPath)
			s.Require().NoError(err, "segment=%d object=%s", segmentID, objectPath)
			// V2 and V3 Parquet share the same physical encryption envelope;
			// only the manifest locator above is specific to Storage V3.
			s.Require().NoError(inspector.InspectRawDataV2(object, s.ezID, collectionID),
				"segment=%d columns=%v object=%s", segmentID, group.Columns, objectPath)
			s.T().Logf("stage=encrypted-object segment=%d columns=%v object=%s bytes=%d sha256=%x", segmentID, group.Columns, objectPath, len(object), sha256.Sum256(object))
		}
	}
}

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
	"debug/buildinfo"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/testutils"
	"github.com/milvus-io/milvus/tests/integration"
	"github.com/milvus-io/milvus/tests/integration/cmek/inspector"
	"github.com/milvus-io/milvus/tests/integration/cmek/testobserver"
)

type RawDataV3Suite struct {
	rawDataSuite
	keyBaselineDone   bool
	keyBaselineColumn string
	observerDir       string
	observerToken     string
}

const (
	parquetBaselineField       = "cmek_known_value"
	parquetBaselineValue int64 = 0x5a173d
)

func (s *RawDataV3Suite) SetupSuite() {
	build, err := buildinfo.ReadFile(filepath.Join(s.WorkDir(), "bin", "milvus"))
	s.Require().NoError(err)
	observerBuilt := false
	for _, setting := range build.Settings {
		if setting.Key == "-tags" {
			tags := strings.Split(setting.Value, ",")
			observerBuilt = slices.Contains(tags, "cmektest") || slices.Contains(tags, "test")
		}
	}
	s.Require().True(observerBuilt, "build the server with CMEK_TEST_OBSERVER=1 before running Storage V3 CMEK IT")
	s.observerDir, s.observerToken = s.T().TempDir(), funcutil.GenRandomStr()
	s.WithMilvusConfig(testobserver.DirectoryEnv, s.observerDir)
	s.WithMilvusConfig(testobserver.TokenEnv, s.observerToken)
	s.setupRawData(3)
}

func TestRawDataV3Suite(t *testing.T) {
	suite.Run(t, new(RawDataV3Suite))
}

// Exercise the non-TEXT canonical DataNode Parquet acceptance path.
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

func (s *RawDataV3Suite) TestParquetRawVector() {
	s.runParquetCampaign(newRawVectorCampaign())
}

func (s *RawDataV3Suite) TestParquetStructArray() {
	s.runParquetCampaign(newStructArrayCampaign())
}

func (s *RawDataV3Suite) runParquetCampaign(c rawDataCampaign) {
	s.keyBaselineDone = false
	// A constant pre-generated payload remains exactly checkable in any
	// physical file even when Milvus splits rows across segments or files.
	c.schema.Fields = append(c.schema.Fields, &schemapb.FieldSchema{Name: parquetBaselineField, DataType: schemapb.DataType_Int64})
	payload := testutils.NewInt64FieldData(parquetBaselineField, rawDataRows)
	for i := range payload.GetScalars().GetLongData().Data {
		payload.GetScalars().GetLongData().Data[i] = parquetBaselineValue
	}
	c.fields = append(c.fields, payload)
	c.loadFields = append(c.loadFields, parquetBaselineField)
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
	s.keyBaselineColumn = ""
	for _, field := range description.GetSchema().GetFields() {
		if field.GetName() == parquetBaselineField {
			s.keyBaselineColumn = strconv.FormatInt(field.GetFieldID(), 10)
		}
	}
	s.Require().NotEmpty(s.keyBaselineColumn)
	// The added regular field shifts StructArray IDs. Keep the deterministic
	// fixture's nested data aligned with the schema accepted by Milvus.
	for _, structure := range description.GetSchema().GetStructArrayFields() {
		for _, data := range c.fields {
			if data.GetFieldName() != structure.GetName() {
				continue
			}
			data.FieldId = structure.GetFieldID()
			ids := make(map[string]int64)
			for _, field := range structure.GetFields() {
				ids[field.GetName()] = field.GetFieldID()
			}
			for _, child := range data.GetStructArrays().GetFields() {
				id, ok := ids[child.GetFieldName()]
				s.Require().True(ok, "unknown StructArray child in fixture")
				child.FieldId = id
			}
		}
	}
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
	s.assertCanonicalParquetFlush(ctx, description.GetCollectionID(), description.GetVirtualChannelNames())
	segments := s.rawFlushedSegments(collection, segmentIDs)
	s.inspectParquetSegments(ctx, segments, description.GetCollectionID())
	s.Require().True(s.keyBaselineDone, "no nonempty Parquet object with known payload was verified in all key modes")
	s.readParquetCampaign(collection, description, c, loadFieldIDs)
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
	references, err := inspector.LocateManifestsV3(segments, collectionID)
	s.Require().NoError(err)
	for _, reference := range references {
		manifestPath := reference.Locator.ObjectPath()
		manifest, err := s.Cluster.ChunkManager.Read(ctx, manifestPath)
		s.Require().NoError(err, "segment=%d manifest=%s", reference.SegmentID, manifestPath)
		s.inspectParquetManifest(ctx, reference.Locator.BasePath, manifest, collectionID, reference.SegmentID)
		expected[reference.SegmentID] = reference.Identity
		rows += reference.Rows
		s.T().Logf("stage=manifest segment=%d rows=%d locator=%s bytes=%d", reference.SegmentID, reference.Rows, reference.Identity, len(manifest))
	}
	s.Require().Equal(int64(rawDataRows), rows)
	return expected
}

type v3LoadedIdentity struct {
	NodeID   int64
	Version  int64
	Manifest string
}

// Check the exact manifest independently of Loon, including structural-only
// metadata and every referenced non-TEXT Parquet object.
func (s *RawDataV3Suite) inspectParquetManifest(ctx context.Context, basePath string, raw []byte, collectionID, segmentID int64) {
	manifest, err := inspector.ParseManifestV3(raw)
	s.Require().NoError(err, "segment=%d", segmentID)
	objects, err := manifest.ParquetObjects(basePath)
	s.Require().NoError(err)
	for _, reference := range objects {
		object, err := s.Cluster.ChunkManager.Read(ctx, reference.Path)
		s.Require().NoError(err, "segment=%d object=%s", segmentID, reference.Path)
		// V2 and V3 share the physical Parquet encryption envelope, but not locators.
		s.Require().NoError(inspector.InspectRawDataV2(object, s.ezID, collectionID),
			"segment=%d columns=%v object=%s", segmentID, reference.Columns, reference.Path)
		s.T().Logf("stage=encrypted-object segment=%d columns=%v object=%s bytes=%d sha256=%x", segmentID, reference.Columns, reference.Path, len(object), sha256.Sum256(object))
		if !s.keyBaselineDone && slices.Contains(reference.Columns, s.keyBaselineColumn) {
			s.assertParquetKeyModes(object, reference.Rows, collectionID, reference.Path)
			s.keyBaselineDone = true
		}
	}
}

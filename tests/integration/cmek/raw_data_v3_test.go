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
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/testutils"
	"github.com/milvus-io/milvus/tests/integration/cmek/inspector"
)

type RawDataV3Suite struct {
	rawDataSuite
}

const (
	parquetBaselineField       = "cmek_known_value"
	parquetBaselineValue int64 = 0x5a173d
)

func (s *RawDataV3Suite) SetupSuite() {
	s.setupRawData(3)
}

func TestRawDataV3Suite(t *testing.T) {
	suite.Run(t, new(RawDataV3Suite))
}

func (s *RawDataV3Suite) TestParquetRawScalar() {
	c := newRawScalarCampaign()
	// A constant remains exactly checkable if rows split across files or segments.
	c.schema.Fields = append(c.schema.Fields, &schemapb.FieldSchema{Name: parquetBaselineField, DataType: schemapb.DataType_Int64})
	payload := testutils.NewInt64FieldData(parquetBaselineField, rawDataRows)
	for i := range payload.GetScalars().GetLongData().Data {
		payload.GetScalars().GetLongData().Data[i] = parquetBaselineValue
	}
	c.fields = append(c.fields, payload)
	c.loadFields = append(c.loadFields, parquetBaselineField)
	s.runParquetCampaign(c, parquetBaselineField)
}

func (s *RawDataV3Suite) TestParquetRawVector()   { s.runParquetCampaign(newRawVectorCampaign(), "") }
func (s *RawDataV3Suite) TestParquetStructArray() { s.runParquetCampaign(newStructArrayCampaign(), "") }

func (s *RawDataV3Suite) runParquetCampaign(c rawDataCampaign, baselineField string) {
	ctx, cancel := context.WithTimeout(s.Cluster.GetContext(), 3*time.Minute)
	defer cancel()
	description, segments := s.prepareRawDataCampaign(ctx, c)
	var baselineColumn string
	if baselineField != "" {
		ids := requestedFieldIDs(description.GetSchema(), []string{baselineField})
		s.Require().Len(ids, 1)
		baselineColumn = strconv.FormatInt(ids[0], 10)
	}
	expected, sample := s.inspectParquetSegments(ctx, segments, description.GetCollectionID(), baselineColumn)
	if baselineField != "" {
		s.Require().NotNil(sample, "no nonempty Parquet object contains the known payload")
		s.assertParquetKeyModes(*sample, description.GetCollectionID())
	}
	s.readParquetCampaign(description, c, segments, expected)
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

type parquetKeySample struct {
	object inspector.ParquetObjectV3
	raw    []byte
	edek   string
	column string
}

func (s *RawDataV3Suite) inspectParquetSegments(ctx context.Context, segments []*datapb.SegmentInfo, collectionID int64, baselineColumn string) (map[int64]inspector.ManifestLocatorV3, *parquetKeySample) {
	expected := make(map[int64]inspector.ManifestLocatorV3, len(segments))
	references, err := inspector.LocateManifestsV3(segments, collectionID)
	s.Require().NoError(err)
	var sample *parquetKeySample
	for _, reference := range references {
		manifest, err := s.Cluster.ChunkManager.Read(ctx, reference.Locator.ObjectPath())
		s.Require().NoError(err, "segment=%d manifest=%s", reference.SegmentID, reference.Locator.ObjectPath())
		objects, err := inspector.ParseParquetObjectsV3(manifest, reference.Locator.BasePath)
		s.Require().NoError(err, "segment=%d", reference.SegmentID)
		for _, object := range objects {
			raw, err := s.Cluster.ChunkManager.Read(ctx, object.Path)
			s.Require().NoError(err, "segment=%d object=%s", reference.SegmentID, object.Path)
			edek, err := inspector.InspectEncryptedParquet(raw, s.ezID, collectionID)
			s.Require().NoError(err, "segment=%d columns=%v object=%s", reference.SegmentID, object.Columns, object.Path)
			s.T().Logf("stage=encrypted-object segment=%d columns=%v object=%s bytes=%d sha256=%x", reference.SegmentID, object.Columns, object.Path, len(raw), sha256.Sum256(raw))
			if sample == nil && baselineColumn != "" && slices.Contains(object.Columns, baselineColumn) {
				sample = &parquetKeySample{object: object, raw: raw, edek: edek, column: baselineColumn}
			}
		}
		expected[reference.SegmentID] = reference.Locator
		s.T().Logf("stage=manifest segment=%d rows=%d locator=%+v objects=%d", reference.SegmentID, reference.Rows, reference.Locator, len(objects))
	}
	return expected, sample
}

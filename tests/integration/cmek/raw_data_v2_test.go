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
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/tests/integration/cmek/inspector"
)

type RawDataV2Suite struct {
	rawDataSuite
}

func (s *RawDataV2Suite) SetupSuite() {
	s.setupRawData(2)
}

func (s *RawDataV2Suite) TestRawScalar() {
	s.runRawDataCampaign(newRawScalarCampaign())
}

func (s *RawDataV2Suite) TestRawVector() {
	s.runRawDataCampaign(newRawVectorCampaign())
}

func (s *RawDataV2Suite) TestStructArray() {
	s.runRawDataCampaign(newStructArrayCampaign())
}

func TestRawDataV2Suite(t *testing.T) {
	suite.Run(t, new(RawDataV2Suite))
}

func (s *RawDataV2Suite) runRawDataCampaign(c rawDataCampaign) {
	ctx, cancel := context.WithTimeout(s.Cluster.GetContext(), 3*time.Minute)
	defer cancel()
	description, segments := s.prepareRawDataCampaign(ctx, c)
	collection, collectionID := description.GetCollectionName(), description.GetCollectionID()
	s.inspectRawObjects(ctx, segments, collectionID)
	if c.index {
		s.assertNoPhysicalVectorIndex(ctx, segments, description.GetSchema())
	}
	release, err := s.Cluster.MilvusClient.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{DbName: s.dbName, CollectionName: collection})
	s.Require().NoError(merr.CheckRPCCall(release, err))
	s.CheckCollectionCacheReleased(collectionID)
	load, err := s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{DbName: s.dbName, CollectionName: collection, ReplicaNumber: 1, LoadFields: c.loadFields})
	s.Require().NoError(merr.CheckRPCCall(load, err))
	s.WaitForLoadWithDB(ctx, s.dbName, collection)
	s.assertLoadedFields(ctx, collectionID, requestedFieldIDs(description.GetSchema(), c.loadFields))
	s.assertRawLoadedSegments(ctx, collectionID, segments)
	s.assertRawDataOracle(ctx, collection, c)
	if c.index {
		s.assertNoPhysicalVectorIndex(ctx, segments, description.GetSchema())
	}
}

func (s *RawDataV2Suite) inspectRawObjects(ctx context.Context, segments []*datapb.SegmentInfo, collectionID int64) {
	objects, err := inspector.LocateRawDataV2(s.Cluster.RootPath(), segments)
	s.Require().NoError(err)
	s.Require().NotEmpty(objects)
	reader := inspector.ObjectReader{ChunkManager: s.Cluster.ChunkManager}
	for _, object := range objects {
		raw, readErr := reader.Read(ctx, inspector.Object{Path: object.Path})
		s.Require().NoError(readErr, "collection=%d segment=%d field=%d path=%s storage_version=%d",
			object.CollectionID, object.SegmentID, object.FieldID, object.Path, object.StorageVersion)
		_, inspectErr := inspector.InspectEncryptedParquet(raw, s.ezID, collectionID)
		s.Require().NoError(inspectErr,
			"collection=%d segment=%d field=%d path=%s storage_version=%d",
			object.CollectionID, object.SegmentID, object.FieldID, object.Path, object.StorageVersion)
	}
}

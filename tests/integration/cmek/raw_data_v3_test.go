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
	"maps"
	"path"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/stretchr/testify/suite"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
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
	s.runRawDataCampaign(c, parquetBaselineField)
}

func (s *RawDataV3Suite) TestParquetRawVector()   { s.runRawDataCampaign(newRawVectorCampaign(), "") }
func (s *RawDataV3Suite) TestParquetStructArray() { s.runRawDataCampaign(newStructArrayCampaign(), "") }

func (s *RawDataV3Suite) runRawDataCampaign(c rawDataCampaign, baselineField string) {
	ctx, cancel := context.WithTimeout(s.Cluster.GetContext(), 3*time.Minute)
	defer cancel()
	description, segments := s.prepareRawDataCampaign(ctx, c)
	var baselineColumn string
	if baselineField != "" {
		ids := requestedFieldIDs(description.GetSchema(), []string{baselineField})
		s.Require().Len(ids, 1)
		baselineColumn = strconv.FormatInt(ids[0], 10)
	}
	expected, sample := s.inspectRawDataV3(ctx, segments, description.GetCollectionID(), baselineColumn)
	if baselineField != "" {
		s.Require().NotNil(sample, "no nonempty Parquet object contains the known payload")
		s.assertParquetKeyModes(*sample, description.GetCollectionID())
	}
	s.reloadRawDataV3(description, c, segments, expected)
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

func (s *RawDataV3Suite) inspectRawDataV3(ctx context.Context, segments []*datapb.SegmentInfo, collectionID int64, baselineColumn string) (map[int64]inspector.ManifestLocatorV3, *parquetKeySample) {
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

type v3LoadedIdentity struct {
	NodeID   int64
	Version  int64
	Manifest inspector.ManifestLocatorV3
}

type v3ReadSnapshot struct {
	Loaded        map[int64][]v3LoadedIdentity
	Serving       map[int64][]int64
	Growing       bool
	Unserviceable bool
}

func (snapshot v3ReadSnapshot) matches(expected, current map[int64]inspector.ManifestLocatorV3) bool {
	if snapshot.Growing || snapshot.Unserviceable || len(expected) == 0 || !maps.Equal(expected, current) || len(snapshot.Loaded) != len(expected) || len(snapshot.Serving) != len(expected) {
		return false
	}
	for segmentID, manifest := range expected {
		loaded, serving := snapshot.Loaded[segmentID], snapshot.Serving[segmentID]
		if len(loaded) != 1 || len(serving) != 1 || loaded[0].NodeID <= 0 || loaded[0].Version <= 0 ||
			loaded[0].Manifest != manifest || loaded[0].NodeID != serving[0] {
			return false
		}
	}
	return true
}

func (s *RawDataV3Suite) currentParquetSegments(ctx context.Context, collectionID int64) []*datapb.SegmentInfo {
	// A single linearizable prefix read observes the persisted DataCoord set;
	// unlike the legacy MetaWatcher helper, this honors the reading deadline.
	prefix := path.Join(s.Cluster.RootPath(), "meta/datacoord-meta/s", strconv.FormatInt(collectionID, 10)) + "/"
	response, err := s.Cluster.EtcdCli.Get(ctx, prefix, clientv3.WithPrefix())
	s.Require().NoError(err)
	var segments []*datapb.SegmentInfo
	for _, kv := range response.Kvs {
		segment := new(datapb.SegmentInfo)
		s.Require().NoError(proto.Unmarshal(kv.Value, segment))
		s.Require().Equal(collectionID, segment.GetCollectionID())
		if (segment.GetState() == commonpb.SegmentState_Sealed || segment.GetState() == commonpb.SegmentState_Flushed) &&
			segment.GetNumOfRows() > 0 && !segment.GetCompacted() && !segment.GetIsInvisible() {
			segments = append(segments, segment)
		}
	}
	return segments
}

func (s *RawDataV3Suite) manifestIdentity(segments []*datapb.SegmentInfo) map[int64]inspector.ManifestLocatorV3 {
	identity := make(map[int64]inspector.ManifestLocatorV3, len(segments))
	for _, segment := range segments {
		locator, err := inspector.ParseManifestLocatorV3(segment.GetManifestPath())
		s.Require().NoError(err)
		identity[segment.GetID()] = locator
	}
	return identity
}

func (s *RawDataV3Suite) parquetReadSnapshot(ctx context.Context, collectionID int64) v3ReadSnapshot {
	snapshot := v3ReadSnapshot{Loaded: make(map[int64][]v3LoadedIdentity), Serving: make(map[int64][]int64)}
	for _, client := range s.Cluster.GetAllStreamingAndQueryNodesClient() {
		response, err := client.GetDataDistribution(ctx, &querypb.GetDataDistributionRequest{LastUpdateTs: 0, SupportDelta: false})
		s.Require().NoError(merr.CheckRPCCall(response, err))
		s.Require().False(response.GetIsDelta())
		for _, segment := range response.GetSegments() {
			if segment.GetCollection() != collectionID {
				continue
			}
			locator, err := inspector.ParseManifestLocatorV3(segment.GetManifestPath())
			s.Require().NoError(err)
			snapshot.Loaded[segment.GetID()] = append(snapshot.Loaded[segment.GetID()], v3LoadedIdentity{response.GetNodeID(), segment.GetVersion(), locator})
		}
		for _, view := range response.GetLeaderViews() {
			if view.GetCollection() != collectionID {
				continue
			}
			snapshot.Unserviceable = snapshot.Unserviceable || !view.GetStatus().GetServiceable()
			snapshot.Growing = snapshot.Growing || len(view.GetGrowingSegmentIDs()) > 0 || len(view.GetGrowingSegments()) > 0
			for id, distribution := range view.GetSegmentDist() {
				snapshot.Serving[id] = append(snapshot.Serving[id], distribution.GetNodeID())
			}
		}
	}
	return snapshot
}

func (s *RawDataV3Suite) waitParquetReleased(ctx context.Context, collectionID int64) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	request, err := metricsinfo.ConstructRequestByMetricType(metricsinfo.SystemInfoMetrics)
	s.Require().NoError(err)
	for {
		released := true
		for _, client := range s.Cluster.GetAllStreamingAndQueryNodesClient() {
			response, err := client.GetMetrics(ctx, request)
			s.Require().NoError(merr.CheckRPCCall(response, err))
			var info metricsinfo.QueryNodeInfos
			s.Require().NoError(metricsinfo.UnmarshalComponentInfos(response.GetResponse(), &info))
			s.Require().NotNil(info.QuotaMetrics)
			for _, id := range info.QuotaMetrics.Effect.CollectionIDs {
				if id == collectionID {
					released = false
				}
			}
		}
		snapshot := s.parquetReadSnapshot(ctx, collectionID)
		if released && len(snapshot.Loaded) == 0 && len(snapshot.Serving) == 0 && !snapshot.Growing {
			return
		}
		select {
		case <-ctx.Done():
			s.T().Fatal(ctx.Err())
		case <-ticker.C:
		}
	}
}

func (s *RawDataV3Suite) reloadRawDataV3(description *milvuspb.DescribeCollectionResponse, c rawDataCampaign, segments []*datapb.SegmentInfo, expected map[int64]inspector.ManifestLocatorV3) {
	ctx, cancel := context.WithTimeout(s.Cluster.GetContext(), 3*time.Minute)
	defer cancel()
	collection, collectionID := description.GetCollectionName(), description.GetCollectionID()
	if c.index {
		s.assertNoPhysicalVectorIndex(ctx, segments, description.GetSchema())
	}
	release, err := s.Cluster.MilvusClient.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{DbName: s.dbName, CollectionName: collection})
	s.Require().NoError(merr.CheckRPCCall(release, err))
	s.waitParquetReleased(ctx, collectionID)
	load, err := s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{DbName: s.dbName, CollectionName: collection, ReplicaNumber: 1, LoadFields: c.loadFields})
	s.Require().NoError(merr.CheckRPCCall(load, err))
	s.waitForParquetLoad(ctx, collection)
	s.assertLoadedFields(ctx, collectionID, requestedFieldIDs(description.GetSchema(), c.loadFields))
	before := s.parquetReadSnapshot(ctx, collectionID)
	current := s.manifestIdentity(s.currentParquetSegments(ctx, collectionID))
	s.Require().True(before.matches(expected, current), "loaded data differs from inspected flush: expected=%v current=%v actual=%+v", expected, current, before)
	s.assertRawDataOracle(ctx, collection, c)
	after := s.parquetReadSnapshot(ctx, collectionID)
	s.Require().Equal(expected, s.manifestIdentity(s.currentParquetSegments(ctx, collectionID)), "flush manifests changed during read")
	s.Require().Equal(before, after, "loaded segments changed during read")
	if c.index {
		s.assertNoPhysicalVectorIndex(ctx, segments, description.GetSchema())
	}
	s.T().Logf("stage=cold-read complete loaded=%+v", after)
}

// The representative object comes from the independently inspected DataNode
// manifest. Read all row groups using the same entry point and immutable bytes
// in each key mode; never obtain the DEK from a production plugin or reader.
func (s *RawDataV3Suite) assertParquetKeyModes(sample parquetKeySample, collectionID int64) {
	raw, rows, objectPath := sample.raw, sample.object.Rows, sample.object.Path
	key := fixtureParquetKey(s.T(), sample.edek, s.ezID, collectionID)
	digest := sha256.Sum256(raw)
	table, err := inspector.ReadEncryptedParquet(raw, key)
	s.Require().NoError(err, "correct key must read the complete representative object")
	func() {
		defer table.Release()
		s.Require().Positive(rows)
		s.Require().Equal(rows, table.NumRows())
		columns := table.Schema().FieldIndices(sample.column)
		s.Require().Len(columns, 1, "representative object must contain the pre-generated payload")
		var count int64
		for _, chunk := range table.Column(columns[0]).Data().Chunks() {
			values, ok := chunk.(*array.Int64)
			s.Require().True(ok)
			s.Require().Zero(values.NullN())
			for _, value := range values.Int64Values() {
				s.Require().Equal(parquetBaselineValue, value)
				count++
			}
		}
		s.Require().Equal(rows, count)
	}()
	s.T().Logf("stage=format-key object=%s sha256=%x mode=correct rows=%d result=exact-read", objectPath, digest, rows)

	missingTable, err := inspector.ReadEncryptedParquet(raw, nil)
	if missingTable != nil {
		missingTable.Release()
	}
	s.Require().Nil(missingTable)
	s.Require().ErrorContains(err, "could not read encrypted metadata, no decryption found in reader's properties")
	s.T().Logf("stage=format-key object=%s sha256=%x mode=missing result=no-decryption-config", objectPath, digest)

	wrongKey := append([]byte(nil), key...)
	wrongKey[0] ^= 1
	wrongTable, err := inspector.ReadEncryptedParquet(raw, wrongKey)
	if wrongTable != nil {
		wrongTable.Release()
	}
	s.Require().Nil(wrongTable)
	s.Require().EqualError(err, "cipher: message authentication failed")
	s.Require().Equal(digest, sha256.Sum256(raw), "key modes must consume the same object bytes")
	s.T().Logf("stage=format-key object=%s sha256=%x mode=wrong result=authentication-failure", objectPath, digest)
}

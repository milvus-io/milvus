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
	"maps"
	"path"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type v3ReadSnapshot struct {
	Loaded        map[int64][]v3LoadedIdentity
	Serving       map[int64][]int64
	Growing       bool
	Unserviceable bool
}

func (snapshot v3ReadSnapshot) matches(candidate, current map[int64]string) bool {
	if snapshot.Growing || snapshot.Unserviceable || len(candidate) == 0 || !maps.Equal(candidate, current) || len(snapshot.Loaded) != len(candidate) || len(snapshot.Serving) != len(candidate) {
		return false
	}
	for segmentID, manifest := range candidate {
		loaded, serving := snapshot.Loaded[segmentID], snapshot.Serving[segmentID]
		if len(loaded) != 1 || len(serving) != 1 || loaded[0].NodeID <= 0 || loaded[0].Version <= 0 ||
			loaded[0].Manifest != manifest || loaded[0].NodeID != serving[0] {
			return false
		}
	}
	return true
}

// Only an observed identity change asks for another round. Operational errors
// and oracle failures are never retried. Every round shares the caller's deadline.
func runV3ReadRounds(ctx context.Context, attempt func(int) (bool, error)) error {
	for round := 1; round <= 3; round++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		stable, err := attempt(round)
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if stable {
			return nil
		}
	}
	return errors.New("no stable storage v3 read window after 3 complete rounds")
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

func manifestIdentity(segments []*datapb.SegmentInfo) map[int64]string {
	identity := make(map[int64]string, len(segments))
	for _, segment := range segments {
		identity[segment.GetID()] = segment.GetManifestPath()
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
			snapshot.Loaded[segment.GetID()] = append(snapshot.Loaded[segment.GetID()], v3LoadedIdentity{response.GetNodeID(), segment.GetVersion(), segment.GetManifestPath()})
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

func (s *RawDataV3Suite) assertParquetNoIndex(ctx context.Context, segments []*datapb.SegmentInfo, schema *schemapb.CollectionSchema) {
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

func (s *RawDataV3Suite) readParquetCampaign(collection string, description *milvuspb.DescribeCollectionResponse, c rawDataCampaign, fields []int64) {
	ctx, cancel := context.WithTimeout(s.Cluster.GetContext(), 3*time.Minute)
	defer cancel()
	collectionID := description.GetCollectionID()
	phase := "candidate inspection"
	var candidate map[int64]string
	var actual v3ReadSnapshot
	defer func() {
		if s.T().Failed() {
			s.T().Logf("stage=read-failed phase=%s candidate=%v actual=%+v context=%v", phase, candidate, actual, ctx.Err())
		}
	}()
	err := runV3ReadRounds(ctx, func(round int) (bool, error) {
		phase = "candidate inspection"
		segments := s.currentParquetSegments(ctx, collectionID)
		expected := s.inspectParquetSegments(ctx, segments, collectionID)
		candidate = expected
		if c.index {
			s.assertParquetNoIndex(ctx, segments, description.GetSchema())
		}
		phase = "release"
		release, err := s.Cluster.MilvusClient.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{DbName: s.dbName, CollectionName: collection})
		if err = merr.CheckRPCCall(release, err); err != nil {
			return false, err
		}
		s.waitParquetReleased(ctx, collectionID)
		phase = "post-release inspection"
		segments = s.currentParquetSegments(ctx, collectionID)
		if !maps.Equal(expected, manifestIdentity(segments)) {
			expected = s.inspectParquetSegments(ctx, segments, collectionID)
			candidate = expected
		}
		phase = "load"
		load, err := s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{DbName: s.dbName, CollectionName: collection, ReplicaNumber: 1, LoadFields: c.loadFields})
		if err = merr.CheckRPCCall(load, err); err != nil {
			return false, err
		}
		s.waitForParquetLoad(ctx, collection)
		s.assertLoadedFields(ctx, collectionID, fields)
		if c.index {
			s.assertParquetNoIndex(ctx, segments, description.GetSchema())
		}
		phase = "before oracle"
		before := s.parquetReadSnapshot(ctx, collectionID)
		actual = before
		current := manifestIdentity(s.currentParquetSegments(ctx, collectionID))
		if !before.matches(expected, current) {
			s.T().Logf("stage=read-round round=%d phase=before-oracle candidate=%v authoritative=%v actual=%+v action=reload", round, expected, current, before)
			return false, nil
		}
		phase = "oracle"
		s.assertRawDataOracle(ctx, collection, c.fields, c.loadFields)
		if c.search {
			s.assertExactFloatSearch(ctx, collection, "float_vector", firstFloatVector(c.fields, "float_vector", rawDataDim), rawDataRows)
		}
		phase = "after oracle"
		after := s.parquetReadSnapshot(ctx, collectionID)
		actual = after
		current = manifestIdentity(s.currentParquetSegments(ctx, collectionID))
		if !after.matches(expected, current) || !reflect.DeepEqual(before, after) {
			s.T().Logf("stage=read-round round=%d phase=after-oracle candidate=%v authoritative=%v before=%+v after=%+v action=reload", round, expected, current, before, after)
			return false, nil
		}
		if c.index {
			s.assertParquetNoIndex(ctx, segments, description.GetSchema())
		}
		s.T().Logf("stage=cold-read round=%d complete loaded=%+v", round, after)
		return true, nil
	})
	s.Require().NoError(err)
}

func TestV3ReadRoundsRejectChangedIdentityAndRespectBounds(t *testing.T) {
	for _, phase := range []string{"after load", "during oracle"} {
		changes := []string{"segment replacement", "manifest revision", "not serviceable"}
		if phase == "during oracle" {
			changes = append(changes, "load version", "serving node")
		}
		for _, change := range changes {
			t.Run(phase+"/"+change, func(t *testing.T) {
				rounds := 0
				err := runV3ReadRounds(context.Background(), func(round int) (bool, error) {
					rounds++
					candidate := map[int64]string{1: "manifest-7"}
					current := maps.Clone(candidate)
					before := v3ReadSnapshot{Loaded: map[int64][]v3LoadedIdentity{1: {{NodeID: 7, Version: 1, Manifest: "manifest-7"}}}, Serving: map[int64][]int64{1: {7}}}
					observed := before
					observed.Loaded = maps.Clone(before.Loaded)
					observed.Serving = maps.Clone(before.Serving)
					if round == 1 {
						switch change {
						case "segment replacement":
							current = map[int64]string{2: "replacement-manifest"}
							observed.Loaded = map[int64][]v3LoadedIdentity{2: {{NodeID: 7, Version: 1, Manifest: "replacement-manifest"}}}
							observed.Serving = map[int64][]int64{2: {7}}
						case "manifest revision":
							current[1] = "manifest-8"
							observed.Loaded[1] = []v3LoadedIdentity{{NodeID: 7, Version: 1, Manifest: "manifest-8"}}
						case "load version":
							observed.Loaded[1] = []v3LoadedIdentity{{NodeID: 7, Version: 2, Manifest: "manifest-7"}}
						case "serving node":
							observed.Loaded[1] = []v3LoadedIdentity{{NodeID: 8, Version: 1, Manifest: "manifest-7"}}
							observed.Serving[1] = []int64{8}
						case "not serviceable":
							observed.Unserviceable = true
						}
					}
					if phase == "after load" {
						return observed.matches(candidate, current), nil
					}
					require.True(t, before.matches(candidate, candidate))
					return observed.matches(candidate, current) && reflect.DeepEqual(before, observed), nil
				})
				require.NoError(t, err)
				require.Equal(t, 2, rounds)
			})
		}
	}
	count := 0
	err := runV3ReadRounds(context.Background(), func(int) (bool, error) { count++; return false, nil })
	require.ErrorContains(t, err, "3 complete rounds")
	require.Equal(t, 3, count)
	count = 0
	failure := errors.New("object authentication failed")
	err = runV3ReadRounds(context.Background(), func(int) (bool, error) { count++; return false, failure })
	require.ErrorIs(t, err, failure)
	require.Equal(t, 1, count)
	ctx, cancel := context.WithCancel(context.Background())
	count = 0
	err = runV3ReadRounds(ctx, func(int) (bool, error) { count++; cancel(); return false, nil })
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, count)
	ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	count = 0
	err = runV3ReadRounds(ctx, func(int) (bool, error) { count++; return true, nil })
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Zero(t, count)
}

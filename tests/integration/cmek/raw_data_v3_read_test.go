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
	"strconv"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/tests/integration/cmek/inspector"
)

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

func (s *RawDataV3Suite) readParquetCampaign(description *milvuspb.DescribeCollectionResponse, c rawDataCampaign, segments []*datapb.SegmentInfo, expected map[int64]inspector.ManifestLocatorV3) {
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

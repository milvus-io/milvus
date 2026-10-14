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

package indexparams

import (
	"context"
	"fmt"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/tests/integration"
)

const (
	vectorDim = 32
	indexRows = 4096 // Above both the 1024-row segment build threshold and PQ training minimum.
	indexName = "disk_index"
)

type ParameterOwnershipSuite struct {
	integration.MiniClusterSuite
}

func (s *ParameterOwnershipSuite) SetupSuite() {
	s.WithMilvusConfig("queryNode.enableDisk", "true")
	s.WithMilvusConfig("autoIndex.enable", "false")
	s.WithMilvusConfig("indexCoord.segment.minSegmentNumRowsToEnableIndex", "1024")
	s.WithMilvusConfig("dataCoord.enableCompaction", "false")
	s.MiniClusterSuite.SetupSuite()
}

func (s *ParameterOwnershipSuite) TestDiskParametersThroughAPI() {
	for _, algorithm := range []string{"DISKANN", "AISAQ"} {
		configs := []struct {
			name   string
			params map[string]string
		}{
			{name: "defaults"},
			{name: "degree32", params: map[string]string{
				"max_degree": "32", "search_list_size": "64", "pq_code_budget_gb_ratio": "0.125",
				"search_cache_budget_gb_ratio": "0", "num_build_thread": "1",
			}},
			{name: "degree48", params: map[string]string{
				"max_degree": "48", "search_list_size": "96", "pq_code_budget_gb_ratio": "0.25",
				"search_cache_budget_gb_ratio": "0.2", "num_build_thread": "1",
			}},
		}
		if algorithm == "AISAQ" {
			configs[1].params["disk_pq_code_budget_gb_ratio"] = "0"
			configs[1].params["inline_pq"] = "0"
			configs[1].params["rearrange"] = "false"
			configs[2].params["disk_pq_code_budget_gb_ratio"] = "0.25"
			configs[2].params["inline_pq"] = "0"
			configs[2].params["rearrange"] = "true"
		}
		for _, config := range configs {
			s.Run(algorithm+"_"+config.name, func() {
				s.exerciseIndex(algorithm, config.name, config.params)
			})
		}
	}
}

func (s *ParameterOwnershipSuite) exerciseIndex(algorithm, configName string, explicit map[string]string) {
	ctx, cancel := context.WithTimeout(s.Cluster.GetContext(), 3*time.Minute)
	defer cancel()
	cacheKey := "knowhere." + algorithm + ".load.search_cache_budget_gb_ratio"
	revertInitialCache := s.overrideKnowhereConfig(ctx, cacheKey, "0")
	defer revertInitialCache()
	collection := "index_params_" + algorithm + "_" + configName + "_" + funcutil.GenRandomStr()
	schemaBytes, err := proto.Marshal(integration.ConstructSchema(collection, vectorDim, true))
	s.Require().NoError(err)
	status, err := s.Cluster.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: collection, Schema: schemaBytes, ShardsNum: 1,
	})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		status, err := s.Cluster.MilvusClient.DropCollection(cleanupCtx, &milvuspb.DropCollectionRequest{CollectionName: collection})
		s.NoError(merr.CheckRPCCall(status, err))
	}()

	vectors := integration.NewFloatVectorFieldData(integration.FloatVecField, indexRows, vectorDim)
	insert, err := s.Cluster.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
		CollectionName: collection, FieldsData: []*schemapb.FieldData{vectors}, NumRows: indexRows,
	})
	s.Require().NoError(merr.CheckRPCCall(insert, err))
	flush, err := s.Cluster.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{CollectionNames: []string{collection}})
	s.Require().NoError(merr.CheckRPCCall(flush, err))
	segmentIDs := flush.GetFlushCollSegIDs()[collection].GetData()
	s.Require().NotEmpty(segmentIDs)
	s.WaitForFlush(ctx, segmentIDs, flush.GetCollFlushTs()[collection], "", collection)

	params := map[string]string{common.IndexTypeKey: algorithm, common.MetricTypeKey: metric.L2}
	for key, value := range explicit {
		params[key] = value
	}
	status, err = s.Cluster.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collection, FieldName: integration.FloatVecField,
		IndexName: indexName, ExtraParams: funcutil.Map2KeyValuePair(params),
	})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.WaitForIndexBuiltWithIndexName(ctx, collection, integration.FloatVecField, indexName)

	description, err := s.Cluster.MilvusClient.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{CollectionName: collection})
	s.Require().NoError(merr.CheckRPCCall(description, err))
	s.assertPersistedParams(ctx, collection, description.GetCollectionID(), params)
	s.assertIndexArtifacts(ctx, collection, description.GetCollectionID(), segmentIDs, algorithm)

	s.loadAndSearch(ctx, collection, description.GetCollectionID(), algorithm)
	status, err = s.Cluster.MilvusClient.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{CollectionName: collection})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.CheckCollectionCacheReleased(description.GetCollectionID())

	// These runtime load settings must never turn into explicit CreateIndex metadata.
	revert := s.overrideKnowhereConfig(ctx, cacheKey, "0.05")
	defer revert()
	s.loadAndSearch(ctx, collection, description.GetCollectionID(), algorithm)
	s.assertPersistedParams(ctx, collection, description.GetCollectionID(), params)
	status, err = s.Cluster.MilvusClient.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{CollectionName: collection})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.CheckCollectionCacheReleased(description.GetCollectionID())
}

func (s *ParameterOwnershipSuite) assertPersistedParams(ctx context.Context, collection string, collectionID int64, expected map[string]string) {
	public, err := s.Cluster.MilvusClient.DescribeIndex(ctx, &milvuspb.DescribeIndexRequest{CollectionName: collection, IndexName: indexName})
	s.Require().NoError(merr.CheckRPCCall(public, err))
	s.Require().Len(public.GetIndexDescriptions(), 1)
	s.Equal(expected, funcutil.KeyValuePair2Map(public.GetIndexDescriptions()[0].GetParams()))
	internal, err := s.Cluster.MixCoordClient.DescribeIndex(ctx, &indexpb.DescribeIndexRequest{CollectionID: collectionID, IndexName: indexName})
	s.Require().NoError(merr.CheckRPCCall(internal, err))
	s.Require().Len(internal.GetIndexInfos(), 1)
	stored := funcutil.KeyValuePair2Map(internal.GetIndexInfos()[0].GetIndexParams())
	s.Equal(expected, stored)
	s.NotContains(stored, "beamwidth")
}

func (s *ParameterOwnershipSuite) assertIndexArtifacts(ctx context.Context, collection string, collectionID int64, segmentIDs []int64, algorithm string) {
	stats, err := s.Cluster.MilvusClient.GetIndexStatistics(ctx, &milvuspb.GetIndexStatisticsRequest{CollectionName: collection, IndexName: indexName})
	s.Require().NoError(merr.CheckRPCCall(stats, err))
	s.Require().Len(stats.GetIndexDescriptions(), 1)
	s.Require().Equal(int64(indexRows), stats.GetIndexDescriptions()[0].GetIndexedRows())
	files, err := s.Cluster.MixCoordClient.GetIndexInfos(ctx, &indexpb.GetIndexInfoRequest{CollectionID: collectionID, SegmentIDs: segmentIDs})
	s.Require().NoError(merr.CheckRPCCall(files, err))
	for _, segmentID := range segmentIDs {
		segment := files.GetSegmentInfo()[segmentID]
		s.Require().NotNil(segment, "flushed segment %d", segmentID)
		s.Require().Len(segment.GetIndexInfos(), 1, "segment %d must have an actual built index", segmentID)
		index := segment.GetIndexInfos()[0]
		s.Require().Positive(index.GetBuildID())
		s.Require().NotEmpty(index.GetIndexFilePaths(), "small-segment brute force is not acceptable")
		s.Equal(algorithm, funcutil.KeyValuePair2Map(index.GetIndexParams())[common.IndexTypeKey])
	}
}

func (s *ParameterOwnershipSuite) loadAndSearch(ctx context.Context, collection string, collectionID int64, algorithm string) {
	status, err := s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{CollectionName: collection})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.WaitForLoad(ctx, collection)
	// No deployment search override exists here: the omitted beamwidth reaches
	// Knowhere's own default, and the valid explicit request is passed unchanged.
	for _, searchParams := range []map[string]any{{}, {"search_list": 32}, {"search_list": 32, "beamwidth": 4}} {
		s.assertSearch(ctx, collection, searchParams)
	}
	// Prove the runtime override reached the search consumer before asserting
	// that a valid default cannot hide an invalid explicit request.
	searchKey := "knowhere." + algorithm + ".search.beamwidth"
	revertInvalid := s.overrideKnowhereConfig(ctx, searchKey, "0")
	defer revertInvalid()
	s.Require().Eventually(func() bool {
		req := integration.ConstructSearchRequest("", collection, "", integration.FloatVecField,
			schemapb.DataType_FloatVector, nil, metric.L2, map[string]any{"search_list": 32}, 1, vectorDim, 5, -1)
		resp, err := s.Cluster.MilvusClient.Search(ctx, req)
		return err == nil && merr.Error(resp.GetStatus()) != nil && strings.Contains(resp.GetStatus().GetReason(), "beamwidth")
	}, 10*time.Second, 100*time.Millisecond, "deployment override must reach native search validation")
	revertValid := s.overrideKnowhereConfig(ctx, searchKey, "2")
	defer revertValid()
	s.Require().Eventually(func() bool {
		req := integration.ConstructSearchRequest("", collection, "", integration.FloatVecField,
			schemapb.DataType_FloatVector, nil, metric.L2, map[string]any{"search_list": 32}, 1, vectorDim, 5, -1)
		resp, err := s.Cluster.MilvusClient.Search(ctx, req)
		return merr.CheckRPCCall(resp, err) == nil
	}, 10*time.Second, 100*time.Millisecond, "valid deployment override must become effective")
	s.assertSearch(ctx, collection, map[string]any{"search_list": 32})
	s.assertSearch(ctx, collection, map[string]any{"search_list": 32, "beamwidth": 4})
	req := integration.ConstructSearchRequest("", collection, "", integration.FloatVecField,
		schemapb.DataType_FloatVector, nil, metric.L2, map[string]any{"search_list": 32, "beamwidth": 0}, 1, vectorDim, 5, -1)
	invalid, err := s.Cluster.MilvusClient.Search(ctx, req)
	s.Require().NoError(err)
	s.Require().Error(merr.Error(invalid.GetStatus()), "explicit invalid beamwidth must reach native validation")
	s.Contains(invalid.GetStatus().GetReason(), "beamwidth")
	// A successful search alone could hide a small-segment fallback; verify loaded disk indexes too.
	loaded, err := s.Cluster.MilvusClient.GetQuerySegmentInfo(ctx, &milvuspb.GetQuerySegmentInfoRequest{CollectionName: collection})
	s.Require().NoError(merr.CheckRPCCall(loaded, err))
	s.Require().NotEmpty(loaded.GetInfos())
	for _, segment := range loaded.GetInfos() {
		s.Equal(collectionID, segment.GetCollectionID())
		s.Require().Positive(segment.GetIndexID())
	}
}

func (s *ParameterOwnershipSuite) assertSearch(ctx context.Context, collection string, searchParams map[string]any) {
	req := integration.ConstructSearchRequest("", collection, "", integration.FloatVecField,
		schemapb.DataType_FloatVector, nil, metric.L2, searchParams, 1, vectorDim, 5, -1)
	resp, err := s.Cluster.MilvusClient.Search(ctx, req)
	s.Require().NoError(merr.CheckRPCCall(resp, err), fmt.Sprint(searchParams))
	s.Require().Equal([]int64{5}, resp.GetResults().GetTopks())
}

// Generic MiniCluster config helpers normalize names to environment spellings.
// Knowhere's open namespace deliberately keeps dots/case; write its original
// spelling to the existing etcd config source and restore it after the scenario.
func (s *ParameterOwnershipSuite) overrideKnowhereConfig(ctx context.Context, key, value string) func() {
	etcdKey := path.Join(s.Cluster.RootPath(), "config", key)
	previous, err := s.Cluster.EtcdCli.Get(ctx, etcdKey)
	s.Require().NoError(err)
	_, err = s.Cluster.EtcdCli.Put(ctx, etcdKey, value)
	s.Require().NoError(err)
	// MiniCluster's source refresh interval is 100ms; the search assertions
	// additionally wait for behavior to prove the override is in force.
	time.Sleep(500 * time.Millisecond)
	return func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if len(previous.Kvs) == 0 {
			_, err = s.Cluster.EtcdCli.Delete(cleanupCtx, etcdKey)
		} else {
			_, err = s.Cluster.EtcdCli.Put(cleanupCtx, etcdKey, string(previous.Kvs[0].Value))
		}
		s.NoError(err)
		time.Sleep(500 * time.Millisecond)
	}
}

func TestIndexParameterOwnership(t *testing.T) {
	suite.Run(t, new(ParameterOwnershipSuite))
}

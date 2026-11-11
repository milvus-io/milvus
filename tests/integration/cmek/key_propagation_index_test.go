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
	"encoding/json"
	"fmt"
	"maps"
	"path"
	"strconv"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/testutils"
	"github.com/milvus-io/milvus/tests/integration"
	"github.com/milvus-io/milvus/tests/integration/cmek/inspector"
)

type propagationIndexSuite struct {
	rawDataSuite
	fixture propagationFixture
	version int64
	kind    string
	fault   bool
}

func (s *propagationIndexSuite) SetupSuite() {
	s.fixture.dir = s.T().TempDir()
	s.WithMilvusConfig("MILVUS_CMEK_FIXTURE_CONTROL", s.fixture.dir)
	s.WithMilvusConfig("dataCoord.targetScalarIndexVersion", "3")
	s.WithMilvusConfig("dataCoord.forceRebuildScalarSegmentIndex", "true")
	s.WithMilvusConfig("dataCoord.targetVecIndexVersion", "8")
	s.WithMilvusConfig("dataCoord.forceRebuildSegmentIndex", "true")
	s.WithMilvusConfig("dataCoord.index.storePathVersion", "0")
	s.WithMilvusConfig("queryNode.segcore.tieredStorage.warmup.vectorIndex", "sync")
	s.WithMilvusConfig("queryNode.segcore.tieredStorage.warmup.scalarIndex", "sync")
	s.physicalIndexThreshold = 64
	s.setupRawData(s.version)
}

func (s *rawDataSuite) indexTasks(ctx context.Context, collectionID, indexID int64) map[int64]*indexpb.SegmentIndex {
	response, err := s.Cluster.EtcdCli.Get(ctx,
		path.Join(s.Cluster.RootPath(), "meta", "segment-index", strconv.FormatInt(collectionID, 10))+"/",
		clientv3.WithPrefix())
	s.Require().NoError(err)
	tasks := make(map[int64]*indexpb.SegmentIndex)
	for _, item := range response.Kvs {
		task := &indexpb.SegmentIndex{}
		s.Require().NoError(proto.Unmarshal(item.Value, task))
		if task.GetIndexID() == indexID && !task.GetDeleted() {
			s.Require().Equal(collectionID, task.GetCollectionID())
			tasks[task.GetBuildID()] = task
		}
	}
	return tasks
}

func (s *propagationIndexSuite) TestBuild() {
	defer s.fixture.restore(s.T())
	ctx, cancel := context.WithTimeout(s.Cluster.GetContext(), 7*time.Minute)
	defer cancel()
	campaign := consumerCampaign("propagation_" + s.kind)
	description := s.prepareRawDataCollection(ctx, campaign)
	collection, collectionID := description.GetCollectionName(), description.GetCollectionID()
	fields, expected := consumerBatch(0, 512, 1)
	s.insertRawDataBatch(ctx, description, fields, len(expected))
	segments := s.flushConsumerCollection(ctx, description)
	s.WaitForIndexBuiltWithDB(ctx, s.dbName, collection, "float_vector")
	field, indexType := "batch_id", "STL_SORT"
	params := []*commonpb.KeyValuePair{{Key: "index_type", Value: indexType}}
	if s.kind == "vector" {
		field, indexType = "float_vector", integration.IndexHNSW
		status, err := s.Cluster.MilvusClient.DropIndex(ctx, &milvuspb.DropIndexRequest{
			DbName: s.dbName, CollectionName: collection, IndexName: "raw_float_vector",
		})
		s.Require().NoError(merr.CheckRPCCall(status, err))
		params = integration.ConstructIndexParam(rawDataDim, indexType, metric.L2)
	}
	if s.kind == "default" {
		field = "default_value"
		encoded, err := proto.Marshal(&schemapb.FieldSchema{
			Name: field, DataType: schemapb.DataType_Int64, Nullable: true,
			DefaultValue: &schemapb.ValueField{Data: &schemapb.ValueField_LongData{LongData: 77}},
		})
		s.Require().NoError(err)
		status, err := s.Cluster.MilvusClient.AddCollectionField(ctx, &milvuspb.AddCollectionFieldRequest{
			DbName: s.dbName, CollectionName: collection, Schema: encoded,
		})
		s.Require().NoError(merr.CheckRPCCall(status, err))
		description, err = s.Cluster.MilvusClient.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
			DbName: s.dbName, CollectionName: collection,
		})
		s.Require().NoError(merr.CheckRPCCall(description, err))
		campaign.loadFields = append(campaign.loadFields, field)
	}
	fieldIDs := requestedFieldIDs(description.GetSchema(), []string{field})
	s.Require().Len(fieldIDs, 1)
	fieldID := fieldIDs[0]
	if s.version == 3 {
		locators, _ := s.inspectRawDataV3(ctx, segments, collectionID, "")
		if s.kind == "default" {
			for _, locator := range locators {
				raw, err := s.Cluster.ChunkManager.Read(ctx, locator.ObjectPath())
				s.Require().NoError(err)
				objects, err := inspector.ParseParquetObjectsV3(raw, locator.BasePath)
				s.Require().NoError(err)
				for _, object := range objects {
					s.Require().NotContains(object.Columns, strconv.FormatInt(fieldID, 10),
						"default-value build must have no source file for the new field")
				}
			}
		}
	} else {
		s.inspectRawObjects(ctx, segments, collectionID)
	}
	nodeID := s.coldDataNode(s.fixture)
	prefixes := []string{"index_files", fmt.Sprintf("insert_log/%d", collectionID)}
	before := s.objectSnapshot(ctx, prefixes...)
	s.Require().NotEmpty(before, "snapshot must include the existing raw input objects")
	fault := keyFaultControl{
		Role: "mixcoord", EZID: s.ezID, CollectionID: collectionID,
		Operation: "unsafe", Caller: "(*indexBuildTask).prepareJobRequest",
	}
	if s.fault {
		s.fixture.setFault(s.T(), fault)
	}
	status, err := s.Cluster.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		DbName: s.dbName, CollectionName: collection, FieldName: field, IndexName: "propagation_index", ExtraParams: params,
	})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	described, err := s.Cluster.MilvusClient.DescribeIndex(ctx, &milvuspb.DescribeIndexRequest{
		DbName: s.dbName, CollectionName: collection, IndexName: "propagation_index",
	})
	s.Require().NoError(merr.CheckRPCCall(described, err))
	s.Require().Len(described.GetIndexDescriptions(), 1)
	indexID := described.GetIndexDescriptions()[0].GetIndexID()
	var blocked map[int64]*indexpb.SegmentIndex
	if s.fault {
		s.fixture.waitForRetry(s.T(), fault, func() {
			s.Require().True(maps.Equal(before, s.objectSnapshot(ctx, prefixes...)),
				"writer-key failure produced target index files or changed source objects")
			blocked = s.indexTasks(ctx, collectionID, indexID)
			for _, task := range blocked {
				s.Require().NotEqual(commonpb.IndexState_Finished, task.GetState())
				s.Require().NotEqual(commonpb.IndexState_Failed, task.GetState())
				s.Require().Empty(task.GetIndexFileKeys())
			}
			s.fixture.assertNotRegistered(s.T(), nodeID, s.ezID)
		})
		s.Require().NotEmpty(blocked)
		s.fixture.restore(s.T())
	}
	s.WaitForIndexBuiltWithDB(ctx, s.dbName, collection, field)
	tasks := s.indexTasks(ctx, collectionID, indexID)
	s.Require().NotEmpty(tasks)
	if s.fault {
		s.Require().Equal(len(blocked), len(tasks))
		for buildID := range blocked {
			s.Require().Contains(tasks, buildID, "recovery replaced the original task")
		}
	}
	builds := make(map[int64]int64)
	segmentIDs := make([]int64, 0, len(segments))
	for _, segment := range segments {
		segmentIDs = append(segmentIDs, segment.GetID())
	}
	info, err := s.Cluster.MixCoordClient.GetIndexInfos(ctx, &indexpb.GetIndexInfoRequest{
		CollectionID: collectionID, SegmentIDs: segmentIDs,
	})
	s.Require().NoError(merr.CheckRPCCall(info, err))
	for _, task := range tasks {
		s.Require().Equal(commonpb.IndexState_Finished, task.GetState())
		s.Require().Equal(nodeID, task.GetNodeID())
		s.Require().NotEmpty(task.GetIndexFileKeys(), "fake-finished build is not propagation coverage")
		builds[task.GetSegmentID()] = task.GetBuildID()
		found := false
		for _, index := range info.GetSegmentInfo()[task.GetSegmentID()].GetIndexInfos() {
			if index.GetIndexID() != indexID {
				continue
			}
			found = true
			s.Require().Equal(task.GetBuildID(), index.GetBuildID())
			s.Require().NotEmpty(index.GetIndexFilePaths())
			for _, object := range index.GetIndexFilePaths() {
				raw, err := s.Cluster.ChunkManager.Read(ctx, object)
				s.Require().NoError(err)
				if s.kind == "vector" {
					s.Require().Equal(int32(8), index.GetCurrentIndexVersion())
					err = inspector.InspectIndexDataV2(raw, inspector.VectorIndexObject{
						CollectionID: collectionID, PartitionID: task.GetPartitionID(), SegmentID: task.GetSegmentID(),
						FieldID: fieldID, BuildID: task.GetBuildID(), EZID: s.ezID,
					})
				} else {
					s.Require().Equal(int32(3), index.GetCurrentScalarIndexVersion())
					err = inspector.InspectV3(raw, s.ezID)
				}
				s.Require().NoError(err, "build=%d object=%s", task.GetBuildID(), object)
			}
		}
		s.Require().True(found)
		s.T().Logf("stage=index-built kind=%s storage=%d node=%d task=%d segment=%d version=%d",
			s.kind, s.version, nodeID, task.GetBuildID(), task.GetSegmentID(), task.GetIndexVersion())
	}
	s.fixture.assertRegistration(s.T(), nodeID, s.ezID, collectionID)
	status, err = s.Cluster.MilvusClient.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{
		DbName: s.dbName, CollectionName: collection,
	})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.CheckCollectionCacheReleased(collectionID)
	status, err = s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName: s.dbName, CollectionName: collection, ReplicaNumber: 1, LoadFields: campaign.loadFields,
	})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.WaitForLoadWithDB(ctx, s.dbName, collection)
	s.waitLoadedHNSW(ctx, description, builds, fieldID, indexID)
	s.assertConsumerRows(ctx, collection, expected)
	if s.kind == "vector" {
		s.assertHNSWSearch(ctx, collection, 0)
		s.assertHNSWSearch(ctx, collection, 511)
	} else {
		value := 1
		if s.kind == "default" {
			value = 77
		}
		result, err := s.Cluster.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
			DbName: s.dbName, CollectionName: collection,
			Expr: fmt.Sprintf("%s == %d", field, value), OutputFields: []string{fixturePrimaryKey},
			ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
		})
		s.Require().NoError(merr.CheckRPCCall(result, err))
		s.Require().Len(result.GetFieldsData(), 1)
		s.Require().ElementsMatch(sequentialIDs(len(expected)), result.GetFieldsData()[0].GetScalars().GetLongData().GetData())
	}
}

type propagationJSONSuite struct {
	rawDataSuite
	fixture propagationFixture
	fault   bool
}

func (s *propagationJSONSuite) SetupSuite() {
	s.fixture.dir = s.T().TempDir()
	s.WithMilvusConfig("MILVUS_CMEK_FIXTURE_CONTROL", s.fixture.dir)
	s.configurePropagationCompaction()
	s.WithMilvusConfig("dataCoord.jsonShreddingRatioThreshold", "0.5")
	s.WithMilvusConfig("common.usingJSONShreddingForQuery", "true")
	// JSON columns and the shared-key Tantivy index inherit these two policies.
	s.WithMilvusConfig("queryNode.segcore.tieredStorage.warmup.scalarField", "sync")
	s.WithMilvusConfig("queryNode.segcore.tieredStorage.warmup.scalarIndex", "sync")
	s.setupRawData(2)
	s.Cluster.MustModifyMilvusConfig(map[string]string{"dataCoord.enableCompaction": "false"})
}

func (s *propagationJSONSuite) TestSharedKeys() {
	ctx, cancel := context.WithTimeout(s.Cluster.GetContext(), 7*time.Minute)
	defer cancel()
	defer s.fixture.restore(s.T())
	campaign := consumerCampaign("json_stats")
	campaign.schema.Fields = append(campaign.schema.Fields, &schemapb.FieldSchema{Name: "json", DataType: schemapb.DataType_JSON})
	description := s.prepareRawDataCollection(ctx, campaign)
	collection, collectionID := description.GetCollectionName(), description.GetCollectionID()
	fieldID := requestedFieldIDs(description.GetSchema(), []string{"json"})[0]
	fields, expected := consumerBatch(0, 128, 1)
	values := make([][]byte, 128)
	var matching []int64
	for i := range values {
		values[i] = []byte(fmt.Sprintf(`{"frequent":"private-value-%d"}`, i))
		if i%16 == 0 {
			values[i] = []byte(fmt.Sprintf(`{"frequent":"private-value-%d","rare":"private-rare-%d"}`, i, i))
			matching = append(matching, int64(i))
		}
	}
	fields = append(fields, testutils.NewJSONFieldDataWithValue("json", values))
	s.insertRawDataBatch(ctx, description, fields, len(expected))
	s.flushConsumerCollection(ctx, description)
	inputs := s.sortPropagationInputs(ctx, collection, 1)
	s.Require().Empty(inputs[0].GetJsonKeyStats())
	s.WaitForIndexBuiltWithDB(ctx, s.dbName, collection, "float_vector")
	s.inspectPropagationRaw(ctx, inputs, collectionID)
	nodeID := s.coldDataNode(s.fixture)
	tasks := s.runPropagationStats(ctx, s.fixture, collectionID, nodeID, indexpb.StatsSubJob_JsonKeyIndexJob, s.fault)
	outputs := s.propagationSegments(ctx, collection)
	s.Require().Len(outputs, 1)
	stats := outputs[0].GetJsonKeyStats()[fieldID]
	s.Require().NotNil(stats)
	s.Require().Contains(tasks, stats.GetBuildID())
	s.Require().Equal(tasks[stats.GetBuildID()].GetVersion(), stats.GetVersion())
	s.Require().NotEmpty(stats.GetFiles())
	basePath := metautil.BuildJSONKeyStatsPrefix(s.Cluster.RootPath(), stats.GetJsonKeyStatsDataFormat(),
		stats.GetBuildID(), stats.GetVersion(), collectionID, outputs[0].GetPartitionID(), outputs[0].GetID(), fieldID)
	files := metautil.BuildStatsFilePaths(basePath, stats.GetFiles())
	shared, metaCount := 0, 0
	for _, object := range files {
		raw, err := s.Cluster.ChunkManager.Read(ctx, object)
		s.Require().NoError(err, object)
		if strings.Contains(object, "/shared_key_index/") {
			shared++
			s.Require().NoError(inspector.InspectIndexDataV2(raw, inspector.VectorIndexObject{
				EZID: s.ezID, CollectionID: collectionID, PartitionID: outputs[0].GetPartitionID(), SegmentID: outputs[0].GetID(), FieldID: fieldID, BuildID: stats.GetBuildID(),
			}), object)
		}
		if strings.HasSuffix(object, "/meta.json") {
			metaCount++
			var meta struct {
				Rows   int64             `json:"num_rows"`
				Layout map[string]string `json:"layout_type_map"`
			}
			s.Require().NoError(json.Unmarshal(raw, &meta))
			s.Require().Equal(int64(len(expected)), meta.Rows)
			found := false
			for key, layout := range meta.Layout {
				if strings.Contains(key, "rare") {
					s.Require().Equal("SHARED", layout)
					found = true
				}
			}
			s.Require().True(found, "rare key is absent from JSON Stats layout")
			s.Require().NotContains(string(raw), "private-value-")
			s.Require().NotContains(string(raw), "private-rare-")
			var shape map[string]json.RawMessage
			s.Require().NoError(json.Unmarshal(raw, &shape))
			for key := range shape {
				s.Require().Contains([]string{"version", "layout_type_map", "num_rows", "num_shredding_columns"}, key, "unexpected metadata field")
			}
		}
	}
	s.Require().Positive(shared)
	s.Require().Equal(1, metaCount)
	s.T().Logf("stage=json-stats build=%d version=%d shared_slices=%d files=%v", stats.GetBuildID(), stats.GetVersion(), shared, files)
	s.reloadPropagation(ctx, description, outputs)
	s.Require().Eventually(func() bool {
		seen := 0
		for _, client := range s.Cluster.GetAllStreamingAndQueryNodesClient() {
			response, err := client.GetDataDistribution(ctx, &querypb.GetDataDistributionRequest{})
			s.Require().NoError(merr.CheckRPCCall(response, err))
			for _, segment := range response.GetSegments() {
				if segment.GetID() != outputs[0].GetID() {
					continue
				}
				loaded := segment.GetJsonStatsInfo()[fieldID]
				if loaded.GetBuildID() == stats.GetBuildID() && loaded.GetVersionID() == stats.GetVersion() {
					seen++
				}
			}
		}
		return seen == 1
	}, time.Minute, 100*time.Millisecond)
	s.assertPropagationQuery(ctx, collection, `exists json["rare"]`, matching)
	s.assertConsumerRows(ctx, collection, expected)
	// The SHARED BSON values in shredding_data remain outside CMEK support.
}

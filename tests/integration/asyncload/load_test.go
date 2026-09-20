// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package asyncload

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
	"github.com/milvus-io/milvus/tests/integration"
	"github.com/milvus-io/milvus/tests/integration/cmek/inspector"
)

const (
	rowsPerSegment = 2048
	segmentCount   = 2
	valueFieldID   = int64(101)
)

type loadSuite struct {
	integration.MiniClusterSuite
	async  bool
	faults *storageFaultProxy
}

func TestSyncLoad(t *testing.T) {
	suite.Run(t, &loadSuite{})
}

func TestAsyncLoad(t *testing.T) {
	suite.Run(t, &loadSuite{async: true})
}

// Each mode starts new processes: translators capture the switch at construction.
func (s *loadSuite) SetupSuite() {
	s.WithOptions(integration.WithoutResetDeploymentWhenTestTearDown())
	s.WithMilvusConfig("queryNode.segcore.storageV2.enableAsyncLoad", strconv.FormatBool(s.async))
	s.WithMilvusConfig("queryNode.segcore.storageV2.asyncLoadThreadPoolSize", "1")
	s.WithMilvusConfig("common.loadAdmissionSlots", "1")
	s.WithMilvusConfig("common.loadTransientBudgetBytes", "8388608")
	s.WithMilvusConfig("dataCoord.targetScalarIndexVersion", "2")
	s.WithMilvusConfig("dataCoord.forceRebuildScalarSegmentIndex", "true")
	// Flush-triggered sorting is required for JSON stats. Disable background
	// merging so the two independent segment artifacts remain available.
	s.WithMilvusConfig("dataCoord.enableCompaction", "true")
	s.WithMilvusConfig("dataCoord.compaction.enableAutoCompaction", "false")
	s.WithMilvusConfig("common.enabledJSONShredding", "true")
	s.WithMilvusConfig("common.usingJSONShreddingForQuery", "true")
	s.WithMilvusConfig("dataCoord.taskCheckInterval", "1")
	s.WithMilvusConfig("dataCoord.jsonShreddingWriteBatchSize", "256")
	// V2 segment metadata exposes exact stats object keys without a manifest.
	s.WithMilvusConfig("common.storage.useLoonFFI", "false")
	s.MiniClusterSuite.SetupSuite()
}

type loadFixture struct {
	name         string
	collectionID int64
	json         bool
	objects      []string
}

func (s *loadSuite) TestLegacyScalar() {
	s.checkReload(s.buildFixture(false))
}

func (s *loadSuite) TestJSONStats() {
	s.checkReload(s.buildFixture(true))
}

// Wait for real immutable artifacts and verify their format before loading.
func (s *loadSuite) buildFixture(jsonStats bool) loadFixture {
	ctx := s.Cluster.GetContext()
	f := loadFixture{name: "async_load_" + funcutil.GenRandomStr(), json: jsonStats}
	valueType := schemapb.DataType_Int64
	if jsonStats {
		valueType = schemapb.DataType_JSON
	}
	schema := &schemapb.CollectionSchema{
		Name: f.name,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
			{FieldID: valueFieldID, Name: "value", DataType: valueType, Nullable: !jsonStats},
			{FieldID: 102, Name: "vector", DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "4"}}},
		},
	}
	encoded, err := proto.Marshal(schema)
	s.Require().NoError(err)
	status, err := s.Cluster.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: f.name, Schema: encoded, ShardsNum: 1,
	})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.T().Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		status, err := s.Cluster.MilvusClient.DropCollection(cleanupCtx, &milvuspb.DropCollectionRequest{CollectionName: f.name})
		s.NoError(merr.CheckRPCCall(status, err))
	})
	desc, err := s.Cluster.MilvusClient.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{CollectionName: f.name})
	s.Require().NoError(merr.CheckRPCCall(desc, err))
	f.collectionID = desc.GetCollectionID()
	for batch := 0; batch < segmentCount; batch++ {
		start := batch * rowsPerSegment
		value := &schemapb.FieldData{FieldName: "value", Type: valueType}
		if jsonStats {
			data := make([][]byte, rowsPerSegment)
			for j := range data {
				i := start + j
				object := map[string]any{"city": fmt.Sprintf("city_%d", i%3)}
				if i%7 != 0 {
					if i%5 == 0 {
						object["age"] = nil
					} else {
						object["age"] = i
					}
				}
				data[j], err = json.Marshal(object)
				s.Require().NoError(err)
			}
			value.Field = &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{Data: data}},
			}}
		} else {
			valid := make([]bool, rowsPerSegment)
			data := make([]int64, 0, rowsPerSegment)
			for j := range valid {
				i := start + j
				valid[j] = i%5 != 0
				if valid[j] {
					data = append(data, int64(i))
				}
			}
			value.Field = &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: data}},
			}}
			typeutil.SetFieldDataValidData(value, valid)
		}
		insert, err := s.Cluster.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
			CollectionName: f.name, NumRows: rowsPerSegment,
			HashKeys: integration.GenerateHashKeys(rowsPerSegment),
			FieldsData: []*schemapb.FieldData{
				integration.NewInt64FieldDataWithStart("id", rowsPerSegment, int64(start)), value,
				integration.NewFloatVectorFieldData("vector", rowsPerSegment, 4),
			},
		})
		s.Require().NoError(merr.CheckRPCCall(insert, err))
		flush, err := s.Cluster.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{CollectionNames: []string{f.name}})
		s.Require().NoError(merr.CheckRPCCall(flush, err))
		s.WaitForFlush(ctx, flush.GetCollSegIDs()[f.name].GetData(), flush.GetCollFlushTs()[f.name], "", f.name)
	}
	index, err := s.Cluster.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: f.name, FieldName: "vector", ExtraParams: []*commonpb.KeyValuePair{
			{Key: common.IndexTypeKey, Value: "FLAT"}, {Key: common.MetricTypeKey, Value: "L2"},
		},
	})
	s.Require().NoError(merr.CheckRPCCall(index, err))
	s.WaitForIndexBuilt(ctx, f.name, "vector")
	if !jsonStats {
		index, err = s.Cluster.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
			CollectionName: f.name, FieldName: "value",
			ExtraParams: []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "STL_SORT"}},
		})
		s.Require().NoError(merr.CheckRPCCall(index, err))
		s.WaitForIndexBuilt(ctx, f.name, "value")
	}
	// Stats sorting replaces segments. Wait for final visible segments and their
	// finished artifacts, rather than relying only on collection index state.
	s.Require().Eventually(func() bool {
		segments, err := s.Cluster.ShowSegments(f.name)
		if err != nil {
			return false
		}
		sealed := make([]*datapb.SegmentInfo, 0, len(segments))
		var rows int64
		for _, segment := range segments {
			if segment.GetState() == commonpb.SegmentState_Flushed && !segment.GetCompacted() && !segment.GetIsInvisible() && segment.GetNumOfRows() > 0 {
				sealed = append(sealed, segment)
				rows += segment.GetNumOfRows()
			}
		}
		if len(sealed) < segmentCount || rows != rowsPerSegment*segmentCount {
			return false
		}
		f.objects = nil
		if !jsonStats {
			objects, err := inspector.LocateScalarIndex(ctx, s.Cluster.MixCoordClient, sealed, "STL_SORT", valueFieldID, 2)
			if err != nil {
				return false
			}
			for _, object := range objects {
				f.objects = append(f.objects, object.Path)
			}
		} else {
			for _, segment := range sealed {
				stats := segment.GetJsonKeyStats()[valueFieldID]
				if stats == nil || segment.GetStorageVersion() != storage.StorageV2 {
					return false
				}
				prefix := metautil.BuildJSONKeyStatsPrefix(s.Cluster.RootPath(), stats.GetJsonKeyStatsDataFormat(),
					stats.GetBuildID(), stats.GetVersion(), f.collectionID, segment.GetPartitionID(), segment.GetID(), valueFieldID)
				var meta, parquet bool
				for _, file := range stats.GetFiles() {
					meta = meta || path.Base(file) == "meta.json"
					parquet = parquet || strings.Contains(file, "shredding_data/")
					f.objects = append(f.objects, path.Join(prefix, file))
				}
				if !meta || !parquet {
					return false
				}
			}
		}
		return len(f.objects) > 0
	}, 3*time.Minute, 200*time.Millisecond, "final index/stats artifacts were not published")
	for _, object := range f.objects {
		data, err := s.Cluster.ChunkManager.Read(ctx, object)
		s.Require().NoError(err, object)
		s.Require().NotEmpty(data, object)
		if jsonStats && strings.Contains(object, "/shredding_data/") {
			s.Require().GreaterOrEqual(len(data), 12)
			s.Equal("PAR1", string(data[:4]), object)
			s.Equal("PAR1", string(data[len(data)-4:]), object)
		}
		if !jsonStats {
			reader, err := storage.NewBinlogReader(data)
			s.Require().NoError(err, "expected legacy IndexData envelope: %s", object)
			reader.Close()
		}
	}
	return f
}

func (s *loadSuite) checkReload(f loadFixture) {
	if s.faults != nil {
		s.checkStorageFaults(f)
		return
	}
	for pass := 0; pass < 2; pass++ {
		s.loadAndQuery(f)
		s.release(f)
	}
}

func (s *loadSuite) loadAndQuery(f loadFixture) {
	ctx := s.Cluster.GetContext()
	status, err := s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{CollectionName: f.name})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.WaitForLoad(ctx, f.name)
	field := "value"
	valid := func(i int) bool { return i%5 != 0 }
	if f.json {
		field = `value["age"]`
		valid = func(i int) bool { return i%5 != 0 && i%7 != 0 }
	}
	s.assertIDs(f, field+" >= 2000 and "+field+" < 2100", func(i int) bool { return valid(i) && i >= 2000 && i < 2100 })
	s.assertIDs(f, field+" is null", func(i int) bool { return !valid(i) })
	if f.json {
		// Milvus EXISTS treats both a missing path and explicit JSON null as absent.
		s.assertIDs(f, `not exists value["age"]`, func(i int) bool { return !valid(i) })
		s.assertIDs(f, `value["city"] == "city_1"`, func(i int) bool { return i%3 == 1 })
	}
}

func (s *loadSuite) assertIDs(f loadFixture, expression string, matches func(int) bool) {
	expected := make([]int64, 0)
	for i := 0; i < rowsPerSegment*segmentCount; i++ {
		if matches(i) {
			expected = append(expected, int64(i))
		}
	}
	result, err := s.Cluster.MilvusClient.Query(s.Cluster.GetContext(), &milvuspb.QueryRequest{
		CollectionName: f.name, Expr: expression, OutputFields: []string{"id"},
		ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
	})
	s.Require().NoError(merr.CheckRPCCall(result, err), expression)
	s.Require().Len(result.GetFieldsData(), 1)
	s.ElementsMatch(expected, result.GetFieldsData()[0].GetScalars().GetLongData().GetData(), expression)
}

func (s *loadSuite) release(f loadFixture) {
	status, err := s.Cluster.MilvusClient.ReleaseCollection(s.Cluster.GetContext(), &milvuspb.ReleaseCollectionRequest{CollectionName: f.name})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.CheckCollectionCacheReleased(f.collectionID)
}

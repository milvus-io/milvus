// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package featureusage

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/rgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/featureusage"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/util/function/embedding"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
	"github.com/milvus-io/milvus/tests/integration"
)

// The group tests cover the parts of the report that are not request counters:
// the static groups computed from metadata, the loaded group from QueryCoord,
// the QueryNode config group, node reachability, and sanitization.

// sentinel appears as the collection name, a field name, a property key and a
// property value. None of it may reach the report.
const sentinel = "fu_sentinel_do_not_leak"

// TestStaticGroupsAndSanitization creates one collection carrying a wide set of
// declared features and asserts both that they are reported and that no user
// string is.
func (s *Suite) TestStaticGroupsAndSanitization() {
	ctx := context.Background()
	name := sentinel + funcutil.GenRandomStr()
	dim := 8

	schema := &schemapb.CollectionSchema{
		Name:               name,
		Description:        sentinel,
		EnableDynamicField: true,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: true},
			{
				FieldID: 101, Name: sentinel + "_pkey", DataType: schemapb.DataType_VarChar, IsPartitionKey: true,
				TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "64"}},
			},
			{
				FieldID: 102, Name: "note", DataType: schemapb.DataType_VarChar, Nullable: true,
				TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "64"}},
			},
			{
				FieldID: 103, Name: "vec", DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: fmt.Sprint(dim)}},
			},
		},
		Properties: []*commonpb.KeyValuePair{
			{Key: common.MmapEnabledKey, Value: "true"},
			{Key: sentinel + ".key", Value: sentinel},
		},
	}
	marshaled, err := proto.Marshal(schema)
	s.Require().NoError(err)

	before := entries(s.report(ctx), typeutil.MixCoordRole, featureusage.GroupDeclared)
	status, err := s.Cluster.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: name,
		Schema:         marshaled,
		ShardsNum:      1,
		NumPartitions:  4,
		Properties:     schema.Properties,
	})
	s.Require().NoError(err)
	s.Require().Equal(commonpb.ErrorCode_Success, status.GetErrorCode(), status.GetReason())
	defer s.Cluster.MilvusClient.DropCollection(ctx, &milvuspb.DropCollectionRequest{CollectionName: name})

	report := s.report(ctx)
	declared := entries(report, typeutil.MixCoordRole, featureusage.GroupDeclared)
	s.Equal(before["is_partition_key"]+1, declared["is_partition_key"])
	s.Equal(before["enable_dynamic_field"]+1, declared["enable_dynamic_field"])
	s.Equal(before["nullable"]+1, declared["nullable"])
	s.Equal(before["auto_id"]+1, declared["auto_id"])

	props := entries(report, typeutil.MixCoordRole, featureusage.GroupProperties)
	s.GreaterOrEqual(props[common.MmapEnabledKey+"=true"], int64(1), "an official boolean key is reported per value")
	s.GreaterOrEqual(props[featureusage.CustomKey], int64(1), "the custom key is counted, not named")

	dist := entries(report, typeutil.MixCoordRole, featureusage.GroupDist)
	s.GreaterOrEqual(dist["partition_count|2-16"], int64(1))
	s.GreaterOrEqual(dist["dim|<=128"], int64(1))

	// Nothing the user chose may appear anywhere in the serialized report.
	s.NotContains(reportJSON(s.T(), report), sentinel)
}

// TestLoadedGroup asserts the group QueryCoord contributes: a collection loaded
// with a subset of its fields is distinguishable from a fully loaded one.
func (s *Suite) TestLoadedGroup() {
	ctx := context.Background()
	name := "fu_loaded_" + funcutil.GenRandomStr()
	dim := 8
	s.createSimpleCollection(ctx, name, dim)
	defer s.Cluster.MilvusClient.DropCollection(ctx, &milvuspb.DropCollectionRequest{CollectionName: name})

	before := entries(s.report(ctx), typeutil.MixCoordRole, featureusage.GroupLoaded)

	// "extra" is left out of the load, so the load is a strict subset.
	load, err := s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		CollectionName: name,
		LoadFields:     []string{"pk", "vec"},
	})
	s.Require().NoError(err)
	s.Require().Equal(commonpb.ErrorCode_Success, load.GetErrorCode(), load.GetReason())
	s.WaitForLoad(ctx, name)

	after := entries(s.report(ctx), typeutil.MixCoordRole, featureusage.GroupLoaded)
	s.Equal(before[featureusage.LoadedCollections]+1, after[featureusage.LoadedCollections])
	s.Equal(before[featureusage.LoadedFieldsSubset]+1, after[featureusage.LoadedFieldsSubset],
		"a load naming fewer fields than the schema is a partial load")

	dist := entries(s.report(ctx), typeutil.MixCoordRole, featureusage.GroupDist)
	s.GreaterOrEqual(dist[featureusage.DistLoadedReplicaNumber+"|1"], int64(1))

	release, err := s.Cluster.MilvusClient.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{CollectionName: name})
	s.Require().NoError(err)
	s.Require().Equal(commonpb.ErrorCode_Success, release.GetErrorCode())
}

// TestQueryNodeGroups asserts the node reports its configuration and that its
// execution-path counters move on a search that reaches unindexed data.
func (s *Suite) TestQueryNodeGroups() {
	ctx := context.Background()
	report := s.report(ctx)

	perNode := perNodeEntries(report, typeutil.QueryNodeRole, featureusage.GroupConfig)
	s.Require().NotEmpty(perNode, "a QueryNode reports its boolean configuration")
	for _, config := range perNode {
		s.Require().NotEmpty(config)
		for name, value := range config {
			s.EqualValues(1, value, name)
			s.True(strings.HasSuffix(name, "=true") || strings.HasSuffix(name, "=false"),
				"%s is not rendered as key=true/false", name)
		}
		s.Contains(config, "queryNode.enableDisk=false")
	}

	name := "fu_qn_" + funcutil.GenRandomStr()
	dim := 8
	s.createSimpleCollection(ctx, name, dim)
	defer s.Cluster.MilvusClient.DropCollection(ctx, &milvuspb.DropCollectionRequest{CollectionName: name})
	load, err := s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{CollectionName: name})
	s.Require().NoError(err)
	s.Require().Equal(commonpb.ErrorCode_Success, load.GetErrorCode(), load.GetReason())
	s.WaitForLoad(ctx, name)

	before := counters(s.report(ctx), typeutil.QueryNodeRole)

	// Rows that were never flushed live in a growing segment with no index, so
	// the search runs brute force.
	s.insertRows(ctx, name, dim, 100)
	req := integration.ConstructSearchRequest("", name, "", "vec",
		schemapb.DataType_FloatVector, nil, metric.L2, map[string]any{"nprobe": 4}, 1, dim, 5, -1)
	req.UseDefaultConsistency = false
	req.ConsistencyLevel = commonpb.ConsistencyLevel_Strong
	_, err = s.Cluster.MilvusClient.Search(ctx, req)
	s.Require().NoError(err)

	after := counters(s.report(ctx), typeutil.QueryNodeRole)
	s.Greater(after["brute_force_search"], before["brute_force_search"],
		"searching a segment with no index runs brute force")

	// A filtered pure-ANN search takes the two-stage branch; the suite lowers
	// the thresholds so a small collection reaches it.
	filtered := integration.ConstructSearchRequest("", name, "pk >= 0", "vec",
		schemapb.DataType_FloatVector, nil, metric.L2, map[string]any{"nprobe": 4}, 1, dim, 5, -1)
	filtered.UseDefaultConsistency = true
	_, err = s.Cluster.MilvusClient.Search(ctx, filtered)
	s.Require().NoError(err)
	s.Greater(counters(s.report(ctx), typeutil.QueryNodeRole)["two_stage_search"], after["two_stage_search"],
		"a filtered pure-ANN search takes the two-stage branch")

	// RunAnalyzer is the one user feature only a QueryNode serves.
	beforeAnalyzer := after["run_analyzer"]
	_, err = s.Cluster.MilvusClient.RunAnalyzer(ctx, &milvuspb.RunAnalyzerRequest{
		Placeholder:    [][]byte{[]byte("milvus vector database")},
		AnalyzerParams: `{"tokenizer": "standard"}`,
	})
	s.Require().NoError(err)
	s.Equal(beforeAnalyzer+1, counters(s.report(ctx), typeutil.QueryNodeRole)["run_analyzer"])
}

// TestUnreachableNodeIsReported kills a QueryNode without letting it deregister.
// The report must still list it, marked unreachable: an omitted node reads as
// "this feature is unused".
func (s *Suite) TestUnreachableNodeIsReported() {
	ctx := context.Background()
	qn := s.Cluster.AddQueryNode()
	nodeID := qn.GetNodeID()

	s.Require().Eventually(func() bool {
		for _, n := range s.reportRaw(ctx).GetNodes() {
			if n.GetNodeId() == nodeID {
				return true
			}
		}
		return false
	}, 60*timeUnit, timeUnit, "the new QueryNode should appear in the report")

	// ForceStop reports the kill signal it sent; that is the intent here.
	_ = qn.ForceStop()

	s.Require().Eventually(func() bool {
		for _, n := range s.reportRaw(ctx).GetNodes() {
			if n.GetNodeId() == nodeID && !n.GetReachable() {
				return n.GetError() != ""
			}
		}
		return false
	}, 120*timeUnit, timeUnit, "the killed QueryNode should be reported unreachable, not omitted")
}

// TestImportFileTypesAreCounted submits one import job per file format and
// asserts the DataCoord-side counter for that format moved. The counter is
// recorded where the job is accepted, so it does not wait for the import.
func (s *Suite) TestImportFileTypesAreCounted() {
	ctx := context.Background()
	name := "fu_import_" + funcutil.GenRandomStr()
	dim := 8
	schema := s.createSimpleCollection(ctx, name, dim)
	defer s.Cluster.MilvusClient.DropCollection(ctx, &milvuspb.DropCollectionRequest{CollectionName: name})

	for _, tc := range []struct {
		counter string
		write   func() string
	}{
		{"import_file_type=JSON", func() string { return s.writeJSONImportFile(ctx, schema, dim, "json") }},
		{"import_file_type=JSONLines", func() string { return s.writeJSONImportFile(ctx, schema, dim, "jsonl") }},
		{"import_file_type=CSV", func() string { return s.writeCSVImportFile(ctx, schema, dim) }},
	} {
		s.Run(tc.counter, func() {
			before := counters(s.report(ctx), typeutil.MixCoordRole)
			resp, err := s.Cluster.ProxyClient.ImportV2(ctx, &internalpb.ImportRequest{
				CollectionName: name,
				Files:          []*internalpb.ImportFile{{Paths: []string{tc.write()}}},
			})
			s.Require().NoError(err)
			s.Require().NoError(merr.Error(resp.GetStatus()))
			after := counters(s.report(ctx), typeutil.MixCoordRole)
			s.Equal(before[tc.counter]+1, after[tc.counter], "%s should have been counted", tc.counter)
		})
	}
}

// timeUnit keeps the Eventually calls readable.
const timeUnit = time.Second

// reportRaw is report without the all-nodes-reachable assertion, for the tests
// that are about reachability itself.
func (s *Suite) reportRaw(ctx context.Context) *internalpb.FeatureUsageReport {
	resp, err := s.Cluster.MixCoordClient.GetFeatureUsage(ctx, &internalpb.GetFeatureUsageRequest{})
	s.Require().NoError(err)
	s.Require().NoError(merr.Error(resp.GetStatus()))
	return resp
}

// createSimpleCollection creates a collection with a pk, one scalar and one
// vector field, inserts a flushed batch and builds the vector index. It does
// not load.
func (s *Suite) createSimpleCollection(ctx context.Context, name string, dim int) *schemapb.CollectionSchema {
	schema := &schemapb.CollectionSchema{
		Name: name,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: true},
			{
				FieldID: 101, Name: "extra", DataType: schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "64"}},
			},
			{
				FieldID: 102, Name: "vec", DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: fmt.Sprint(dim)}},
			},
		},
	}
	marshaled, err := proto.Marshal(schema)
	s.Require().NoError(err)
	status, err := s.Cluster.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: name,
		Schema:         marshaled,
		ShardsNum:      1,
	})
	s.Require().NoError(err)
	s.Require().Equal(commonpb.ErrorCode_Success, status.GetErrorCode(), status.GetReason())

	s.insertRows(ctx, name, dim, 200)
	flush, err := s.Cluster.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{CollectionNames: []string{name}})
	s.Require().NoError(err)
	s.WaitForFlush(ctx, flush.GetCollSegIDs()[name].GetData(), flush.GetCollFlushTs()[name], "", name)

	index, err := s.Cluster.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: name,
		FieldName:      "vec",
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(dim, integration.IndexFaissIvfFlat, metric.L2),
	})
	s.Require().NoError(err)
	s.Require().Equal(commonpb.ErrorCode_Success, index.GetErrorCode(), index.GetReason())
	s.WaitForIndexBuilt(ctx, name, "vec")
	return schema
}

func (s *Suite) insertRows(ctx context.Context, name string, dim, rowNum int) {
	values := make([]string, rowNum)
	for i := range values {
		values[i] = fmt.Sprintf("row-%d", i)
	}
	insert, err := s.Cluster.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
		CollectionName: name,
		FieldsData: []*schemapb.FieldData{
			newVarCharColumn("extra", values),
			integration.NewFloatVectorFieldData("vec", rowNum, dim),
		},
		HashKeys: integration.GenerateHashKeys(rowNum),
		NumRows:  uint32(rowNum),
	})
	s.Require().NoError(err)
	s.Require().Equal(commonpb.ErrorCode_Success, insert.GetStatus().GetErrorCode(), insert.GetStatus().GetReason())
}

// writeJSONImportFile writes rows as a JSON array (.json) or as one object per
// line (.jsonl); the file type is read from the extension.
func (s *Suite) writeJSONImportFile(ctx context.Context, schema *schemapb.CollectionSchema, dim int, ext string) string {
	rows := make([]map[string]any, 0, 10)
	for i := 0; i < 10; i++ {
		vec := make([]float32, dim)
		for d := range vec {
			vec[d] = float32(i) / float32(dim)
		}
		rows = append(rows, map[string]any{"extra": fmt.Sprintf("imported-%d", i), "vec": vec})
	}

	var body []byte
	var err error
	if ext == "jsonl" {
		var b strings.Builder
		for _, row := range rows {
			line, mErr := json.Marshal(row)
			s.Require().NoError(mErr)
			b.Write(line)
			b.WriteByte('\n')
		}
		body = []byte(b.String())
	} else {
		body, err = json.Marshal(map[string]any{"rows": rows})
		s.Require().NoError(err)
	}

	filePath := path.Join(s.Cluster.RootPath(), "fu_import", uuid.New().String()+"."+ext)
	s.Require().NoError(s.Cluster.ChunkManager.Write(ctx, filePath, body))
	return filePath
}

func (s *Suite) writeCSVImportFile(ctx context.Context, schema *schemapb.CollectionSchema, dim int) string {
	var b strings.Builder
	w := csv.NewWriter(&b)
	s.Require().NoError(w.Write([]string{"extra", "vec"}))
	for i := 0; i < 10; i++ {
		vec := make([]float32, dim)
		for d := range vec {
			vec[d] = float32(i) / float32(dim)
		}
		encoded, err := json.Marshal(vec)
		s.Require().NoError(err)
		s.Require().NoError(w.Write([]string{fmt.Sprintf("imported-%d", i), string(encoded)}))
	}
	w.Flush()
	s.Require().NoError(w.Error())

	filePath := path.Join(s.Cluster.RootPath(), "fu_import", uuid.New().String()+".csv")
	s.Require().NoError(s.Cluster.ChunkManager.Write(ctx, filePath, []byte(b.String())))
	return filePath
}

// TestProvidersAndCustomResourceGroup covers the two static entries a plain
// workload never reaches: the embedding provider of a text-embedding function,
// and a collection loaded into a resource group other than the default.
func (s *Suite) TestProvidersAndCustomResourceGroup() {
	ctx := context.Background()

	// --- providers: a TEI text-embedding function. The endpoint is a local
	// mock server; the report names the provider, never the endpoint.
	server := embedding.CreateTEIEmbeddingServer(8)
	defer server.Close()

	name := "fu_provider_" + funcutil.GenRandomStr()
	schema := &schemapb.CollectionSchema{
		Name: name,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: true},
			{
				FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "128"}},
			},
			{
				FieldID: 102, Name: "vec", DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "8"}},
			},
		},
		Functions: []*schemapb.FunctionSchema{{
			Name:             "embed",
			Type:             schemapb.FunctionType_TextEmbedding,
			InputFieldNames:  []string{"text"},
			OutputFieldNames: []string{"vec"},
			Params: []*commonpb.KeyValuePair{
				{Key: "provider", Value: "TEI"},
				{Key: "endpoint", Value: server.URL},
			},
		}},
	}
	marshaled, err := proto.Marshal(schema)
	s.Require().NoError(err)

	before := entries(s.report(ctx), typeutil.MixCoordRole, featureusage.GroupProviders)
	status, err := s.Cluster.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: name,
		Schema:         marshaled,
		ShardsNum:      1,
	})
	s.Require().NoError(err)
	s.Require().Equal(commonpb.ErrorCode_Success, status.GetErrorCode(), status.GetReason())
	// This collection is deliberately not dropped: the providers group is an
	// open-value group, so it disappears from the report when no collection has
	// a function, and the coverage check at the end of the suite requires it.

	report := s.report(ctx)
	after := entries(report, typeutil.MixCoordRole, featureusage.GroupProviders)
	s.Equal(before["tei"]+1, after["tei"], "the provider is reported, lowercased")
	s.NotContains(reportJSON(s.T(), report), server.URL, "the endpoint is a user string and never reported")

	funcs := entries(report, typeutil.MixCoordRole, featureusage.GroupFunctions)
	s.GreaterOrEqual(funcs[schemapb.FunctionType_TextEmbedding.String()], int64(1))

	// --- custom resource group: one extra QueryNode moved into a named group,
	// then a collection loaded into it.
	rgName := "fu_rg_" + funcutil.GenRandomStr()
	rgStatus, err := s.Cluster.MilvusClient.CreateResourceGroup(ctx, &milvuspb.CreateResourceGroupRequest{
		ResourceGroup: rgName,
		Config: &rgpb.ResourceGroupConfig{
			Requests:     &rgpb.ResourceGroupLimit{NodeNum: 1},
			Limits:       &rgpb.ResourceGroupLimit{NodeNum: 1},
			TransferFrom: []*rgpb.ResourceGroupTransfer{{ResourceGroup: meta.DefaultResourceGroupName}},
			TransferTo:   []*rgpb.ResourceGroupTransfer{{ResourceGroup: meta.DefaultResourceGroupName}},
		},
	})
	s.Require().NoError(err)
	s.Require().Equal(commonpb.ErrorCode_Success, rgStatus.GetErrorCode(), rgStatus.GetReason())
	defer s.Cluster.MilvusClient.DropResourceGroup(ctx, &milvuspb.DropResourceGroupRequest{ResourceGroup: rgName})

	qn := s.Cluster.AddQueryNode()

	loadedName := "fu_rgload_" + funcutil.GenRandomStr()
	s.createSimpleCollection(ctx, loadedName, 8)
	defer s.Cluster.MilvusClient.DropCollection(ctx, &milvuspb.DropCollectionRequest{CollectionName: loadedName})

	beforeLoaded := entries(s.report(ctx), typeutil.MixCoordRole, featureusage.GroupLoaded)
	load, err := s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		CollectionName: loadedName,
		ReplicaNumber:  1,
		ResourceGroups: []string{rgName},
	})
	s.Require().NoError(err)
	s.Require().Equal(commonpb.ErrorCode_Success, load.GetErrorCode(), load.GetReason())
	s.WaitForLoad(ctx, loadedName)

	afterReport := s.report(ctx)
	afterLoaded := entries(afterReport, typeutil.MixCoordRole, featureusage.GroupLoaded)
	s.Equal(beforeLoaded[featureusage.LoadedCustomResourceGroups]+1, afterLoaded[featureusage.LoadedCustomResourceGroups],
		"a replica outside the default resource group is counted")
	s.NotContains(reportJSON(s.T(), afterReport), rgName, "the resource group name never leaves the node")

	// Give the nodes back before leaving. A resource group that still holds the
	// only QueryNode makes every later load in this package fail with "resource
	// group node not enough", which reads as a bug in the test that follows.
	// A database carrying a property is what puts the db_properties group in
	// the report; like the provider collection, it is left in place.
	dbName := "fu_db_" + funcutil.GenRandomStr()
	dbStatus, err := s.Cluster.MilvusClient.CreateDatabase(ctx, &milvuspb.CreateDatabaseRequest{
		DbName: dbName,
		Properties: []*commonpb.KeyValuePair{
			{Key: common.DatabaseReplicaNumber, Value: "1"},
		},
	})
	s.Require().NoError(err)
	s.Require().Equal(commonpb.ErrorCode_Success, dbStatus.GetErrorCode(), dbStatus.GetReason())
	dbProps := entries(s.report(ctx), typeutil.MixCoordRole, featureusage.GroupDBProperties)
	s.GreaterOrEqual(dbProps[common.DatabaseReplicaNumber], int64(1), "a database property is reported")

	release, err := s.Cluster.MilvusClient.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{CollectionName: loadedName})
	s.Require().NoError(err)
	s.Require().Equal(commonpb.ErrorCode_Success, release.GetErrorCode())

	updated, err := s.Cluster.MilvusClient.UpdateResourceGroups(ctx, &milvuspb.UpdateResourceGroupsRequest{
		ResourceGroups: map[string]*rgpb.ResourceGroupConfig{
			rgName: {
				Requests:     &rgpb.ResourceGroupLimit{NodeNum: 0},
				Limits:       &rgpb.ResourceGroupLimit{NodeNum: 0},
				TransferFrom: []*rgpb.ResourceGroupTransfer{{ResourceGroup: meta.DefaultResourceGroupName}},
				TransferTo:   []*rgpb.ResourceGroupTransfer{{ResourceGroup: meta.DefaultResourceGroupName}},
			},
		},
	})
	s.Require().NoError(err)
	s.Require().Equal(commonpb.ErrorCode_Success, updated.GetErrorCode(), updated.GetReason())
	s.Require().Eventually(func() bool {
		desc, err := s.Cluster.MilvusClient.DescribeResourceGroup(ctx, &milvuspb.DescribeResourceGroupRequest{
			ResourceGroup: meta.DefaultResourceGroupName,
		})
		return err == nil && desc.GetResourceGroup().GetNumAvailableNode() > 0
	}, 60*timeUnit, timeUnit, "the default resource group should get its nodes back")

	qn.Stop()
}

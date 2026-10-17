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

package ddl

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/indexparams"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type indexParameterCapture struct {
	*MixCoordMock
	request *indexpb.CreateIndexRequest
}

func (c *indexParameterCapture) CreateIndex(_ context.Context, req *indexpb.CreateIndexRequest, _ ...grpc.CallOption) (*commonpb.Status, error) {
	c.request = proto.Clone(req).(*indexpb.CreateIndexRequest)
	return merr.Success(), nil
}

func TestCreateIndexParameterOwnershipMetadata(t *testing.T) {
	params := paramtable.Get()
	params.Save(params.AutoIndexConfig.Enable.Key, "false")
	params.Save(params.KnowhereConfig.Enable.Key, "true")
	defer params.Reset(params.AutoIndexConfig.Enable.Key)
	defer params.Reset(params.KnowhereConfig.Enable.Key)
	for _, indexType := range []string{"DISKANN", "AISAQ"} {
		t.Run(indexType, func(t *testing.T) {
			loadKey := "knowhere." + indexType + ".load.search_cache_budget_gb_ratio"
			searchKey := "knowhere." + indexType + ".search.search_list"
			params.Save(loadKey, "0.25")
			params.Save(searchKey, "20")
			defer params.Remove(loadKey)
			defer params.Remove(searchKey)
			for _, explicit := range []map[string]string{
				{},
				{"max_degree": "32", "search_list_size": "64", "pq_code_budget_gb_ratio": "0.125", "disk_pq_code_budget_gb_ratio": "0", "search_cache_budget_gb_ratio": "0", "use_bfs_cache": "false"},
				{"max_degree": "48", "search_list_size": "96", "pq_code_budget_gb_ratio": "0.25", "search_cache_budget_gb_ratio": "0.2", "use_bfs_cache": "true"},
			} {
				capture := &indexParameterCapture{MixCoordMock: NewMixCoordMock()}
				req := &milvuspb.CreateIndexRequest{ExtraParams: []*commonpb.KeyValuePair{
					{Key: common.IndexTypeKey, Value: indexType}, {Key: common.MetricTypeKey, Value: "L2"},
				}}
				if len(explicit) > 0 {
					raw, err := funcutil.MapToJSON(explicit)
					require.NoError(t, err)
					req.ExtraParams = append(req.ExtraParams, &commonpb.KeyValuePair{Key: common.ParamsKey, Value: raw})
				}
				task := &CreateIndexTask{
					req: req, mixCoord: capture, collectionID: 1,
					fieldSchema: &schemapb.FieldSchema{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "128"}}},
				}
				require.NoError(t, task.parseIndexParams(context.Background()))
				require.NoError(t, task.Execute(context.Background()))
				wire := model.MarshalIndexModel(&model.Index{CollectionID: 1, FieldID: 101, IndexID: 10, IndexParams: capture.request.IndexParams, TypeParams: capture.request.TypeParams, UserIndexParams: capture.request.UserIndexParams})
				serialized, err := proto.Marshal(wire)
				require.NoError(t, err)
				decoded := &indexpb.FieldIndex{}
				require.NoError(t, proto.Unmarshal(serialized, decoded))
				metadata := model.UnmarshalIndexModel(decoded)
				stored := funcutil.KeyValuePair2Map(metadata.IndexParams)
				for key, value := range explicit {
					require.Equal(t, value, stored[key], key)
				}
				require.NotContains(t, stored, "search_list")
				if len(explicit) == 0 {
					require.NotContains(t, stored, "max_degree")
					require.NotContains(t, stored, "pq_code_budget_gb_ratio")
					require.NotContains(t, stored, "search_cache_budget_gb_ratio")
				}
				// Each release/load starts again from persisted metadata. Live load
				// defaults must change while explicit API settings stay fixed.
				for _, value := range []string{"0.25", "0.5"} {
					params.Save(loadKey, value)
					loadParams := funcutil.KeyValuePair2Map(metadata.IndexParams)
					require.NoError(t, indexparams.AppendPrepareLoadParams(params, loadParams))
					want, ok := explicit["search_cache_budget_gb_ratio"]
					if !ok {
						want = value
					}
					require.Equal(t, want, loadParams["search_cache_budget_gb_ratio"])
				}
			}
		})
	}
}

func TestCreateIndexAutoIndexParameterOwnership(t *testing.T) {
	params := paramtable.Get()
	defer params.Reset(params.AutoIndexConfig.Enable.Key)
	defer params.Reset(params.AutoIndexConfig.IndexParams.Key)
	defer params.Reset(params.AutoIndexConfig.LoadAdaptParams.Key)
	for _, cloud := range []string{"false", "true"} {
		for _, algorithm := range []string{"DISKANN", "AISAQ"} {
			t.Run(algorithm+"/cloud="+cloud, func(t *testing.T) {
				selected := map[string]string{
					common.IndexTypeKey: algorithm, common.MetricTypeKey: "COSINE",
					"max_degree": "48", "search_list_size": "96", "pq_code_budget_gb_ratio": "0.125",
					"disk_pq_code_budget_gb_ratio": "0", "search_cache_budget_gb_ratio": "0",
				}
				if algorithm == "AISAQ" {
					selected["inline_pq"] = "-1"
					selected["rearrange"] = "false"
					selected["num_entry_points"] = "0"
				}
				raw, err := funcutil.MapToJSON(selected)
				require.NoError(t, err)
				require.NoError(t, params.Save(params.AutoIndexConfig.Enable.Key, cloud))
				require.NoError(t, params.Save(params.AutoIndexConfig.IndexParams.Key, raw))
				require.NoError(t, params.Save(params.AutoIndexConfig.LoadAdaptParams.Key, `{"search_cache_budget_gb_ratio":0.8,"rearrange":true}`))
				req := &milvuspb.CreateIndexRequest{ExtraParams: []*commonpb.KeyValuePair{
					{Key: common.IndexTypeKey, Value: AutoIndexName}, {Key: common.MetricTypeKey, Value: "L2"},
				}}
				if cloud == "true" {
					// Cloud AutoIndex intentionally selects an authoritative build
					// map, even if the caller names a different algorithm/config.
					req.ExtraParams[0].Value = "HNSW"
					req.ExtraParams = append(req.ExtraParams, &commonpb.KeyValuePair{Key: common.ParamsKey, Value: `{"max_degree":32}`})
				}
				capture := &indexParameterCapture{MixCoordMock: NewMixCoordMock()}
				task := &CreateIndexTask{
					req: req, mixCoord: capture, collectionID: 1,
					fieldSchema: &schemapb.FieldSchema{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "128"}}},
				}
				require.NoError(t, task.parseIndexParams(context.Background()))
				require.NoError(t, task.Execute(context.Background()))
				wire := model.MarshalIndexModel(&model.Index{CollectionID: 1, FieldID: 101, IndexID: 10, IndexParams: capture.request.IndexParams, TypeParams: capture.request.TypeParams, UserIndexParams: capture.request.UserIndexParams})
				serialized, err := proto.Marshal(wire)
				require.NoError(t, err)
				decoded := &indexpb.FieldIndex{}
				require.NoError(t, proto.Unmarshal(serialized, decoded))
				metadata := model.UnmarshalIndexModel(decoded)
				selected[common.MetricTypeKey] = "L2"
				require.Equal(t, selected, funcutil.KeyValuePair2Map(metadata.IndexParams))
				loadParams := funcutil.KeyValuePair2Map(metadata.IndexParams)
				require.NoError(t, indexparams.AppendPrepareLoadParams(params, loadParams))
				for key, value := range selected {
					require.Equal(t, value, loadParams[key], key)
				}
			})
		}
	}
}

func TestCreateIndexMinHashBuildDefaultsPersist(t *testing.T) {
	params := paramtable.Get()
	params.Save(params.AutoIndexConfig.Enable.Key, "false")
	defer params.Reset(params.AutoIndexConfig.Enable.Key)
	params.Save("knowhere.MINHASH_LSH.build.mh_element_bit_width", "32")
	defer params.Remove("knowhere.MINHASH_LSH.build.mh_element_bit_width")
	task := &CreateIndexTask{
		req:         &milvuspb.CreateIndexRequest{ExtraParams: []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "MINHASH_LSH"}, {Key: common.MetricTypeKey, Value: "MHJACCARD"}}},
		fieldSchema: &schemapb.FieldSchema{DataType: schemapb.DataType_BinaryVector, TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "128"}}},
	}
	require.NoError(t, task.parseIndexParams(context.Background()))
	require.Equal(t, "32", funcutil.KeyValuePair2Map(task.newIndexParams)["mh_element_bit_width"])
}

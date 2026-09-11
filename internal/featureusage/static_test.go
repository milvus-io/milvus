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

package featureusage

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

// sentinel is a string that stands in for every user-controlled value. It
// must never appear in the serialized report.
const sentinel = "USER_CONTROLLED_SENTINEL_9f3a"

func kv(k, v string) *commonpb.KeyValuePair { return &commonpb.KeyValuePair{Key: k, Value: v} }

func index(entries []*internalpb.FeatureEntry) map[string]*internalpb.FeatureEntry {
	m := make(map[string]*internalpb.FeatureEntry, len(entries))
	for _, e := range entries {
		key := e.Name
		if e.Bucket != "" {
			key += "|" + e.Bucket
		}
		m[key] = e
	}
	return m
}

func indexGroup(entries []*internalpb.FeatureEntry, group string) map[string]*internalpb.FeatureEntry {
	m := make(map[string]*internalpb.FeatureEntry)
	for _, e := range entries {
		if e.Group != group {
			continue
		}
		key := e.Name
		if e.Bucket != "" {
			key += "|" + e.Bucket
		}
		m[key] = e
	}
	return m
}

func vectorField(id int64, dt schemapb.DataType, dim string) *model.Field {
	return &model.Field{FieldID: id, Name: sentinel, DataType: dt, TypeParams: []*commonpb.KeyValuePair{kv(common.DimKey, dim)}}
}

func sampleCollection() *model.Collection {
	return &model.Collection{
		CollectionID: 1, Name: sentinel, Description: sentinel, DBName: sentinel,
		ShardsNum:        2,
		Partitions:       []*model.Partition{{PartitionID: 1}, {PartitionID: 2}, {PartitionID: 3}},
		ConsistencyLevel: commonpb.ConsistencyLevel_Bounded,
		Fields: []*model.Field{
			{FieldID: 100, Name: sentinel, DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: true},
			{
				FieldID: 101, Name: sentinel, DataType: schemapb.DataType_VarChar, IsPartitionKey: true, Nullable: true,
				TypeParams: []*commonpb.KeyValuePair{kv(common.MaxLengthKey, "512"), kv(common.EnableAnalyzerKey, "true"), kv(sentinel, sentinel)},
			},
			{FieldID: 102, Name: sentinel, DataType: schemapb.DataType_JSON, IsDynamic: true},
			{
				FieldID: 103, Name: sentinel, DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int32,
				TypeParams: []*commonpb.KeyValuePair{kv(common.MaxCapacityKey, "2000")}, DefaultValue: &schemapb.ValueField{},
			},
			vectorField(104, schemapb.DataType_FloatVector, "768"),
			vectorField(105, schemapb.DataType_SparseFloatVector, ""),
		},
		StructArrayFields: []*model.StructArrayField{{
			FieldID: 200, Name: sentinel,
			Fields: []*model.Field{vectorField(201, schemapb.DataType_ArrayOfVector, "4096")},
		}},
		Functions: []*model.Function{
			{Name: sentinel, Type: schemapb.FunctionType_BM25},
			{Name: sentinel, Type: schemapb.FunctionType_TextEmbedding, Params: []*commonpb.KeyValuePair{
				kv("provider", "OpenAI"), kv("model_name", sentinel), kv("credential", sentinel),
			}},
			{Name: sentinel, Type: schemapb.FunctionType_Rerank, Params: []*commonpb.KeyValuePair{
				kv("reranker", "model"), kv("provider", sentinel), kv("endpoint", sentinel),
			}},
		},
		Properties: []*commonpb.KeyValuePair{
			kv(common.CollectionTTLConfigKey, "86400"),
			kv(common.MmapEnabledKey, "False"),
			kv(common.CollectionAutoCompactionKey, "true"),
			kv(common.CollectionReplicaNumber, "3"),
			kv(common.EncryptionEnabledKey, "true"),
			kv(common.EncryptionRootKeyKey, sentinel),
			kv(common.MaxFieldIDKey, "105"),
			kv(sentinel, sentinel),
			kv("another."+sentinel, "true"),
		},
	}
}

func TestComputeCollectionEntries(t *testing.T) {
	col := sampleCollection()
	plain := &model.Collection{
		CollectionID: 2, Name: sentinel, ShardsNum: 1, ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
		Partitions: []*model.Partition{{PartitionID: 1}},
		Fields: []*model.Field{
			{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			vectorField(101, schemapb.DataType_FloatVector, "128"),
		},
		// Declared through the property path only, after AlterCollection.
		Properties: []*commonpb.KeyValuePair{
			kv(common.EnableDynamicSchemaKey, "true"),
			kv(common.NamespaceShardingEnabledKey, "true"),
		},
	}
	in := CollectionInput{
		Databases: []*model.Database{
			{Name: "default"},
			{Name: sentinel, Properties: []*commonpb.KeyValuePair{kv(common.DatabaseReplicaNumber, "2"), kv(common.DatabaseForceDenyWritingKey, "true"), kv(sentinel, "x")}},
		},
		Collections:         []*model.Collection{col, plain},
		AliasCount:          4,
		CustomRoleCount:     2,
		GrantCount:          9,
		PrivilegeGroupCount: 1,
	}
	entries := ComputeCollectionEntries(in)

	// Enum walk: every DataType except None is present, even at zero.
	ft := indexGroup(entries, GroupFieldTypes)
	for v, name := range schemapb.DataType_name {
		if v == 0 {
			assert.NotContains(t, ft, name)
			continue
		}
		require.Contains(t, ft, name, "enum value %s must be emitted", name)
	}
	assert.EqualValues(t, 2, ft["Int64"].Value)
	assert.EqualValues(t, 2, ft["FloatVector"].Value)
	assert.EqualValues(t, 1, ft["VarChar"].Value)
	assert.EqualValues(t, 1, ft["ArrayOfVector"].Value, "struct sub-fields are counted")
	assert.EqualValues(t, 0, ft["JSON"].Value, "the dynamic $meta field is not counted as JSON")
	assert.EqualValues(t, 0, ft["Geometry"].Value)

	fn := indexGroup(entries, GroupFunctions)
	assert.EqualValues(t, 1, fn["BM25"].Value)
	assert.EqualValues(t, 1, fn["TextEmbedding"].Value)
	assert.EqualValues(t, 1, fn["Rerank"].Value)
	assert.NotContains(t, fn, "Unknown")

	pv := indexGroup(entries, GroupProviders)
	assert.EqualValues(t, 1, pv["openai"].Value, "provider is lowercased and recognized")
	assert.EqualValues(t, 1, pv[RerankProviderPrefix+OtherValue].Value, "unrecognized rerank provider folds to _other")
	assert.Len(t, pv, 2)

	dc := indexGroup(entries, GroupDeclared)
	assert.EqualValues(t, 1, dc[DeclaredPartitionKey].Value)
	assert.EqualValues(t, 0, dc[DeclaredClusteringKey].Value, "predicates are present at zero")
	assert.EqualValues(t, 1, dc[DeclaredEnableDynamicField].Value, "declared via property only")
	assert.EqualValues(t, 1, dc[DeclaredEnableNamespace].Value, "declared via property only")
	assert.EqualValues(t, 1, dc[DeclaredNullable].Value)
	assert.EqualValues(t, 1, dc[DeclaredDefaultValue].Value)
	assert.EqualValues(t, 1, dc[DeclaredAutoID].Value)
	assert.EqualValues(t, 1, dc[DeclaredMultiVectorField].Value)
	assert.EqualValues(t, 1, dc[DeclaredStructArrayFields].Value)
	assert.EqualValues(t, 1, dc["consistency_level=Bounded"].Value)
	assert.EqualValues(t, 1, dc["consistency_level=Strong"].Value)

	pr := indexGroup(entries, GroupProperties)
	assert.EqualValues(t, 1, pr[common.CollectionTTLConfigKey].Value, "non-boolean value: key only")
	assert.EqualValues(t, 1, pr[common.MmapEnabledKey+"=false"].Value, "boolean split, case-insensitive")
	assert.EqualValues(t, 1, pr[common.CollectionAutoCompactionKey+"=true"].Value)
	assert.EqualValues(t, 1, pr[common.EncryptionEnabledKey+"=true"].Value)
	assert.EqualValues(t, 1, pr[common.EncryptionRootKeyKey].Value, "key named, value never")
	assert.EqualValues(t, 1, pr[CustomKey].Value, "two custom keys on one collection count once")
	assert.EqualValues(t, 1, pr[common.EnableDynamicSchemaKey+"=true"].Value)
	assert.NotContains(t, pr, common.MmapEnabledKey)
	assert.NotContains(t, pr, common.MaxFieldIDKey, "server-managed key is not reported")
	assert.NotContains(t, pr, common.MmapEnabledKey+"=true")

	dbp := indexGroup(entries, GroupDBProperties)
	assert.EqualValues(t, 1, dbp[common.DatabaseReplicaNumber].Value)
	assert.EqualValues(t, 1, dbp[common.DatabaseForceDenyWritingKey+"=true"].Value)
	assert.EqualValues(t, 1, dbp[CustomKey].Value)

	fp := indexGroup(entries, GroupFieldParams)
	assert.EqualValues(t, 2, fp[common.DimKey].Value)
	assert.EqualValues(t, 1, fp[common.MaxLengthKey].Value)
	assert.EqualValues(t, 1, fp[common.EnableAnalyzerKey+"=true"].Value)
	assert.EqualValues(t, 1, fp[CustomKey].Value)

	ob := indexGroup(entries, GroupObjects)
	assert.EqualValues(t, 1, ob[ObjectDatabases].Value, "default database is not counted")
	assert.EqualValues(t, 4, ob[ObjectAliases].Value)
	assert.EqualValues(t, 2, ob[ObjectCustomRoles].Value)
	assert.EqualValues(t, 9, ob[ObjectGrants].Value)
	assert.EqualValues(t, 1, ob[ObjectPrivilegeGroups].Value)

	ds := indexGroup(entries, GroupDist)
	assert.EqualValues(t, 1, ds[DistPartitionCount+"|2-16"].Value)
	assert.EqualValues(t, 1, ds[DistPartitionCount+"|1"].Value)
	assert.EqualValues(t, 1, ds[DistShardsNum+"|2"].Value)
	assert.EqualValues(t, 1, ds[DistShardsNum+"|1"].Value)
	assert.EqualValues(t, 1, ds[DistDim+"|>2048"].Value, "max dim over the collection, struct sub-field included")
	assert.EqualValues(t, 1, ds[DistDim+"|<=128"].Value)
	assert.EqualValues(t, 1, ds[DistMaxLength+"|257-4096"].Value)
	assert.EqualValues(t, 1, ds[DistMaxCapacity+"|>1024"].Value)
	assert.EqualValues(t, 1, ds[DistReplicaNumber+"|3+"].Value)
	assert.EqualValues(t, 1, ds[DistReplicaNumber+"|1"].Value, "replica defaults to 1")

	// Deterministic order.
	again := ComputeCollectionEntries(in)
	require.Equal(t, len(entries), len(again))
	for i := range entries {
		assert.Equal(t, entries[i].String(), again[i].String())
	}
}

func TestComputeCollectionEntriesEmpty(t *testing.T) {
	entries := ComputeCollectionEntries(CollectionInput{})
	ft := indexGroup(entries, GroupFieldTypes)
	assert.Len(t, ft, len(schemapb.DataType_name)-1)
	for _, e := range ft {
		assert.Zero(t, e.Value)
	}
	assert.Len(t, indexGroup(entries, GroupFunctions), len(schemapb.FunctionType_name)-1)
	assert.Empty(t, indexGroup(entries, GroupProviders), "open-value groups emit nothing when empty")
	assert.Empty(t, indexGroup(entries, GroupProperties))
	assert.Empty(t, indexGroup(entries, GroupDist))
	ob := indexGroup(entries, GroupObjects)
	assert.Len(t, ob, 5)
	for _, e := range ob {
		assert.Zero(t, e.Value)
	}
}

func TestBucketBoundaries(t *testing.T) {
	cases := []struct {
		spec bucketSpec
		v    int64
		want string
	}{
		{partitionCountBuckets, 0, "1"},
		{partitionCountBuckets, 1, "1"},
		{partitionCountBuckets, 2, "2-16"},
		{partitionCountBuckets, 16, "2-16"},
		{partitionCountBuckets, 17, "17-64"},
		{partitionCountBuckets, 1024, "65-1024"},
		{partitionCountBuckets, 1025, ">1024"},
		{shardsNumBuckets, 1, "1"},
		{shardsNumBuckets, 2, "2"},
		{shardsNumBuckets, 3, "3-8"},
		{shardsNumBuckets, 9, ">8"},
		{dimBuckets, 128, "<=128"},
		{dimBuckets, 129, "129-512"},
		{dimBuckets, 2048, "1025-2048"},
		{dimBuckets, 2049, ">2048"},
		{maxLengthBuckets, 256, "<=256"},
		{maxLengthBuckets, 65535, "4097-65535"},
		{maxLengthBuckets, 65536, ">65535"},
		{maxCapacityBuckets, 64, "<=64"},
		{maxCapacityBuckets, 1025, ">1024"},
		{replicaNumberBuckets, 1, "1"},
		{replicaNumberBuckets, 2, "2"},
		{replicaNumberBuckets, 7, "3+"},
	}
	for _, c := range cases {
		assert.Equal(t, c.want, c.spec.bucket(c.v), "%s(%d)", c.spec.name, c.v)
	}
}

func TestComputeIndexEntries(t *testing.T) {
	indexes := []*model.Index{
		{
			CollectionID: 1, IndexID: 1, IndexName: sentinel, FieldID: 104,
			IndexParams: []*commonpb.KeyValuePair{kv(common.IndexTypeKey, "HNSW"), kv(common.MetricTypeKey, "COSINE"), kv("M", "16"), kv("efConstruction", "200")},
			UserIndexParams: []*commonpb.KeyValuePair{
				kv(common.IndexTypeKey, "HNSW"), kv(common.MetricTypeKey, "COSINE"),
				kv(common.ParamsKey, `{"M":16,"efConstruction":200,"refine":true,"`+sentinel+`":1}`), kv(common.MmapEnabledKey, "true"),
			},
		},
		{
			CollectionID: 1, IndexID: 2, IndexName: sentinel, FieldID: 105,
			IndexParams:     []*commonpb.KeyValuePair{kv(common.IndexTypeKey, "SPARSE_INVERTED_INDEX"), kv(common.MetricTypeKey, "BM25")},
			UserIndexParams: []*commonpb.KeyValuePair{kv(common.IndexTypeKey, "SPARSE_INVERTED_INDEX"), kv(common.MetricTypeKey, "BM25"), kv(common.ParamsKey, "not json")},
		},
		{
			CollectionID: 2, IndexID: 3, IndexName: sentinel, FieldID: 101, IsAutoIndex: true,
			IndexParams:     []*commonpb.KeyValuePair{kv(common.IndexTypeKey, "HNSW"), kv(common.MetricTypeKey, "L2")},
			UserIndexParams: []*commonpb.KeyValuePair{kv(common.IndexTypeKey, "AUTOINDEX"), kv(common.MetricTypeKey, "L2")},
		},
		{
			CollectionID: 3, IndexID: 4, IsDeleted: true,
			IndexParams: []*commonpb.KeyValuePair{kv(common.IndexTypeKey, "DISKANN"), kv(common.MetricTypeKey, "IP")},
		},
		nil,
	}
	entries := ComputeIndexEntries(indexes)

	it := indexGroup(entries, GroupIndexTypes)
	assert.EqualValues(t, 2, it["HNSW"].Value, "effective index type from IndexParams; one per collection")
	assert.EqualValues(t, 1, it["SPARSE_INVERTED_INDEX"].Value)
	assert.NotContains(t, it, "DISKANN", "deleted indexes are skipped")
	assert.NotContains(t, it, "AUTOINDEX")

	mt := indexGroup(entries, GroupMetricTypes)
	assert.EqualValues(t, 1, mt["COSINE"].Value)
	assert.EqualValues(t, 1, mt["BM25"].Value)
	assert.EqualValues(t, 1, mt["L2"].Value)
	assert.NotContains(t, mt, "IP")

	ip := indexGroup(entries, GroupIndexParams)
	assert.EqualValues(t, 1, ip["M"].Value, "params JSON is opened one level")
	assert.EqualValues(t, 1, ip["efConstruction"].Value)
	assert.EqualValues(t, 1, ip["refine=true"].Value, "boolean values inside params are split")
	assert.EqualValues(t, 1, ip[common.MmapEnabledKey+"=true"].Value)
	assert.EqualValues(t, 1, ip[CustomKey].Value)
	assert.NotContains(t, ip, common.IndexTypeKey)
	assert.NotContains(t, ip, common.MetricTypeKey)
	assert.NotContains(t, ip, common.ParamsKey)

	dc := indexGroup(entries, GroupDeclared)
	assert.EqualValues(t, 1, dc[DeclaredIsAutoIndex].Value)
	assert.Len(t, dc, 1)

	assert.Contains(t, indexGroup(ComputeIndexEntries(nil), GroupDeclared), DeclaredIsAutoIndex)
}

// No user-controlled string may reach the report. Every name, description,
// model name, key value, custom key and provider in the input is the sentinel;
// the serialized output must not contain it.
func TestSanitization(t *testing.T) {
	col := sampleCollection()
	col.Properties = append(col.Properties,
		kv(common.CollectionResourceGroups, sentinel),
		kv(common.EncryptionEzIDKey, sentinel),
		kv(common.CollectionDescription, sentinel),
	)
	col.Functions[1].Params = append(col.Functions[1].Params, kv("provider", sentinel)) // second provider, unrecognized
	in := CollectionInput{
		Databases:   []*model.Database{{Name: sentinel, Properties: []*commonpb.KeyValuePair{kv(sentinel, sentinel), kv(common.DatabaseResourceGroups, sentinel)}}},
		Collections: []*model.Collection{col},
	}
	indexes := []*model.Index{{
		CollectionID: 1, IndexName: sentinel,
		IndexParams:     []*commonpb.KeyValuePair{kv(common.IndexTypeKey, "HNSW"), kv(common.MetricTypeKey, "L2")},
		UserIndexParams: []*commonpb.KeyValuePair{kv(common.ParamsKey, `{"`+sentinel+`":"`+sentinel+`"}`), kv(sentinel, sentinel), kv(common.MmapEnabledKey, sentinel)},
	}}

	var sb strings.Builder
	for _, e := range ComputeCollectionEntries(in) {
		sb.WriteString(e.String())
	}
	for _, e := range ComputeIndexEntries(indexes) {
		sb.WriteString(e.String())
	}
	assert.NotContains(t, sb.String(), sentinel)
}

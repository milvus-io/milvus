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

package loadresource

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/cgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/indexparams"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestEstimateIndexLoadResourceDoesNotReadIndexFiles(t *testing.T) {
	field := &schemapb.FieldSchema{
		FieldID:  100,
		Name:     "vec",
		DataType: schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: "128"},
		},
	}
	segment := &querypb.SegmentLoadInfo{
		CollectionID: 1,
		PartitionID:  2,
		SegmentID:    3,
		NumOfRows:    1000,
	}
	index := &querypb.FieldIndexInfo{
		FieldID:   100,
		IndexID:   10,
		BuildID:   11,
		IndexSize: 1024 * 1024,
		NumRows:   1000,
		IndexParams: []*commonpb.KeyValuePair{
			{Key: common.IndexTypeKey, Value: "HNSW"},
			{Key: common.MetricTypeKey, Value: "L2"},
		},
		IndexFilePaths: []string{"/path/that/must/not/exist/index_file"},
	}

	usage, err := EstimateIndexLoadResource(context.Background(), field, segment, index)

	require.NoError(t, err)
	require.NotZero(t, usage.FinalMemoryBytes)
	require.True(t, usage.HasRawData)
}

func TestEstimateIndexLoadResourceWithRunner(t *testing.T) {
	field := &schemapb.FieldSchema{
		FieldID:  100,
		Name:     "vec",
		DataType: schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: "128"},
		},
	}
	segment := &querypb.SegmentLoadInfo{
		CollectionID: 1,
		PartitionID:  2,
		SegmentID:    3,
		NumOfRows:    1000,
	}
	index := &querypb.FieldIndexInfo{
		FieldID:   100,
		IndexID:   10,
		BuildID:   11,
		IndexSize: 1024 * 1024,
		NumRows:   1000,
		IndexParams: []*commonpb.KeyValuePair{
			{Key: common.IndexTypeKey, Value: "HNSW"},
			{Key: common.MetricTypeKey, Value: "L2"},
		},
	}
	calls := 0
	runner := func(fn func() error) error {
		calls++
		return fn()
	}

	usage, err := EstimateIndexLoadResourceWithRunner(context.Background(), field, segment, index, runner)

	require.NoError(t, err)
	require.NotZero(t, usage.FinalMemoryBytes)
	require.Equal(t, 1, calls)
}

func TestBuildCLoadIndexInfoPreservesIndexNumRows(t *testing.T) {
	field := &schemapb.FieldSchema{
		FieldID:  100,
		Name:     "vec",
		DataType: schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: "128"},
		},
	}
	segment := &querypb.SegmentLoadInfo{
		CollectionID: 1,
		PartitionID:  2,
		SegmentID:    3,
		NumOfRows:    1000,
	}
	index := &querypb.FieldIndexInfo{
		FieldID:   100,
		IndexID:   10,
		BuildID:   11,
		IndexSize: 1024 * 1024,
		IndexParams: []*commonpb.KeyValuePair{
			{Key: common.IndexTypeKey, Value: "HNSW"},
			{Key: common.MetricTypeKey, Value: "L2"},
		},
	}

	info, err := buildCLoadIndexInfo(field, segment, index)

	require.NoError(t, err)
	require.Zero(t, info.GetNumRows())
}

func TestBuildCLoadIndexInfoPreservesIndexStorePathVersion(t *testing.T) {
	field := &schemapb.FieldSchema{
		FieldID:  100,
		Name:     "vec",
		DataType: schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: "128"},
		},
	}
	segment := &querypb.SegmentLoadInfo{
		CollectionID: 1,
		PartitionID:  2,
		SegmentID:    3,
		NumOfRows:    1000,
	}
	index := &querypb.FieldIndexInfo{
		FieldID:               100,
		IndexID:               10,
		BuildID:               11,
		IndexSize:             1024 * 1024,
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
		IndexParams: []*commonpb.KeyValuePair{
			{Key: common.IndexTypeKey, Value: "HNSW"},
			{Key: common.MetricTypeKey, Value: "L2"},
		},
	}

	info, err := buildCLoadIndexInfo(field, segment, index)

	require.NoError(t, err)
	require.Equal(t, indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED, info.GetIndexStorePathVersion())
}

func TestBuildCLoadIndexInfoResolvesMmap(t *testing.T) {
	paramtable.Init()

	t.Run("explicit disable takes precedence", func(t *testing.T) {
		paramtable.Get().Save(paramtable.Get().QueryNodeCfg.MmapVectorIndex.Key, "true")
		defer paramtable.Get().Reset(paramtable.Get().QueryNodeCfg.MmapVectorIndex.Key)

		info, err := buildCLoadIndexInfo(
			&schemapb.FieldSchema{DataType: schemapb.DataType_FloatVector},
			&querypb.SegmentLoadInfo{},
			&querypb.FieldIndexInfo{IndexParams: []*commonpb.KeyValuePair{
				{Key: common.IndexTypeKey, Value: "IVF_FLAT"},
				{Key: common.MmapEnabledKey, Value: "false"},
			}},
		)

		require.NoError(t, err)
		require.False(t, info.GetEnableMmap())
	})

	t.Run("global defaults apply to supported indexes", func(t *testing.T) {
		paramtable.Get().Save(paramtable.Get().QueryNodeCfg.MmapScalarIndex.Key, "true")
		defer paramtable.Get().Reset(paramtable.Get().QueryNodeCfg.MmapScalarIndex.Key)

		info, err := buildCLoadIndexInfo(
			&schemapb.FieldSchema{DataType: schemapb.DataType_String},
			&querypb.SegmentLoadInfo{},
			&querypb.FieldIndexInfo{IndexParams: []*commonpb.KeyValuePair{
				{Key: common.IndexTypeKey, Value: "INVERTED"},
			}},
		)

		require.NoError(t, err)
		require.True(t, info.GetEnableMmap())
	})
}

func TestBuildCLoadIndexInfoResolvesWarmup(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().QueryNodeCfg.TieredWarmupScalarIndex.Key, common.WarmupAsync)
	defer paramtable.Get().Reset(paramtable.Get().QueryNodeCfg.TieredWarmupScalarIndex.Key)

	t.Run("index setting takes precedence", func(t *testing.T) {
		info, err := buildCLoadIndexInfo(
			&schemapb.FieldSchema{DataType: schemapb.DataType_String},
			&querypb.SegmentLoadInfo{},
			&querypb.FieldIndexInfo{IndexParams: []*commonpb.KeyValuePair{
				{Key: common.IndexTypeKey, Value: "INVERTED"},
				{Key: common.WarmupKey, Value: common.WarmupDisable},
			}},
		)

		require.NoError(t, err)
		require.Equal(t, common.WarmupDisable, info.GetIndexParams()[common.WarmupKey])
	})

	t.Run("global setting is the fallback", func(t *testing.T) {
		info, err := buildCLoadIndexInfo(
			&schemapb.FieldSchema{DataType: schemapb.DataType_String},
			&querypb.SegmentLoadInfo{},
			&querypb.FieldIndexInfo{IndexParams: []*commonpb.KeyValuePair{
				{Key: common.IndexTypeKey, Value: "INVERTED"},
			}},
		)

		require.NoError(t, err)
		require.Equal(t, common.WarmupAsync, info.GetLoadParams()[common.WarmupKey])
		require.NotContains(t, info.GetIndexParams(), common.WarmupKey)
	})
}

func TestEstimateIndexLoadResourceReturnsErrorWithoutIndexType(t *testing.T) {
	field := &schemapb.FieldSchema{
		FieldID:  100,
		Name:     "vec",
		DataType: schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: "128"},
		},
	}
	segment := &querypb.SegmentLoadInfo{
		CollectionID: 1,
		PartitionID:  2,
		SegmentID:    3,
		NumOfRows:    1000,
	}
	index := &querypb.FieldIndexInfo{
		FieldID:   100,
		IndexID:   10,
		BuildID:   11,
		IndexSize: 1024 * 1024,
		NumRows:   1000,
	}

	usage, err := EstimateIndexLoadResource(context.Background(), field, segment, index)

	require.Error(t, err)
	require.Zero(t, usage)
}

func TestBuildCLoadIndexInfoSeparatesRuntimeParams(t *testing.T) {
	params := paramtable.Get()
	field := &schemapb.FieldSchema{DataType: schemapb.DataType_FloatVector}
	for _, algorithm := range []string{"DISKANN", "AISAQ"} {
		t.Run(algorithm, func(t *testing.T) {
			prefix := "knowhere." + algorithm + ".load."
			for _, key := range []string{"search_cache_budget_gb_ratio", "use_bfs_cache", "disk_pq_dims"} {
				defer params.Remove(prefix + key)
			}
			for _, explicit := range []bool{false, true} {
				metadata := &querypb.FieldIndexInfo{IndexParams: []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: algorithm}, {Key: common.MmapEnabledKey, Value: "false"}}}
				if explicit {
					metadata.IndexParams = append(metadata.IndexParams,
						&commonpb.KeyValuePair{Key: "search_cache_budget_gb_ratio", Value: "0"},
						&commonpb.KeyValuePair{Key: "use_bfs_cache", Value: "false"})
				}
				original := proto.Clone(metadata).(*querypb.FieldIndexInfo)
				for _, ratio := range []string{"0.25", "0.5"} {
					require.NoError(t, params.Save(prefix+"search_cache_budget_gb_ratio", ratio))
					require.NoError(t, params.Save(prefix+"use_bfs_cache", "true"))
					require.NoError(t, params.Save(prefix+"disk_pq_dims", "0"))
					payload, err := buildCLoadIndexInfo(field, &querypb.SegmentLoadInfo{}, metadata)
					require.NoError(t, err)
					raw, err := proto.Marshal(payload)
					require.NoError(t, err)
					wire := &cgopb.LoadIndexInfo{}
					require.NoError(t, proto.Unmarshal(raw, wire))
					require.Equal(t, funcutil.KeyValuePair2Map(original.GetIndexParams()), wire.GetIndexParams())
					require.False(t, wire.GetEnableMmap())
					require.True(t, proto.Equal(original, metadata))
					if explicit {
						require.NotContains(t, wire.GetLoadParams(), "search_cache_budget_gb_ratio")
						require.NotContains(t, wire.GetLoadParams(), "use_bfs_cache")
					} else {
						require.Equal(t, ratio, wire.GetLoadParams()["search_cache_budget_gb_ratio"])
						require.Equal(t, "true", wire.GetLoadParams()["use_bfs_cache"])
					}
					require.Equal(t, "0", wire.GetLoadParams()["disk_pq_dims"])
				}
			}
		})
	}
}

func TestEstimateIndexLoadResourceIgnoresLoadRepresentationDefaults(t *testing.T) {
	params := paramtable.Get()
	const key = "knowhere.AISAQ.load.disk_pq_dims"
	require.NoError(t, params.Save(key, "0"))
	defer params.Remove(key)
	field := &schemapb.FieldSchema{FieldID: 101, DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "32"}}}
	segment := &querypb.SegmentLoadInfo{NumOfRows: 1024}
	index := &querypb.FieldIndexInfo{FieldID: 101, NumRows: 1024, IndexParams: []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "AISAQ"}, {Key: common.MetricTypeKey, Value: "L2"}}}
	usage, err := EstimateIndexLoadResource(context.Background(), field, segment, index)
	require.NoError(t, err)
	require.False(t, usage.HasRawData, "load-only disk_pq_dims cannot change the persisted compressed representation")
	index.IndexParams = append(index.IndexParams, &commonpb.KeyValuePair{Key: "disk_pq_dims", Value: "0"})
	usage, err = EstimateIndexLoadResource(context.Background(), field, segment, index)
	require.NoError(t, err)
	require.True(t, usage.HasRawData, "explicit build choice still determines static capability")
}

func TestBuildCLoadIndexInfoRuntimeBitmapMmapWarmup(t *testing.T) {
	params := paramtable.Get()
	require.NoError(t, params.Save(params.QueryNodeCfg.IndexOffsetCacheEnabled.Key, "true"))
	defer params.Reset(params.QueryNodeCfg.IndexOffsetCacheEnabled.Key)
	require.NoError(t, params.Save("knowhere.BITMAP.load."+common.MmapEnabledKey, "true"))
	defer params.Remove("knowhere.BITMAP.load." + common.MmapEnabledKey)
	require.NoError(t, params.Save("knowhere.BITMAP.load."+common.WarmupKey, common.WarmupAsync))
	defer params.Remove("knowhere.BITMAP.load." + common.WarmupKey)
	field := &schemapb.FieldSchema{DataType: schemapb.DataType_Int64}
	metadata := &querypb.FieldIndexInfo{IndexParams: []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "BITMAP"}}}
	payload, err := buildCLoadIndexInfo(field, &querypb.SegmentLoadInfo{}, metadata)
	require.NoError(t, err)
	require.Equal(t, map[string]string{common.IndexTypeKey: "BITMAP"}, payload.GetIndexParams())
	require.Equal(t, "true", payload.GetLoadParams()[common.IndexOffsetCacheEnabledKey])
	require.Equal(t, common.WarmupAsync, payload.GetLoadParams()[common.WarmupKey])
	require.True(t, payload.GetEnableMmap())
	metadata.IndexParams = append(metadata.IndexParams,
		&commonpb.KeyValuePair{Key: common.MmapEnabledKey, Value: "false"},
		&commonpb.KeyValuePair{Key: common.WarmupKey, Value: common.WarmupDisable},
		&commonpb.KeyValuePair{Key: common.IndexOffsetCacheEnabledKey, Value: "false"})
	payload, err = buildCLoadIndexInfo(field, &querypb.SegmentLoadInfo{}, metadata)
	require.NoError(t, err)
	require.False(t, payload.GetEnableMmap())
	require.Equal(t, funcutil.KeyValuePair2Map(metadata.GetIndexParams()), payload.GetIndexParams())
	for _, key := range []string{common.MmapEnabledKey, common.WarmupKey, common.IndexOffsetCacheEnabledKey} {
		require.NotContains(t, payload.GetLoadParams(), key)
	}
}

func TestBuildCLoadIndexInfoPropagatesRuntimePreparationError(t *testing.T) {
	patch := mockey.Mock(indexparams.GetRuntimeLoadParams).
		To(func(_ *paramtable.ComponentParam, _ map[string]string) (map[string]string, error) {
			return nil, merr.ErrServiceUnavailable
		}).Build()
	defer patch.UnPatch()
	info, err := buildCLoadIndexInfo(&schemapb.FieldSchema{}, &querypb.SegmentLoadInfo{}, &querypb.FieldIndexInfo{IndexParams: []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "TEST_INDEX"}}})
	require.Nil(t, info)
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
}

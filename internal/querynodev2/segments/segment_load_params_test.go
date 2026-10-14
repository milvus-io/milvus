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

package segments

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// TestSegmentLoaderFullLoadParameterOwnership enters through the real full-load
// path, including NewSegment and LoadSegment. Capture the native creation
// request: segcore serializes this LoadInfo into NewSegmentWithLoadInfo, and the
// subsequent CSegment.Load consumes that metadata via AsyncSegmentLoad.
func TestSegmentLoaderFullLoadParameterOwnership(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	for _, setting := range []struct{ key, value string }{
		{params.KnowhereConfig.Enable.Key, "true"},
		{params.AutoIndexConfig.Enable.Key, "false"},
		{params.CommonCfg.BloomFilterEnabled.Key, "false"},
		{params.QueryNodeCfg.TieredEvictionEnabled.Key, "false"},
		{params.QueryNodeCfg.ExprResCacheEnabled.Key, "false"},
	} {
		require.NoError(t, params.Save(setting.key, setting.value))
		defer params.Reset(setting.key)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	manager := NewManager()
	loader := NewLoader(ctx, manager, nil)
	const collectionID = int64(10)
	schema := mock_segcore.GenTestCollectionSchema("load_parameters", schemapb.DataType_Int64, false)
	require.NoError(t, manager.Collection.PutOrRef(collectionID, schema,
		mock_segcore.GenTestIndexMeta(collectionID, schema),
		&querypb.LoadMetaInfo{CollectionID: collectionID, LoadType: querypb.LoadType_LoadCollection}))
	defer manager.Collection.Unref(collectionID, 1)
	vectorField := funcutil.GetVecFieldIDs(schema)[0]

	// Capacity estimates and file IO are unrelated to parameter transport. Keep
	// all load preparation/segment construction/load methods real; mock only
	// the native boundary and estimates so no disk index fixture is required.
	resourcePatch := mockey.Mock((*segmentLoader).requestResource).
		To(func(_ *segmentLoader, _ context.Context, _ ...*querypb.SegmentLoadInfo) (requestResourceResult, error) {
			return requestResourceResult{ConcurrencyLevel: 1}, nil
		}).Build()
	defer resourcePatch.UnPatch()
	estimatePatch := mockey.Mock(estimateLogicalResourceUsageOfSegment).
		To(func(_ *schemapb.CollectionSchema, _ *querypb.SegmentLoadInfo, _ resourceEstimateFactor) (*ResourceUsage, error) {
			return &ResourceUsage{}, nil
		}).Build()
	defer estimatePatch.UnPatch()

	segmentID := int64(20)
	for _, algorithm := range []string{"DISKANN", "AISAQ"} {
		cacheKey := "knowhere." + algorithm + ".load.search_cache_budget_gb_ratio"
		bfsKey := "knowhere." + algorithm + ".load.use_bfs_cache"
		for _, explicit := range []bool{false, true} {
			metadata := &querypb.SegmentLoadInfo{
				CollectionID: collectionID, PartitionID: 11, SegmentID: segmentID,
				NumOfRows: 1024, Level: datapb.SegmentLevel_L1,
				InsertChannel: "by-dev-rootcoord-dml_0_10v0",
				IndexInfos: []*querypb.FieldIndexInfo{{
					FieldID: vectorField, IndexID: 100, BuildID: 200, NumRows: 1024,
					IndexFilePaths: []string{"index.data"},
					IndexParams: []*commonpb.KeyValuePair{
						{Key: common.IndexTypeKey, Value: algorithm},
						{Key: common.MetricTypeKey, Value: "L2"},
						{Key: common.DimKey, Value: fmt.Sprint(mock_segcore.DefaultDim)},
					},
				}},
			}
			if explicit {
				metadata.IndexInfos[0].IndexParams = append(metadata.IndexInfos[0].IndexParams,
					&commonpb.KeyValuePair{Key: "search_cache_budget_gb_ratio", Value: "0"},
					&commonpb.KeyValuePair{Key: "use_bfs_cache", Value: "false"})
			}
			for _, ratio := range []string{"0.25", "0.5"} {
				t.Run(fmt.Sprintf("%s/explicit=%t/live=%s", algorithm, explicit, ratio), func(t *testing.T) {
					require.NoError(t, params.Save(cacheKey, ratio))
					require.NoError(t, params.Save(bfsKey, "true"))
					defer params.Remove(cacheKey)
					defer params.Remove(bfsKey)

					var captured *segcorepb.SegmentLoadInfo
					var capturedMetadata *querypb.SegmentLoadInfo
					csegment := mock_segcore.NewMockCSegment(t)
					csegment.EXPECT().RawPointer().Return(nil)
					csegment.EXPECT().MemSize().Return(int64(0)).Maybe()
					csegment.EXPECT().RowNum().Return(int64(1024)).Maybe()
					csegment.EXPECT().Load(mock.Anything).Return(nil).Once()
					createPatch := mockey.Mock(segcore.CreateCSegment).
						To(func(req *segcore.CreateCSegmentRequest) (segcore.CSegment, error) {
							capturedMetadata = proto.Clone(req.LoadInfo).(*querypb.SegmentLoadInfo)
							converted, err := segcore.ConvertToSegcoreSegmentLoadInfo(req.LoadInfo)
							if err != nil {
								return nil, err
							}
							payload, err := proto.Marshal(converted)
							if err != nil {
								return nil, err
							}
							captured = &segcorepb.SegmentLoadInfo{}
							if err := proto.Unmarshal(payload, captured); err != nil {
								return nil, err
							}
							return csegment, nil
						}).Build()
					defer createPatch.UnPatch()

					// Release/load uses the same segment ID but a fresh metadata copy.
					loaded, err := loader.Load(ctx, collectionID, SegmentTypeSealed, 1,
						proto.Clone(metadata).(*querypb.SegmentLoadInfo))
					require.NoError(t, err)
					require.Len(t, loaded, 1)
					defer manager.Segment.Remove(ctx, segmentID, querypb.DataScope_All)
					require.NotNil(t, captured)
					require.Len(t, captured.GetIndexInfos(), 1)
					original := funcutil.KeyValuePair2Map(metadata.GetIndexInfos()[0].GetIndexParams())
					require.Equal(t, original, funcutil.KeyValuePair2Map(capturedMetadata.GetIndexInfos()[0].GetIndexParams()))
					require.Equal(t, original, funcutil.KeyValuePair2Map(captured.GetIndexInfos()[0].GetIndexParams()))
					loadParams := captured.GetIndexInfos()[0].GetLoadParams()
					actual := make(map[string]string)
					for key, value := range loadParams {
						actual[key] = value
					}
					for key, value := range original {
						actual[key] = value
					}
					wantRatio, wantBfs := ratio, "true"
					if explicit {
						wantRatio, wantBfs = "0", "false"
					}
					require.Equal(t, algorithm, actual[common.IndexTypeKey])
					require.Equal(t, wantRatio, actual["search_cache_budget_gb_ratio"])
					require.Equal(t, wantBfs, actual["use_bfs_cache"])
					require.NotContains(t, actual, "num_load_thread")
					require.NotContains(t, actual, "search_cache_budget_gb")
					if explicit {
						require.NotContains(t, loadParams, "search_cache_budget_gb_ratio")
						require.NotContains(t, loadParams, "use_bfs_cache")
					} else {
						require.NotContains(t, original, "search_cache_budget_gb_ratio")
						require.NotContains(t, original, "use_bfs_cache")
					}
				})
			}
			segmentID++
		}
	}
}

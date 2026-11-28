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

package querycoordv2

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metacache"
	metastoremocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/querycoordv2/autoscale"
	"github.com/milvus-io/milvus/internal/querycoordv2/job"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr/loadmgrtest"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func setAutoscaleCapacityRatiosForTest(t *testing.T, memoryHighWaterLevel, maxDiskUsagePercentage string) {
	t.Helper()
	params := paramtable.Get()
	require.NoError(t, params.Save(params.QuotaConfig.QueryNodeMemoryHighWaterLevel.Key, memoryHighWaterLevel))
	require.NoError(t, params.Save(params.QueryNodeCfg.MaxDiskUsagePercentage.Key, maxDiskUsagePercentage))
	t.Cleanup(func() {
		params.Reset(params.QuotaConfig.QueryNodeMemoryHighWaterLevel.Key)
		params.Reset(params.QueryNodeCfg.MaxDiskUsagePercentage.Key)
	})
}

func TestAutoscalePrecheckDecision(t *testing.T) {
	setAutoscaleCapacityRatiosForTest(t, "1", "100")

	required := autoscaleResourceUsage{memoryBytes: 200, diskBytes: 160}
	current := autoscaleResourceUsage{memoryBytes: 100, diskBytes: 100}
	globalCapacity := autoscaleResourceUsage{memoryBytes: 900, diskBytes: 940}
	check := func(required, available, globalCapacity, globalLimit autoscaleResourceUsage, autoscaleEnabled bool) error {
		rgName := meta.DefaultResourceGroupName
		return checkAutoscaleLoadResourceByRGWithCapacity(
			map[string]autoscaleResourceUsage{rgName: required},
			map[string]autoscaleResourceUsage{rgName: available},
			map[string]autoscaleResourceUsage{rgName: available},
			globalCapacity,
			globalLimit,
			autoscaleEnabled,
		)
	}

	assert.NoError(t, check(required, required, globalCapacity, autoscaleResourceUsage{}, false))
	assert.Error(t, check(required, current, globalCapacity, autoscaleResourceUsage{}, false))
	assert.Error(t, check(required, current, globalCapacity, autoscaleResourceUsage{}, true))
	assert.NoError(t, check(required, current, globalCapacity, autoscaleResourceUsage{memoryBytes: 1000, diskBytes: 1000}, true))
	assert.NoError(t, check(
		autoscaleResourceUsage{memoryBytes: 200, diskBytes: 100},
		current,
		globalCapacity,
		autoscaleResourceUsage{memoryBytes: 1000},
		true,
	))

	err := check(required, current, globalCapacity, autoscaleResourceUsage{memoryBytes: 999, diskBytes: 1000}, true)
	require.Error(t, err)

	var precheckErr *loadResourcePrecheckError
	require.True(t, errors.As(err, &precheckErr))
	assert.Equal(t, int64(100), precheckErr.suggestedExpandPercent)
	assert.ErrorIs(t, err, merr.ErrServiceResourceInsufficient)
	assert.Equal(t, "Insufficient cluster resources. Please scale out the cluster or enable autoscaling. Suggested scale-out ratio: 100%", err.Error())
}

func TestAutoscalePrecheckDecisionByResourceGroup(t *testing.T) {
	setAutoscaleCapacityRatiosForTest(t, "1", "100")

	requiredByRG := map[string]autoscaleResourceUsage{
		"rg-a": {memoryBytes: 200, diskBytes: 100},
		"rg-b": {memoryBytes: 100, diskBytes: 100},
	}
	currentByRG := map[string]autoscaleResourceUsage{
		"rg-a": {memoryBytes: 100, diskBytes: 100},
		"rg-b": {memoryBytes: 1000, diskBytes: 1000},
	}

	err := checkAutoscaleLoadResourceByRGWithCapacity(requiredByRG, currentByRG, currentByRG, autoscaleResourceUsage{}, autoscaleResourceUsage{}, false)
	require.Error(t, err)

	var precheckErr *loadResourcePrecheckError
	require.True(t, errors.As(err, &precheckErr))
	assert.Equal(t, int64(100), precheckErr.suggestedExpandPercent)

	assert.NoError(t, checkAutoscaleLoadResourceByRGWithCapacity(
		requiredByRG,
		currentByRG,
		currentByRG,
		autoscaleResourceUsage{memoryBytes: 900},
		autoscaleResourceUsage{memoryBytes: 1000},
		true,
	))
	assert.Error(t, checkAutoscaleLoadResourceByRGWithCapacity(
		requiredByRG,
		currentByRG,
		currentByRG,
		autoscaleResourceUsage{memoryBytes: 901},
		autoscaleResourceUsage{memoryBytes: 1000},
		true,
	))
}

func TestAutoscalePrecheckGlobalLimitUsesTotalCapacity(t *testing.T) {
	setAutoscaleCapacityRatiosForTest(t, "0.9", "90")

	globalCapacity := autoscaleResourceUsage{memoryBytes: 80, diskBytes: 80}
	globalLimit := autoscaleResourceUsage{memoryBytes: 100, diskBytes: 100}

	assert.True(t, canExpandWithinGlobalLimit(
		autoscaleResourceUsage{memoryBytes: 18, diskBytes: 18},
		globalCapacity,
		globalLimit,
	))
	assert.False(t, canExpandWithinGlobalLimit(
		autoscaleResourceUsage{memoryBytes: 19, diskBytes: 18},
		globalCapacity,
		globalLimit,
	))
	assert.False(t, canExpandWithinGlobalLimit(
		autoscaleResourceUsage{memoryBytes: 18, diskBytes: 19},
		globalCapacity,
		globalLimit,
	))
	assert.False(t, canExpandWithinGlobalLimit(
		autoscaleResourceUsage{memoryBytes: 1, diskBytes: 1},
		globalLimit,
		globalLimit,
	))
}

func TestAutoscalePrecheckSuggestionUsesCapacityBase(t *testing.T) {
	requiredByRG := map[string]autoscaleResourceUsage{
		"rg-a": {memoryBytes: 200, diskBytes: 160},
	}
	availableByRG := map[string]autoscaleResourceUsage{
		"rg-a": {memoryBytes: 100, diskBytes: 100},
	}
	capacityByRG := map[string]autoscaleResourceUsage{
		"rg-a": {memoryBytes: 1000, diskBytes: 1000},
	}

	err := checkAutoscaleLoadResourceByRGWithCapacity(
		requiredByRG,
		availableByRG,
		capacityByRG,
		autoscaleResourceUsage{},
		autoscaleResourceUsage{},
		false,
	)
	require.Error(t, err)

	var precheckErr *loadResourcePrecheckError
	require.True(t, errors.As(err, &precheckErr))
	assert.Equal(t, int64(10), precheckErr.suggestedExpandPercent)
}

func TestAutoscalePrecheckIncludesNegativeAvailableResourceInShortage(t *testing.T) {
	err := checkAutoscaleLoadResourceByRGWithCapacity(
		map[string]autoscaleResourceUsage{"rg": {memoryBytes: 10}},
		map[string]autoscaleResourceUsage{"rg": {memoryBytes: -20}},
		map[string]autoscaleResourceUsage{"rg": {memoryBytes: 100}},
		autoscaleResourceUsage{},
		autoscaleResourceUsage{},
		false,
	)

	require.Error(t, err)
	var precheckErr *loadResourcePrecheckError
	require.ErrorAs(t, err, &precheckErr)
	assert.Equal(t, int64(30), precheckErr.suggestedExpandPercent)
}

func TestAutoscalePrecheckGlobalLimitUsesGB(t *testing.T) {
	limit := autoscaleGlobalLimitFromConfig(2, 3)

	assert.Equal(t, int64(2*1024*1024*1024), limit.memoryBytes)
	assert.Equal(t, int64(3*1024*1024*1024), limit.diskBytes)
}

func TestBuildRequiredLoadResourceByRGForLoadConfig(t *testing.T) {
	requiredByRG := buildRequiredLoadResourceByRGForLoadConfig(
		autoscaleResourceUsage{memoryBytes: 300, diskBytes: 200},
		autoscaleResourceUsage{memoryBytes: 100, diskBytes: 80},
		map[string]int{
			"rg-a": 1,
			"rg-b": 1,
		},
		map[string]int{
			"rg-a": 2,
			"rg-b": 0,
			"rg-c": 1,
		},
	)

	assert.Equal(t, map[string]autoscaleResourceUsage{
		"rg-a": {memoryBytes: 500, diskBytes: 320},
		"rg-c": {memoryBytes: 300, diskBytes: 200},
	}, requiredByRG)
}

func TestAutoscaleLoadResourceFiltersLoadFieldsAndKeepsAllIndexesPerField(t *testing.T) {
	segments := []*datapb.SegmentInfo{
		{
			ID:          1,
			PartitionID: 10,
			Binlogs: []*datapb.FieldBinlog{
				{FieldID: 100},
				{FieldID: 101},
			},
			Statslogs: []*datapb.FieldBinlog{
				{FieldID: 100},
				{FieldID: 101},
			},
			Deltalogs: []*datapb.FieldBinlog{
				{FieldID: 999},
			},
			Bm25Statslogs: []*datapb.FieldBinlog{
				{FieldID: 100},
				{FieldID: 101},
			},
			JsonKeyStats: map[int64]*datapb.JsonKeyStats{
				100: {},
				101: {},
			},
			TextStatsLogs: map[int64]*datapb.TextIndexStats{
				100: {},
				101: {},
			},
		},
	}

	filteredSegments := autoscale.FilterSegmentsByLoadFields(segments, []int64{100})
	require.Len(t, filteredSegments, 1)
	require.Len(t, filteredSegments[0].GetBinlogs(), 1)
	assert.Equal(t, int64(100), filteredSegments[0].GetBinlogs()[0].GetFieldID())
	require.Len(t, filteredSegments[0].GetStatslogs(), 1)
	assert.Equal(t, int64(100), filteredSegments[0].GetStatslogs()[0].GetFieldID())
	require.Len(t, filteredSegments[0].GetBm25Statslogs(), 1)
	assert.Equal(t, int64(100), filteredSegments[0].GetBm25Statslogs()[0].GetFieldID())
	assert.Contains(t, filteredSegments[0].GetJsonKeyStats(), int64(100))
	assert.NotContains(t, filteredSegments[0].GetJsonKeyStats(), int64(101))
	assert.Contains(t, filteredSegments[0].GetTextStatsLogs(), int64(100))
	assert.NotContains(t, filteredSegments[0].GetTextStatsLogs(), int64(101))
	require.Len(t, filteredSegments[0].GetDeltalogs(), 1)
	require.Len(t, segments[0].GetBinlogs(), 2)

	indexes := map[int64][]*querypb.FieldIndexInfo{
		1: {
			{FieldID: 100, IndexID: 10, IndexParams: []*commonpb.KeyValuePair{{Key: common.JSONPathKey, Value: "/a"}}},
			{FieldID: 100, IndexID: 11, IndexParams: []*commonpb.KeyValuePair{{Key: common.JSONPathKey, Value: "/b"}}},
			{FieldID: 101, IndexID: 20},
		},
	}

	filteredIndexes := autoscale.FilterIndexesByLoadFields(indexes, []int64{100})
	require.Len(t, filteredIndexes[1], 2)
	assert.ElementsMatch(t, []int64{10, 11}, lo.Map(filteredIndexes[1], func(index *querypb.FieldIndexInfo, _ int) int64 {
		return index.GetIndexID()
	}))
}

func TestGetIndexInfoBySegmentsBatchesRequests(t *testing.T) {
	ctx := context.Background()
	collectionID := int64(100)
	segmentIDs := make([]int64, maxSegmentNumPerGetIndexInfoRequest+1)
	for i := range segmentIDs {
		segmentIDs[i] = int64(i + 1)
	}

	firstBatchArgs := make([]interface{}, 0, maxSegmentNumPerGetIndexInfoRequest)
	for _, segmentID := range segmentIDs[:maxSegmentNumPerGetIndexInfoRequest] {
		firstBatchArgs = append(firstBatchArgs, segmentID)
	}
	broker := meta.NewMockBroker(t)
	broker.EXPECT().
		GetIndexInfo(mock.Anything, collectionID, firstBatchArgs...).
		Return(map[int64][]*querypb.FieldIndexInfo{
			segmentIDs[0]: {{FieldID: 10, IndexID: 100}},
		}, nil).
		Once()
	broker.EXPECT().
		GetIndexInfo(mock.Anything, collectionID, segmentIDs[maxSegmentNumPerGetIndexInfoRequest]).
		Return(map[int64][]*querypb.FieldIndexInfo{
			segmentIDs[maxSegmentNumPerGetIndexInfoRequest]: {{FieldID: 11, IndexID: 101}},
		}, nil).
		Once()

	indexes, err := getIndexInfoBySegments(ctx, broker, collectionID, segmentIDs)

	require.NoError(t, err)
	require.Len(t, indexes, 2)
	assert.Equal(t, int64(100), indexes[segmentIDs[0]][0].GetIndexID())
	assert.Equal(t, int64(101), indexes[segmentIDs[maxSegmentNumPerGetIndexInfoRequest]][0].GetIndexID())
}

func TestMemoryResourceFromMetricsUsesQueryNodeMetrics(t *testing.T) {
	params := paramtable.Get()
	params.Save(params.QuotaConfig.QueryNodeMemoryLowWaterLevel.Key, "0.4")
	defer params.Reset(params.QuotaConfig.QueryNodeMemoryLowWaterLevel.Key)
	params.Save(params.QuotaConfig.QueryNodeMemoryHighWaterLevel.Key, "0.5")
	defer params.Reset(params.QuotaConfig.QueryNodeMemoryHighWaterLevel.Key)

	for _, test := range []struct {
		usedMemory        uint64
		expectedAvailable int64
	}{
		{usedMemory: 400, expectedAvailable: 100},
		{usedMemory: 499, expectedAvailable: 1},
		{usedMemory: 500, expectedAvailable: 0},
		{usedMemory: 600, expectedAvailable: -100},
	} {
		availableMemoryBytes, capacityMemoryBytes := memoryResourceFromMetrics(1000, test.usedMemory)
		assert.Equal(t, test.expectedAvailable, availableMemoryBytes)
		assert.Equal(t, int64(1000), capacityMemoryBytes)
	}
}

const (
	autoscalePrecheckTestCollectionID = int64(100)
	autoscalePrecheckTestPartitionID  = int64(10)
	autoscalePrecheckTestSegmentID    = int64(1)
)

func newAutoscalePrecheckTestRequest() *job.AlterLoadConfigRequest {
	return &job.AlterLoadConfigRequest{
		CollectionInfo: &metacache.CollectionInfo{
			ID:         autoscalePrecheckTestCollectionID,
			Partitions: []int64{autoscalePrecheckTestPartitionID},
			Schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{{FieldID: 100, Name: "field_100", DataType: schemapb.DataType_Int64}},
			},
		},
		Expected: job.ExpectedLoadConfig{
			ExpectedPartitionIDs:  []int64{autoscalePrecheckTestPartitionID},
			ExpectedReplicaNumber: map[string]int{meta.DefaultResourceGroupName: 1},
		},
	}
}

func TestRequiredLoadResourceAppliesCollectionMmap(t *testing.T) {
	params := paramtable.Get()
	require.NoError(t, params.Save(params.QueryNodeCfg.TieredEvictionEnabled.Key, "false"))
	t.Cleanup(func() {
		params.Reset(params.QueryNodeCfg.TieredEvictionEnabled.Key)
		params.Reset(params.QueryNodeCfg.MmapScalarField.Key)
	})
	for _, test := range []struct {
		name, global, collection, field string
		wantDisk                        bool
	}{
		{"collection enables mmap", "false", "true", "", true},
		{"collection disables mmap", "true", "false", "", false},
		{"field disables mmap", "false", "true", "false", false},
		{"field enables mmap", "true", "false", "true", true},
		{"global default", "true", "", "", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.NoError(t, params.Save(params.QueryNodeCfg.MmapScalarField.Key, test.global))
			req := newAutoscalePrecheckTestRequest()
			if test.collection != "" {
				req.CollectionInfo.Properties = []*commonpb.KeyValuePair{{Key: common.MmapEnabledKey, Value: test.collection}}
			}
			field := req.CollectionInfo.Schema.Fields[0]
			if test.field != "" {
				field.TypeParams = []*commonpb.KeyValuePair{{Key: common.MmapEnabledKey, Value: test.field}}
			}
			server := &Server{broker: newAutoscalePrecheckTestBroker(t, nil, nil, nil)}

			usage, err := server.requiredLoadResourceByRG(context.Background(), req)

			require.NoError(t, err)
			want := autoscaleResourceUsage{memoryBytes: 1024}
			if test.wantDisk {
				want = autoscaleResourceUsage{diskBytes: 1024}
			}
			assert.Equal(t, want, usage[meta.DefaultResourceGroupName])
			if test.field == "" {
				assert.Empty(t, field.TypeParams, "estimation must not mutate the source schema")
			} else {
				assert.Equal(t, []*commonpb.KeyValuePair{{Key: common.MmapEnabledKey, Value: test.field}}, field.TypeParams)
			}
		})
	}
}

func enableAutoscalePrecheckForTest(t *testing.T) {
	t.Helper()
	params := paramtable.Get()
	params.Save(params.QueryCoordCfg.AutoscalePrecheckEnabled.Key, "true")
	params.Save(params.QueryNodeCfg.TieredEvictionEnabled.Key, "false")
	t.Cleanup(func() {
		params.Reset(params.QueryCoordCfg.AutoscalePrecheckEnabled.Key)
		params.Reset(params.QueryNodeCfg.TieredEvictionEnabled.Key)
	})
}

func newAutoscalePrecheckTestBroker(t *testing.T, recoveryErr, segmentErr, indexErr error) *meta.MockBroker {
	t.Helper()
	broker := meta.NewMockBroker(t)
	if recoveryErr != nil {
		broker.EXPECT().
			GetRecoveryInfoV2(mock.Anything, autoscalePrecheckTestCollectionID, autoscalePrecheckTestPartitionID).
			Return(nil, nil, recoveryErr).
			Once()
		return broker
	}

	broker.EXPECT().
		GetRecoveryInfoV2(mock.Anything, autoscalePrecheckTestCollectionID, autoscalePrecheckTestPartitionID).
		Return(nil, []*datapb.SegmentInfo{
			{
				ID:          autoscalePrecheckTestSegmentID,
				PartitionID: autoscalePrecheckTestPartitionID,
			},
		}, nil).
		Once()
	broker.EXPECT().
		GetSegmentInfoForResourceEstimation(mock.Anything, autoscalePrecheckTestCollectionID, autoscalePrecheckTestSegmentID).
		Return([]*datapb.SegmentInfo{
			{
				ID:             autoscalePrecheckTestSegmentID,
				PartitionID:    autoscalePrecheckTestPartitionID,
				NumOfRows:      1,
				StorageVersion: storage.StorageV3,
				ManifestPath:   "files/manifest",
				Stats: &datapb.Statistics{
					LoadResource: &datapb.LoadResourceStatistics{
						ColumnGroups: []*datapb.ColumnGroupStatistics{{
							GroupId:    100,
							FieldIds:   []int64{100},
							MemorySize: 1024,
						}},
					},
				},
			},
		}, nil, segmentErr).
		Once()
	if segmentErr != nil {
		return broker
	}
	broker.EXPECT().
		GetIndexInfo(mock.Anything, autoscalePrecheckTestCollectionID, autoscalePrecheckTestSegmentID).
		Return(nil, indexErr).
		Once()
	return broker
}

func newAutoscalePrecheckTestServer(t *testing.T, cluster session.Cluster) *Server {
	t.Helper()
	nodeManager := session.NewNodeManager()
	nodeManager.Add(session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: 1}))
	catalog := metastoremocks.NewQueryCoordCatalog(t)
	catalog.EXPECT().
		SaveResourceGroup(mock.Anything, mock.Anything).
		Return(nil).
		Once()
	store := metacache.NewMetaStore(nil)
	store.PutCollection(newAutoscalePrecheckTestRequest().CollectionInfo)
	metaManager := meta.NewMeta(nil, catalog, nodeManager, store)
	metaManager.HandleNodeUp(context.Background(), 1)
	return &Server{
		meta:    metaManager,
		nodeMgr: nodeManager,
		cluster: cluster,
	}
}

func newAutoscalePrecheckMetrics(t *testing.T, memory, memoryUsage uint64, disk float64) string {
	t.Helper()
	localDiskCapacity := int64(disk * 1e9)
	metrics, err := metricsinfo.MarshalComponentInfos(metricsinfo.QueryNodeInfos{
		BaseComponentInfos: metricsinfo.BaseComponentInfos{
			HardwareInfos: metricsinfo.HardwareMetrics{
				Memory:      memory,
				MemoryUsage: memoryUsage,
				Disk:        disk,
			},
		},
		LocalStorage: &metricsinfo.QueryNodeLocalStorageMetrics{
			CapacityBytes: localDiskCapacity,
			UsedBytes:     0,
		},
	})
	require.NoError(t, err)
	return metrics
}

func TestCurrentQueryNodeResourceStatusByRGUsesReportedAvailability(t *testing.T) {
	params := paramtable.Get()
	params.Save(params.QueryCoordCfg.AutoscaleEnabled.Key, "false")
	defer params.Reset(params.QueryCoordCfg.AutoscaleEnabled.Key)

	metricsPayload := newAutoscalePrecheckMetrics(t, 1024, 512, 1)
	cluster := session.NewMockCluster(t)
	cluster.EXPECT().GetMetrics(mock.Anything, int64(1), mock.Anything).
		Return(&milvuspb.GetMetricsResponse{Status: merr.Success(), Response: metricsPayload}, nil).
		Once()
	server := newAutoscalePrecheckTestServer(t, cluster)

	rawMemory, capacityMemory := memoryResourceFromMetrics(1024, 512)
	usableCapacity := usableResourceCapacity(autoscaleResourceUsage{memoryBytes: capacityMemory, diskBytes: 1e9})
	availableByRG, capacityByRG, _, err := server.currentQueryNodeResourceStatus(
		context.Background(),
		map[string]autoscaleResourceUsage{meta.DefaultResourceGroupName: {memoryBytes: 1}},
		false,
	)

	require.NoError(t, err)
	assert.Equal(t, rawMemory, availableByRG[meta.DefaultResourceGroupName].memoryBytes)
	assert.Equal(t, usableCapacity.diskBytes, availableByRG[meta.DefaultResourceGroupName].diskBytes)
	assert.Equal(t, usableCapacity.memoryBytes, capacityByRG[meta.DefaultResourceGroupName].memoryBytes)
	assert.Equal(t, usableCapacity.diskBytes, capacityByRG[meta.DefaultResourceGroupName].diskBytes)
}

func TestCurrentQueryNodeResourceStatusUsesQueryNodeDiskQuota(t *testing.T) {
	params := paramtable.Get()
	params.Save(params.QueryNodeCfg.MaxDiskUsagePercentage.Key, "95")
	defer params.Reset(params.QueryNodeCfg.MaxDiskUsagePercentage.Key)

	const gib = int64(1024 * 1024 * 1024)
	filesystemBytes := int64(3584) * gib
	workerUsableBytes := int64(1520) * gib
	workerUsedBytes := 17 * gib / 10
	workerAvailableBytes := workerUsableBytes - workerUsedBytes
	physicalUsedBytes := 1312 * gib / 10
	metricsPayload, err := metricsinfo.MarshalComponentInfos(metricsinfo.QueryNodeInfos{
		BaseComponentInfos: metricsinfo.BaseComponentInfos{
			HardwareInfos: metricsinfo.HardwareMetrics{
				Memory:      1024,
				MemoryUsage: 512,
				Disk:        float64(filesystemBytes) / 1e9,
				DiskUsage:   float64(physicalUsedBytes) / 1e9,
			},
		},
		LocalStorage: &metricsinfo.QueryNodeLocalStorageMetrics{
			CapacityBytes: 1600 * gib,
			UsedBytes:     workerUsedBytes,
		},
	})
	require.NoError(t, err)
	cluster := session.NewMockCluster(t)
	cluster.EXPECT().GetMetrics(mock.Anything, int64(1), mock.Anything).
		Return(&milvuspb.GetMetricsResponse{Status: merr.Success(), Response: metricsPayload}, nil).Once()
	server := newAutoscalePrecheckTestServer(t, cluster)

	availableByRG, capacityByRG, globalCapacity, err := server.currentQueryNodeResourceStatus(
		context.Background(),
		map[string]autoscaleResourceUsage{meta.DefaultResourceGroupName: {diskBytes: 1}},
		true,
	)

	require.NoError(t, err)
	assert.Equal(t, workerAvailableBytes, availableByRG[meta.DefaultResourceGroupName].diskBytes)
	assert.Equal(t, workerUsableBytes, capacityByRG[meta.DefaultResourceGroupName].diskBytes)
	assert.Equal(t, int64(1600)*gib, globalCapacity.diskBytes)
	assert.True(t, canExpandWithinGlobalLimit(
		autoscaleResourceUsage{diskBytes: 1000 * gib},
		globalCapacity,
		autoscaleResourceUsage{diskBytes: 3000 * gib},
	))
	assert.False(t, canExpandWithinGlobalLimit(
		autoscaleResourceUsage{diskBytes: 1000 * gib},
		globalCapacity,
		autoscaleResourceUsage{diskBytes: 2000 * gib},
	))
}

func TestCurrentQueryNodeResourceStatusUsesLocalBudgetWithoutPhysicalMinimum(t *testing.T) {
	params := paramtable.Get()
	params.Save(params.QueryNodeCfg.MaxDiskUsagePercentage.Key, "95")
	defer params.Reset(params.QueryNodeCfg.MaxDiskUsagePercentage.Key)

	const gib = int64(1024 * 1024 * 1024)
	const filesystemBytes = int64(1000) * gib
	physicalUsedBytes := int64(900) * gib
	metricsPayload, err := metricsinfo.MarshalComponentInfos(metricsinfo.QueryNodeInfos{
		BaseComponentInfos: metricsinfo.BaseComponentInfos{
			HardwareInfos: metricsinfo.HardwareMetrics{
				Memory:      1024,
				MemoryUsage: 512,
				Disk:        float64(filesystemBytes) / 1e9,
				DiskUsage:   float64(physicalUsedBytes) / 1e9,
			},
		},
		LocalStorage: &metricsinfo.QueryNodeLocalStorageMetrics{
			CapacityBytes: 1600 * gib,
			UsedBytes:     2 * gib,
		},
	})
	require.NoError(t, err)
	cluster := session.NewMockCluster(t)
	cluster.EXPECT().GetMetrics(mock.Anything, int64(1), mock.Anything).
		Return(&milvuspb.GetMetricsResponse{Status: merr.Success(), Response: metricsPayload}, nil).Once()
	server := newAutoscalePrecheckTestServer(t, cluster)

	availableByRG, capacityByRG, _, err := server.currentQueryNodeResourceStatus(
		context.Background(),
		map[string]autoscaleResourceUsage{meta.DefaultResourceGroupName: {diskBytes: 1}},
		false,
	)

	require.NoError(t, err)
	assert.Equal(t, int64(1518)*gib, availableByRG[meta.DefaultResourceGroupName].diskBytes)
	assert.Equal(t, int64(1520)*gib, capacityByRG[meta.DefaultResourceGroupName].diskBytes)
}

func TestCurrentQueryNodeResourceStatusDoesNotRequireFilesystemDiskMetrics(t *testing.T) {
	const gib = int64(1024 * 1024 * 1024)
	metricsPayload, err := metricsinfo.MarshalComponentInfos(metricsinfo.QueryNodeInfos{
		BaseComponentInfos: metricsinfo.BaseComponentInfos{
			HardwareInfos: metricsinfo.HardwareMetrics{
				Memory:      1024,
				MemoryUsage: 512,
			},
		},
		LocalStorage: &metricsinfo.QueryNodeLocalStorageMetrics{
			CapacityBytes: 1600 * gib,
			UsedBytes:     2 * gib,
		},
	})
	require.NoError(t, err)
	cluster := session.NewMockCluster(t)
	cluster.EXPECT().GetMetrics(mock.Anything, int64(1), mock.Anything).
		Return(&milvuspb.GetMetricsResponse{Status: merr.Success(), Response: metricsPayload}, nil).Once()
	server := newAutoscalePrecheckTestServer(t, cluster)

	availableByRG, capacityByRG, globalCapacity, err := server.currentQueryNodeResourceStatus(
		context.Background(),
		map[string]autoscaleResourceUsage{meta.DefaultResourceGroupName: {diskBytes: 1}},
		true,
	)

	require.NoError(t, err)
	usableDiskBytes := usableResourceCapacity(autoscaleResourceUsage{diskBytes: 1600 * gib}).diskBytes
	assert.Equal(t, usableDiskBytes-2*gib, availableByRG[meta.DefaultResourceGroupName].diskBytes)
	assert.Equal(t, usableDiskBytes, capacityByRG[meta.DefaultResourceGroupName].diskBytes)
	assert.Equal(t, int64(1600)*gib, globalCapacity.diskBytes)
}

func TestQueryNodeResourceSnapshotSharedFilesystemUsesLocalStorageQuotas(t *testing.T) {
	params := paramtable.Get()
	require.NoError(t, params.Save(params.QueryNodeCfg.MaxDiskUsagePercentage.Key, "95"))
	t.Cleanup(func() { params.Reset(params.QueryNodeCfg.MaxDiskUsagePercentage.Key) })

	const gib = int64(1024 * 1024 * 1024)
	metricsPayload, err := metricsinfo.MarshalComponentInfos(metricsinfo.QueryNodeInfos{
		BaseComponentInfos: metricsinfo.BaseComponentInfos{
			HardwareInfos: metricsinfo.HardwareMetrics{
				Memory:      1024,
				MemoryUsage: 512,
				Disk:        1000,
				DiskUsage:   500,
			},
		},
		LocalStorage: &metricsinfo.QueryNodeLocalStorageMetrics{
			CapacityBytes: 300 * gib,
			UsedBytes:     10 * gib,
		},
	})
	require.NoError(t, err)

	cluster := session.NewMockCluster(t)
	nodes := make([]*session.NodeInfo, 0, 3)
	for nodeID := int64(1); nodeID <= 3; nodeID++ {
		nodes = append(nodes, session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: nodeID}))
		cluster.EXPECT().GetMetrics(mock.Anything, nodeID, mock.Anything).
			Return(&milvuspb.GetMetricsResponse{Status: merr.Success(), Response: metricsPayload}, nil).Once()
	}
	server := &Server{cluster: cluster}

	snapshot, err := server.queryNodeResourceSnapshot(context.Background(), nodes)
	require.NoError(t, err)
	available, capacity, ok := queryNodeResourceStatusFromSnapshot(nodes, snapshot)
	require.True(t, ok)
	assert.Equal(t, int64(3*275)*gib, available.diskBytes)
	assert.Equal(t, int64(3*300)*gib, capacity.diskBytes)
}

func TestCurrentQueryNodeResourceStatusByRGUsesActiveResourceGroupNodes(t *testing.T) {
	params := paramtable.Get()
	params.Save(params.QueryCoordCfg.AutoscaleEnabled.Key, "false")
	defer params.Reset(params.QueryCoordCfg.AutoscaleEnabled.Key)

	const (
		node1Host = "task-2-active-rg-node-1"
		node2Host = "task-2-stopping-rg-node-2"
		node3Host = "task-2-online-outsider-node-3"
	)
	nodeManager := session.NewNodeManager()
	nodeManager.Add(session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: 1, Hostname: node1Host}))
	nodeManager.Add(session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: 2, Hostname: node2Host}))
	nodeManager.Add(session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: 3, Hostname: node3Host}))
	catalog := metastoremocks.NewQueryCoordCatalog(t)
	catalog.EXPECT().
		SaveResourceGroup(mock.Anything, mock.Anything).
		Return(nil).
		Twice()
	metaManager := meta.NewMeta(nil, catalog, nodeManager, metacache.NewMetaStore(nil))
	metaManager.HandleNodeUp(context.Background(), 1)
	metaManager.HandleNodeUp(context.Background(), 2)
	nodeManager.Stopping(2)

	metricsPayload := newAutoscalePrecheckMetrics(t, 1024, 512, 1)
	cluster := session.NewMockCluster(t)
	cluster.EXPECT().
		GetMetrics(mock.Anything, int64(1), mock.Anything).
		Return(&milvuspb.GetMetricsResponse{Status: merr.Success(), Response: metricsPayload}, nil).
		Once()
	server := &Server{
		meta:    metaManager,
		nodeMgr: nodeManager,
		cluster: cluster,
	}

	rawMemory, capacityMemory := memoryResourceFromMetrics(1024, 512)
	usableCapacity := usableResourceCapacity(autoscaleResourceUsage{memoryBytes: capacityMemory, diskBytes: 1e9})
	availableByRG, capacityByRG, _, err := server.currentQueryNodeResourceStatus(
		context.Background(),
		map[string]autoscaleResourceUsage{meta.DefaultResourceGroupName: {memoryBytes: 1}},
		false,
	)

	require.NoError(t, err)
	assert.Equal(t, rawMemory, availableByRG[meta.DefaultResourceGroupName].memoryBytes)
	assert.Equal(t, usableCapacity.diskBytes, availableByRG[meta.DefaultResourceGroupName].diskBytes)
	assert.Equal(t, usableCapacity.memoryBytes, capacityByRG[meta.DefaultResourceGroupName].memoryBytes)
	assert.Equal(t, usableCapacity.diskBytes, capacityByRG[meta.DefaultResourceGroupName].diskBytes)
}

func TestCheckLoadResourceSkipsEstimateDependencyErrors(t *testing.T) {
	enableAutoscalePrecheckForTest(t)

	tests := []struct {
		name        string
		recoveryErr error
		segmentErr  error
		indexErr    error
	}{
		{
			name:        "recovery info error",
			recoveryErr: errors.New("recovery info unavailable"),
		},
		{
			name:       "segment info error",
			segmentErr: errors.New("segment info unavailable"),
		},
		{
			name:     "index info error",
			indexErr: errors.New("index info unavailable"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := &Server{broker: newAutoscalePrecheckTestBroker(t, test.recoveryErr, test.segmentErr, test.indexErr)}

			requiredByRG, err := server.checkLoadResource(context.Background(), newAutoscalePrecheckTestRequest())

			require.NoError(t, err)
			assert.Empty(t, requiredByRG)
		})
	}
}

func TestCheckLoadResourceSkipsEstimateError(t *testing.T) {
	enableAutoscalePrecheckForTest(t)

	req := newAutoscalePrecheckTestRequest()
	req.CollectionInfo.Schema = nil
	server := &Server{broker: newAutoscalePrecheckTestBroker(t, nil, nil, nil)}
	memoryCounter := metrics.QueryCoordLoadDemandMemoryBytes.WithLabelValues(meta.DefaultResourceGroupName, metrics.LoadDemandSourceLoad)
	diskCounter := metrics.QueryCoordLoadDemandDiskBytes.WithLabelValues(meta.DefaultResourceGroupName, metrics.LoadDemandSourceLoad)
	memoryBefore := testutil.ToFloat64(memoryCounter)
	diskBefore := testutil.ToFloat64(diskCounter)

	requiredByRG, err := server.checkLoadResource(context.Background(), req)
	require.NoError(t, err)
	assert.Empty(t, requiredByRG)
	assert.Equal(t, memoryBefore, testutil.ToFloat64(memoryCounter))
	assert.Equal(t, diskBefore, testutil.ToFloat64(diskCounter))
}

func TestCheckLoadResourceDisabledSkipsPrecheck(t *testing.T) {
	params := paramtable.Get()
	params.Save(params.QueryCoordCfg.AutoscalePrecheckEnabled.Key, "false")
	t.Cleanup(func() {
		params.Reset(params.QueryCoordCfg.AutoscalePrecheckEnabled.Key)
	})

	server := &Server{}
	requiredByRG, err := server.checkLoadResource(context.Background(), newAutoscalePrecheckTestRequest())
	require.NoError(t, err)
	assert.Empty(t, requiredByRG)
}

func TestRecordLoadResourceDemand(t *testing.T) {
	const rgName = "load-demand-metric-test"
	metrics.QueryCoordLoadDemandMemoryBytes.DeleteLabelValues(rgName, metrics.LoadDemandSourceLoad)
	metrics.QueryCoordLoadDemandDiskBytes.DeleteLabelValues(rgName, metrics.LoadDemandSourceLoad)
	memoryCounter := metrics.QueryCoordLoadDemandMemoryBytes.WithLabelValues(rgName, metrics.LoadDemandSourceLoad)
	diskCounter := metrics.QueryCoordLoadDemandDiskBytes.WithLabelValues(rgName, metrics.LoadDemandSourceLoad)
	t.Cleanup(func() {
		metrics.QueryCoordLoadDemandMemoryBytes.DeleteLabelValues(rgName, metrics.LoadDemandSourceLoad)
		metrics.QueryCoordLoadDemandDiskBytes.DeleteLabelValues(rgName, metrics.LoadDemandSourceLoad)
	})

	recordLoadResourceDemand(metrics.LoadDemandSourceLoad, map[string]autoscaleResourceUsage{
		rgName: {memoryBytes: 100, diskBytes: 80},
	})
	recordLoadResourceDemand(metrics.LoadDemandSourceLoad, map[string]autoscaleResourceUsage{
		rgName: {memoryBytes: 20},
	})

	assert.Equal(t, float64(120), testutil.ToFloat64(memoryCounter))
	assert.Equal(t, float64(80), testutil.ToFloat64(diskCounter))
}

func TestRecordImportLoadDemandUsesLoadedPartitionsAndReplicas(t *testing.T) {
	enableAutoscalePrecheckForTest(t)
	setUsageParameter(t, &paramtable.Get().QueryNodeCfg.MmapScalarField, "false")
	ctx := t.Context()
	const (
		rgA = "import-demand-a"
		rgB = "import-demand-b"
	)
	for _, rg := range []string{rgA, rgB} {
		t.Cleanup(func() { metrics.DeleteQueryCoordLoadDemand(rg) })
	}
	collection := newAutoscalePrecheckTestRequest().CollectionInfo
	collection.VChannelNames = []string{"v0"}
	configs, err := loadmgr.RecoverLoadConfigStore(ctx, loadmgrtest.Dependencies(t))
	require.NoError(t, err)
	require.NoError(t, configs.Put(ctx, &loadmgr.LoadConfig{
		CollectionID: collection.ID,
		PartitionIDs: []int64{10},
		LoadFields:   []*viewpb.LoadFieldConfig{{FieldId: 100}},
		Replicas: []*loadmgr.ReplicaAssignment{
			{ReplicaID: 1, ResourceGroup: rgA},
			{ReplicaID: 2, ResourceGroup: rgA},
			{ReplicaID: 3, ResourceGroup: rgB},
		},
	}))
	broker := meta.NewMockBroker(t)
	broker.EXPECT().GetSegmentInfo(mock.Anything, int64(20), int64(21)).Return([]*datapb.SegmentInfo{
		{
			ID: 20, PartitionID: 10, NumOfRows: 1,
			Binlogs: []*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{MemorySize: 1024}}}},
		},
		{
			ID: 21, PartitionID: 11, NumOfRows: 1,
			Binlogs: []*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{MemorySize: 4096}}}},
		},
	}, nil).Once()
	broker.EXPECT().GetIndexInfo(mock.Anything, collection.ID, int64(20)).Return(nil, nil).Once()
	broker.EXPECT().GetSegmentInfo(mock.Anything, int64(22)).Return(nil, context.DeadlineExceeded).Once()

	stats := map[qviews.ShardID]*coordview.ShardStats{}
	markUp := func(replicaID int64) {
		stats[qviews.ShardID{ReplicaID: replicaID, VChannel: "v0"}] = &coordview.ShardStats{
			UpVersion:         &qviews.QueryViewVersion{DataVersion: qviews.DataVersion{StreamingVersion: 1}},
			UpLoadInfoVersion: configs.GetConfigVersion(collection.ID),
		}
	}
	patch := mockey.Mock((*coordview.ShardViewRegistry).SnapshotForShards).
		To(func(_ *coordview.ShardViewRegistry, _ []qviews.ShardID) *coordview.ShardViewSnapshot {
			return coordview.NewShardViewSnapshot(1, stats)
		}).Build()
	defer patch.UnPatch()
	server := &Server{
		broker: broker,
		meta:   newViewMeta(collection),
		qviewsRuntime: &qviewsRuntime{
			loadConfigStore:   configs,
			shardViewRegistry: &coordview.ShardViewRegistry{},
		},
	}

	// A collection with a config and only one of its three replicas Up is
	// still loading. It must not request segment or index metadata.
	markUp(1)
	require.Nil(t, server.GetImportLoadDemandRecorder(ctx, collection.ID))
	markUp(2)
	markUp(3)
	record := server.GetImportLoadDemandRecorder(ctx, collection.ID)
	require.NotNil(t, record)
	record([]int64{20, 21})
	record = server.GetImportLoadDemandRecorder(ctx, collection.ID)
	require.NotNil(t, record)
	record([]int64{22})
	require.Nil(t, server.GetImportLoadDemandRecorder(ctx, collection.ID+1))
	configs.StopPlanning(collection.ID)
	require.Nil(t, server.GetImportLoadDemandRecorder(ctx, collection.ID))
	assert.Equal(t, float64(2048), testutil.ToFloat64(metrics.QueryCoordLoadDemandMemoryBytes.WithLabelValues(rgA, metrics.LoadDemandSourceImport)))
	assert.Equal(t, float64(1024), testutil.ToFloat64(metrics.QueryCoordLoadDemandMemoryBytes.WithLabelValues(rgB, metrics.LoadDemandSourceImport)))
	assert.Equal(t, float64(0), testutil.ToFloat64(metrics.QueryCoordLoadDemandMemoryBytes.WithLabelValues(rgA, metrics.LoadDemandSourceLoad)))
}

func TestCheckLoadResourceSkipsResourceGroupMetricsErrors(t *testing.T) {
	enableAutoscalePrecheckForTest(t)

	params := paramtable.Get()
	params.Save(params.QueryCoordCfg.AutoscaleEnabled.Key, "false")
	defer params.Reset(params.QueryCoordCfg.AutoscaleEnabled.Key)

	validMetrics := newAutoscalePrecheckMetrics(t, 1024, 512, 1)
	tests := []struct {
		name string
		resp *milvuspb.GetMetricsResponse
		err  error
	}{
		{
			name: "rpc error",
			err:  errors.New("metrics unavailable"),
		},
		{
			name: "nil response",
		},
		{
			name: "status error",
			resp: &milvuspb.GetMetricsResponse{
				Status:   merr.Status(merr.ErrServiceNotReady),
				Response: validMetrics,
			},
		},
		{
			name: "malformed payload",
			resp: &milvuspb.GetMetricsResponse{
				Status:   merr.Success(),
				Response: "{",
			},
		},
		{
			name: "missing memory metrics",
			resp: &milvuspb.GetMetricsResponse{
				Status: merr.Success(),
				Response: `{
					"hardware_infos": {
						"disk": 1
					}
				}`,
			},
		},
		{
			name: "missing local storage metrics",
			resp: &milvuspb.GetMetricsResponse{
				Status: merr.Success(),
				Response: `{
					"hardware_infos": {
						"memory": 1024,
						"memory_usage": 512,
						"disk": 1
					}
				}`,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cluster := session.NewMockCluster(t)
			cluster.EXPECT().
				GetMetrics(mock.Anything, int64(1), mock.Anything).
				Return(test.resp, test.err).
				Once()
			server := newAutoscalePrecheckTestServer(t, cluster)
			server.broker = newAutoscalePrecheckTestBroker(t, nil, nil, nil)

			requiredByRG, err := server.checkLoadResource(context.Background(), newAutoscalePrecheckTestRequest())

			require.NoError(t, err)
			assert.Equal(t, map[string]autoscaleResourceUsage{
				meta.DefaultResourceGroupName: {memoryBytes: 1024},
			}, requiredByRG)
		})
	}
}

func TestCheckLoadResourceTieredPreservesDemandWithoutMetrics(t *testing.T) {
	enableAutoscalePrecheckForTest(t)
	params := paramtable.Get()
	require.NoError(t, params.Save(params.QueryNodeCfg.TieredEvictionEnabled.Key, "true"))
	require.NoError(t, params.Save(params.QueryNodeCfg.TieredEvictableMemoryCacheRatio.Key, "1"))
	require.NoError(t, params.Save(params.QueryNodeCfg.MmapScalarField.Key, "false"))
	t.Cleanup(func() {
		params.Reset(params.QueryNodeCfg.TieredEvictableMemoryCacheRatio.Key)
		params.Reset(params.QueryNodeCfg.MmapScalarField.Key)
	})
	cluster := session.NewMockCluster(t)
	server := newAutoscalePrecheckTestServer(t, cluster)
	server.broker = newAutoscalePrecheckTestBroker(t, nil, nil, nil)

	requiredByRG, err := server.checkLoadResource(context.Background(), newAutoscalePrecheckTestRequest())

	require.NoError(t, err)
	assert.Equal(t, map[string]autoscaleResourceUsage{
		meta.DefaultResourceGroupName: {memoryBytes: 1024},
	}, requiredByRG)
	cluster.AssertNotCalled(t, "GetMetrics", mock.Anything, mock.Anything, mock.Anything)
}

func TestCheckLoadResourceSkipsMetricsTimeout(t *testing.T) {
	enableAutoscalePrecheckForTest(t)
	cluster := session.NewMockCluster(t)
	cluster.EXPECT().GetMetrics(mock.Anything, int64(1), mock.Anything).
		RunAndReturn(func(ctx context.Context, _ int64, _ *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
			deadline, ok := ctx.Deadline()
			assert.True(t, ok)
			assert.LessOrEqual(t, time.Until(deadline), 10*time.Second)
			<-ctx.Done()
			return nil, ctx.Err()
		}).Once()
	server := newAutoscalePrecheckTestServer(t, cluster)
	server.broker = newAutoscalePrecheckTestBroker(t, nil, nil, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	requiredByRG, err := server.checkLoadResource(ctx, newAutoscalePrecheckTestRequest())

	require.NoError(t, err)
	assert.Equal(t, map[string]autoscaleResourceUsage{
		meta.DefaultResourceGroupName: {memoryBytes: 1024},
	}, requiredByRG)
	assert.NoError(t, ctx.Err(), "metrics collection should time out before the load request")
}

func TestCheckLoadResourceSkipsUnavailableResourceGroupNodes(t *testing.T) {
	enableAutoscalePrecheckForTest(t)
	params := paramtable.Get()
	require.NoError(t, params.Save(params.QueryCoordCfg.AutoscaleEnabled.Key, "false"))
	t.Cleanup(func() { params.Reset(params.QueryCoordCfg.AutoscaleEnabled.Key) })

	for _, stopping := range []bool{false, true} {
		name := "offline"
		if stopping {
			name = "stopping"
		}
		t.Run(name, func(t *testing.T) {
			cluster := session.NewMockCluster(t)
			server := newAutoscalePrecheckTestServer(t, cluster)
			server.broker = newAutoscalePrecheckTestBroker(t, nil, nil, nil)
			if stopping {
				server.nodeMgr.Stopping(1)
			} else {
				server.nodeMgr.Remove(1)
			}

			requiredByRG, err := server.checkLoadResource(context.Background(), newAutoscalePrecheckTestRequest())

			require.NoError(t, err)
			assert.Equal(t, map[string]autoscaleResourceUsage{
				meta.DefaultResourceGroupName: {memoryBytes: 1024},
			}, requiredByRG)
			cluster.AssertNotCalled(t, "GetMetrics", mock.Anything, mock.Anything, mock.Anything)
		})
	}
}

func TestCheckLoadResourceSkipsGlobalMetricsError(t *testing.T) {
	enableAutoscalePrecheckForTest(t)

	params := paramtable.Get()
	params.Save(params.QueryCoordCfg.AutoscaleEnabled.Key, "true")
	defer params.Reset(params.QueryCoordCfg.AutoscaleEnabled.Key)
	params.Save(params.QueryCoordCfg.AutoscaleMaxMemoryLimit.Key, "1")
	defer params.Reset(params.QueryCoordCfg.AutoscaleMaxMemoryLimit.Key)
	params.Save(params.QueryCoordCfg.AutoscaleMaxDiskLimit.Key, "1")
	defer params.Reset(params.QueryCoordCfg.AutoscaleMaxDiskLimit.Key)

	validMetrics := newAutoscalePrecheckMetrics(t, 1024, 1024, 1)
	cluster := session.NewMockCluster(t)
	cluster.EXPECT().
		GetMetrics(mock.Anything, int64(1), mock.Anything).
		Return(&milvuspb.GetMetricsResponse{Status: merr.Success(), Response: validMetrics}, nil).
		Once()
	cluster.EXPECT().
		GetMetrics(mock.Anything, int64(2), mock.Anything).
		Return(nil, errors.New("global metrics unavailable")).
		Once()
	server := newAutoscalePrecheckTestServer(t, cluster)
	server.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: 2}))
	server.broker = newAutoscalePrecheckTestBroker(t, nil, nil, nil)

	requiredByRG, err := server.checkLoadResource(context.Background(), newAutoscalePrecheckTestRequest())

	require.NoError(t, err)
	assert.Equal(t, map[string]autoscaleResourceUsage{
		meta.DefaultResourceGroupName: {memoryBytes: 1024},
	}, requiredByRG)
}

func TestCheckLoadResourceReturnsAcceptedDemandWithoutRecording(t *testing.T) {
	enableAutoscalePrecheckForTest(t)

	params := paramtable.Get()
	params.Save(params.QueryCoordCfg.AutoscaleEnabled.Key, "false")
	defer params.Reset(params.QueryCoordCfg.AutoscaleEnabled.Key)

	metricsPayload := newAutoscalePrecheckMetrics(t, 4096, 0, 1)
	cluster := session.NewMockCluster(t)
	cluster.EXPECT().
		GetMetrics(mock.Anything, int64(1), mock.Anything).
		Return(&milvuspb.GetMetricsResponse{Status: merr.Success(), Response: metricsPayload}, nil).
		Once()
	server := newAutoscalePrecheckTestServer(t, cluster)
	server.broker = newAutoscalePrecheckTestBroker(t, nil, nil, nil)
	memoryCounter := metrics.QueryCoordLoadDemandMemoryBytes.WithLabelValues(meta.DefaultResourceGroupName, metrics.LoadDemandSourceLoad)
	diskCounter := metrics.QueryCoordLoadDemandDiskBytes.WithLabelValues(meta.DefaultResourceGroupName, metrics.LoadDemandSourceLoad)
	memoryBefore := testutil.ToFloat64(memoryCounter)
	diskBefore := testutil.ToFloat64(diskCounter)

	requiredByRG, err := server.checkLoadResource(context.Background(), newAutoscalePrecheckTestRequest())

	require.NoError(t, err)
	require.Contains(t, requiredByRG, meta.DefaultResourceGroupName)
	assert.Equal(t, int64(1024), requiredByRG[meta.DefaultResourceGroupName].memoryBytes)
	assert.Zero(t, requiredByRG[meta.DefaultResourceGroupName].diskBytes)
	assert.Equal(t, memoryBefore, testutil.ToFloat64(memoryCounter))
	assert.Zero(t, testutil.ToFloat64(diskCounter)-diskBefore)
}

func TestCheckLoadResourcePreservesResourceInsufficientDecision(t *testing.T) {
	enableAutoscalePrecheckForTest(t)

	params := paramtable.Get()
	params.Save(params.QueryCoordCfg.AutoscaleEnabled.Key, "false")
	defer params.Reset(params.QueryCoordCfg.AutoscaleEnabled.Key)

	metricsPayload := newAutoscalePrecheckMetrics(t, 1024, 1024, 1)
	cluster := session.NewMockCluster(t)
	cluster.EXPECT().
		GetMetrics(mock.Anything, int64(1), mock.Anything).
		Return(&milvuspb.GetMetricsResponse{Status: merr.Success(), Response: metricsPayload}, nil).
		Once()
	server := newAutoscalePrecheckTestServer(t, cluster)
	server.broker = newAutoscalePrecheckTestBroker(t, nil, nil, nil)
	memoryCounter := metrics.QueryCoordLoadDemandMemoryBytes.WithLabelValues(meta.DefaultResourceGroupName, metrics.LoadDemandSourceLoad)
	diskCounter := metrics.QueryCoordLoadDemandDiskBytes.WithLabelValues(meta.DefaultResourceGroupName, metrics.LoadDemandSourceLoad)
	memoryBefore := testutil.ToFloat64(memoryCounter)
	diskBefore := testutil.ToFloat64(diskCounter)

	requiredByRG, err := server.checkLoadResource(context.Background(), newAutoscalePrecheckTestRequest())

	require.Error(t, err)
	assert.Empty(t, requiredByRG)
	var precheckErr *loadResourcePrecheckError
	require.ErrorAs(t, err, &precheckErr)
	status := statusWithLoadResourcePrecheckSuggestion(err)
	assert.Equal(t, merr.Code(merr.ErrServiceResourceInsufficient), status.GetCode())
	assert.False(t, status.GetRetriable())
	assert.Contains(t, status.GetExtraInfo(), loadResourcePrecheckSuggestKey)
	assert.Equal(t, memoryBefore, testutil.ToFloat64(memoryCounter))
	assert.Equal(t, diskBefore, testutil.ToFloat64(diskCounter))
}

func TestQueryNodeResourceSnapshotUsesStaticCapacityAndReturnsMetricsError(t *testing.T) {
	params := paramtable.Get()
	params.Save(params.QuotaConfig.QueryNodeMemoryLowWaterLevel.Key, "0.4")
	defer params.Reset(params.QuotaConfig.QueryNodeMemoryLowWaterLevel.Key)
	params.Save(params.QuotaConfig.QueryNodeMemoryHighWaterLevel.Key, "0.5")
	defer params.Reset(params.QuotaConfig.QueryNodeMemoryHighWaterLevel.Key)

	node := session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: 1})
	node.UpdateStats(session.WithMemCapacity(1024))
	nodes := []*session.NodeInfo{node}

	server := &Server{}
	snapshot, err := server.queryNodeResourceSnapshot(context.Background(), nodes)
	require.NoError(t, err)
	_, staticCapacity, ok := queryNodeResourceStatusFromSnapshot(nodes, snapshot)
	require.True(t, ok)
	assert.Positive(t, staticCapacity.memoryBytes)

	cluster := session.NewMockCluster(t)
	cluster.EXPECT().
		GetMetrics(mock.Anything, int64(1), mock.Anything).
		Return(nil, errors.New("metrics unavailable")).
		Once()
	server.cluster = cluster

	_, err = server.queryNodeResourceSnapshot(context.Background(), nodes)
	require.Error(t, err)
}

func TestCurrentQueryNodeGlobalCapacityUsesOnlineNodes(t *testing.T) {
	params := paramtable.Get()
	params.Save(params.QuotaConfig.QueryNodeMemoryLowWaterLevel.Key, "0.4")
	defer params.Reset(params.QuotaConfig.QueryNodeMemoryLowWaterLevel.Key)
	params.Save(params.QuotaConfig.QueryNodeMemoryHighWaterLevel.Key, "0.5")
	defer params.Reset(params.QuotaConfig.QueryNodeMemoryHighWaterLevel.Key)
	params.Save(params.QueryNodeCfg.DiskCapacityLimit.Key, "1")
	defer params.Reset(params.QueryNodeCfg.DiskCapacityLimit.Key)

	nodeManager := session.NewNodeManager()
	node1 := session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: 1})
	node1.UpdateStats(session.WithMemCapacity(1024))
	nodeManager.Add(node1)
	node2 := session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: 2})
	node2.UpdateStats(session.WithMemCapacity(2048))
	nodeManager.Add(node2)
	nodeManager.Stopping(2)
	node3 := session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: 3})
	node3.UpdateStats(session.WithMemCapacity(512))
	nodeManager.Add(node3)
	server := &Server{nodeMgr: nodeManager}

	_, _, capacity, err := server.currentQueryNodeResourceStatus(context.Background(), nil, true)

	require.NoError(t, err)
	assert.Equal(t, int64((1024+512)*1024*1024), capacity.memoryBytes)
	assert.Equal(t, int64(2*bytesPerGB), capacity.diskBytes)
}

func TestCurrentQueryNodeResourceStatusReusesSnapshotForGlobalCapacity(t *testing.T) {
	metricsPayload := newAutoscalePrecheckMetrics(t, 1024, 512, 1)
	cluster := session.NewMockCluster(t)
	cluster.EXPECT().
		GetMetrics(mock.MatchedBy(func(ctx context.Context) bool {
			return retry.MaxAttemptsFromContextOrDefault(ctx, 10) == 1
		}), int64(1), mock.Anything).
		Return(&milvuspb.GetMetricsResponse{Status: merr.Success(), Response: metricsPayload}, nil).
		Once()
	server := newAutoscalePrecheckTestServer(t, cluster)
	for i, label := range []string{
		sessionutil.LabelStreamingNodeEmbeddedQueryNode,
		sessionutil.LegacyLabelStreamingNodeEmbeddedQueryNode,
	} {
		server.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
			NodeID: int64(i + 2),
			Labels: map[string]string{label: "1"},
		}))
	}

	_, capacityByRG, globalCapacity, err := server.currentQueryNodeResourceStatus(
		context.Background(),
		map[string]autoscaleResourceUsage{meta.DefaultResourceGroupName: {memoryBytes: 1}},
		true,
	)

	require.NoError(t, err)
	assert.Equal(t, autoscaleResourceUsage{memoryBytes: 1024, diskBytes: 1_000_000_000}, globalCapacity)
	cluster.AssertNotCalled(t, "GetMetrics", mock.Anything, int64(2), mock.Anything)
	cluster.AssertNotCalled(t, "GetMetrics", mock.Anything, int64(3), mock.Anything)
	assert.Equal(t, capacityByRG[meta.DefaultResourceGroupName], usableResourceCapacity(globalCapacity))
}

func TestQueryNodeResourceSnapshotFetchesMetricsConcurrently(t *testing.T) {
	metricsPayload := newAutoscalePrecheckMetrics(t, 1024, 512, 1)
	started := make(chan struct{}, 2)
	attempts := make(chan uint, 2)
	release := make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()

	cluster := session.NewMockCluster(t)
	cluster.EXPECT().
		GetMetrics(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, _ int64, _ *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
			attempts <- retry.MaxAttemptsFromContextOrDefault(ctx, 10)
			started <- struct{}{}
			<-release
			return &milvuspb.GetMetricsResponse{Status: merr.Success(), Response: metricsPayload}, nil
		}).
		Twice()
	server := &Server{cluster: cluster}
	nodes := []*session.NodeInfo{
		session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: 1}),
		session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: 2}),
	}
	type snapshotResult struct {
		snapshot map[int64]queryNodeResourceStatus
		err      error
	}
	resultCh := make(chan snapshotResult, 1)
	go func() {
		snapshot, err := server.queryNodeResourceSnapshot(context.Background(), nodes)
		resultCh <- snapshotResult{snapshot: snapshot, err: err}
	}()

	for range 2 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("query node metrics requests did not run concurrently")
		}
	}
	close(release)
	released = true
	for range 2 {
		assert.Equal(t, uint(1), <-attempts)
	}
	select {
	case result := <-resultCh:
		require.NoError(t, result.err)
		assert.Len(t, result.snapshot, 2)
	case <-time.After(time.Second):
		t.Fatal("query node metrics snapshot did not complete")
	}
}

func TestAutoscalePrecheckStatusExtraInfo(t *testing.T) {
	err := &loadResourcePrecheckError{
		message:                "Insufficient cluster resources. Please scale out the cluster or enable autoscaling. Suggested scale-out ratio: 75%",
		suggestedExpandPercent: 75,
	}

	status := statusWithLoadResourcePrecheckSuggestion(err)

	assert.Equal(t, merr.Code(merr.ErrServiceResourceInsufficient), status.GetCode())
	assert.Equal(t, "Insufficient cluster resources. Please scale out the cluster or enable autoscaling. Suggested scale-out ratio: 75%", status.GetReason())
	require.NotNil(t, status.GetExtraInfo())
	assert.Equal(t, "75", status.GetExtraInfo()["suggested_expand_percent"])
	assert.NotContains(t, status.GetExtraInfo(), "memory_bytes")
	assert.NotContains(t, status.GetExtraInfo(), "disk_bytes")
}

func TestRequiredLoadResourceUsesProjectedIndexResources(t *testing.T) {
	req := newAutoscalePrecheckTestRequest()
	req.CollectionInfo.Schema.Fields[0].DataType = schemapb.DataType_FloatVector
	req.CollectionInfo.Schema.Fields[0].TypeParams = []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "128"}}
	id := autoscalePrecheckTestSegmentID
	segment := &datapb.SegmentInfo{
		ID: id, CollectionID: req.CollectionInfo.ID,
		PartitionID: autoscalePrecheckTestPartitionID, StorageVersion: storage.StorageV3, NumOfRows: 8000,
		ManifestPath: "unavailable", Stats: &datapb.Statistics{LoadResource: &datapb.LoadResourceStatistics{
			ColumnGroups: []*datapb.ColumnGroupStatistics{{GroupId: 100, FieldIds: []int64{100}, MemorySize: 512000}},
		}},
	}
	for _, artifact := range []bool{true, false} {
		for _, loadFields := range [][]int64{nil, {101}} {
			req.Expected.ExpectedLoadFields = loadFields
			broker := meta.NewMockBroker(t)
			broker.EXPECT().GetRecoveryInfoV2(mock.Anything, segment.GetCollectionID(), segment.GetPartitionID()).
				Return(nil, []*datapb.SegmentInfo{segment}, nil).Once()
			index := &querypb.FieldIndexInfo{
				FieldID: 100, IndexID: 5, BuildID: 6, IndexSize: 1024 * 1024, NumRows: 4000, HasArtifact: &artifact,
				IndexParams: []*commonpb.KeyValuePair{
					{Key: common.IndexTypeKey, Value: "DISKANN"},
					{Key: common.MetricTypeKey, Value: "L2"},
					{Key: common.DimKey, Value: "128"},
				},
			}
			broker.EXPECT().GetSegmentInfoForResourceEstimation(mock.Anything, segment.GetCollectionID(), id).
				Return([]*datapb.SegmentInfo{segment}, map[int64][]*querypb.FieldIndexInfo{id: {index}}, nil).Once()
			got, err := (&Server{broker: broker}).requiredLoadResourceByRG(t.Context(), req)
			require.NoError(t, err)

			legacy := typeutil.Clone(index)
			legacy.HasArtifact = nil
			if artifact {
				legacy.IndexFilePaths = []string{"unavailable/index_file"}
			}
			want, err := estimateLoadResourceForLoadConfig(t.Context(), req.CollectionInfo.Schema, nil,
				[]*datapb.SegmentInfo{segment}, map[int64][]*querypb.FieldIndexInfo{id: {legacy}}, loadFields)
			require.NoError(t, err)
			require.Equal(t, want, got[meta.DefaultResourceGroupName])
		}
	}
}

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

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	metastoremocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/querycoordv2/autoscale"
	"github.com/milvus-io/milvus/internal/querycoordv2/job"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
)

func TestAutoscalePrecheckDecision(t *testing.T) {
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

func TestAutoscalePrecheckIncludesInflightOvercommitInShortage(t *testing.T) {
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

func TestAutoscaleLoadResourceFiltersLoadFieldsAndIndexes(t *testing.T) {
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
			{FieldID: 100, IndexID: 10},
			{FieldID: 100, IndexID: 11},
			{FieldID: 101, IndexID: 20},
		},
	}

	filteredIndexes := autoscale.FilterIndexesByLoadConfig(indexes, []int64{100}, map[int64]int64{100: 11})
	require.Len(t, filteredIndexes[1], 1)
	assert.Equal(t, int64(11), filteredIndexes[1][0].GetIndexID())
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

func TestAvailableResourceFromMetricsUsesQueryNodeLimit(t *testing.T) {
	params := paramtable.Get()
	params.Save(params.QuotaConfig.QueryNodeMemoryLowWaterLevel.Key, "0.4")
	defer params.Reset(params.QuotaConfig.QueryNodeMemoryLowWaterLevel.Key)
	params.Save(params.QuotaConfig.QueryNodeMemoryHighWaterLevel.Key, "0.5")
	defer params.Reset(params.QuotaConfig.QueryNodeMemoryHighWaterLevel.Key)
	params.Save(params.QueryNodeCfg.DiskCapacityLimit.Key, "1")
	defer params.Reset(params.QueryNodeCfg.DiskCapacityLimit.Key)
	params.Save(params.QueryNodeCfg.MaxDiskUsagePercentage.Key, "50")
	defer params.Reset(params.QueryNodeCfg.MaxDiskUsagePercentage.Key)

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
		assert.Equal(t, int64(500), capacityMemoryBytes)
	}

	availableDiskBytes, capacityDiskBytes, ok := diskResourceFromMetrics(10, 0.1)
	require.True(t, ok)
	assert.Equal(t, int64(436870912), availableDiskBytes)
	assert.Equal(t, int64(536870912), capacityDiskBytes)

	params.Save(params.QueryNodeCfg.DiskCapacityLimit.Key, "0")
	availableDiskBytes, capacityDiskBytes, ok = diskResourceFromMetrics(10, 0.1)
	require.True(t, ok)
	assert.Zero(t, availableDiskBytes)
	assert.Zero(t, capacityDiskBytes)
}

const (
	autoscalePrecheckTestCollectionID = int64(100)
	autoscalePrecheckTestPartitionID  = int64(10)
	autoscalePrecheckTestSegmentID    = int64(1)
)

func newAutoscalePrecheckTestRequest() *job.AlterLoadConfigRequest {
	return &job.AlterLoadConfigRequest{
		CollectionInfo: &milvuspb.DescribeCollectionResponse{
			CollectionID: autoscalePrecheckTestCollectionID,
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

func enableAutoscalePrecheckForTest(t *testing.T) {
	t.Helper()
	params := paramtable.Get()
	params.Save(params.QueryCoordCfg.AutoscalePrecheckEnabled.Key, "true")
	t.Cleanup(func() {
		params.Reset(params.QueryCoordCfg.AutoscalePrecheckEnabled.Key)
	})
}

func newAutoscalePrecheckTestBroker(t *testing.T, recoveryErr, indexErr error) *meta.MockBroker {
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
				NumOfRows:   1,
				Binlogs: []*datapb.FieldBinlog{
					{FieldID: 100, Binlogs: []*datapb.Binlog{{MemorySize: 1024, LogSize: 1024}}},
				},
			},
		}, nil).
		Once()
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
	metaManager := meta.NewMeta(nil, catalog, nodeManager)
	metaManager.HandleNodeUp(context.Background(), 1)
	scheduler := task.NewMockScheduler(t)
	scheduler.EXPECT().
		GetAutoscaleInflightLoadResource(mock.Anything).
		Return(autoscale.ResourceUsage{}).
		Maybe()
	return &Server{
		meta:          metaManager,
		nodeMgr:       nodeManager,
		cluster:       cluster,
		taskScheduler: scheduler,
	}
}

func newAutoscalePrecheckMetrics(t *testing.T, memory, memoryUsage uint64, disk float64) string {
	t.Helper()
	metrics, err := metricsinfo.MarshalComponentInfos(metricsinfo.QueryNodeInfos{
		BaseComponentInfos: metricsinfo.BaseComponentInfos{
			HardwareInfos: metricsinfo.HardwareMetrics{
				Memory:      memory,
				MemoryUsage: memoryUsage,
				Disk:        disk,
			},
		},
	})
	require.NoError(t, err)
	return metrics
}

func TestCurrentQueryNodeResourceStatusByRGDeductsInflight(t *testing.T) {
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
	rawDisk, capacityDisk, ok := diskResourceFromMetrics(1, 0)
	require.True(t, ok)
	inflightMemory := rawMemory + 20
	inflightDisk := int64(10)
	scheduler := task.NewMockScheduler(t)
	scheduler.EXPECT().
		GetAutoscaleInflightLoadResource([]int64{1}).
		Return(autoscale.ResourceUsage{MemoryBytes: inflightMemory, DiskBytes: inflightDisk}).
		Once()
	server.taskScheduler = scheduler

	availableByRG, capacityByRG, _, err := server.currentQueryNodeResourceStatus(
		context.Background(),
		map[string]autoscaleResourceUsage{meta.DefaultResourceGroupName: {memoryBytes: 1}},
		false,
	)

	require.NoError(t, err)
	assert.Equal(t, int64(-20), availableByRG[meta.DefaultResourceGroupName].memoryBytes)
	assert.Equal(t, rawDisk-inflightDisk, availableByRG[meta.DefaultResourceGroupName].diskBytes)
	assert.Equal(t, capacityMemory, capacityByRG[meta.DefaultResourceGroupName].memoryBytes)
	assert.Equal(t, capacityDisk, capacityByRG[meta.DefaultResourceGroupName].diskBytes)
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
	metaManager := meta.NewMeta(nil, catalog, nodeManager)
	metaManager.HandleNodeUp(context.Background(), 1)
	metaManager.HandleNodeUp(context.Background(), 2)
	nodeManager.Stopping(2)

	metricsPayload := newAutoscalePrecheckMetrics(t, 1024, 512, 1)
	cluster := session.NewMockCluster(t)
	cluster.EXPECT().
		GetMetrics(mock.Anything, int64(1), mock.Anything).
		Return(&milvuspb.GetMetricsResponse{Status: merr.Success(), Response: metricsPayload}, nil).
		Once()
	scheduler := task.NewMockScheduler(t)
	scheduler.EXPECT().
		GetAutoscaleInflightLoadResource([]int64{1}).
		Return(autoscale.ResourceUsage{MemoryBytes: 120, DiskBytes: 80}).
		Once()
	server := &Server{
		meta:          metaManager,
		nodeMgr:       nodeManager,
		cluster:       cluster,
		taskScheduler: scheduler,
	}

	rawMemory, capacityMemory := memoryResourceFromMetrics(1024, 512)
	rawDisk, capacityDisk, ok := diskResourceFromMetrics(1, 0)
	require.True(t, ok)
	availableByRG, capacityByRG, _, err := server.currentQueryNodeResourceStatus(
		context.Background(),
		map[string]autoscaleResourceUsage{meta.DefaultResourceGroupName: {memoryBytes: 1}},
		false,
	)

	require.NoError(t, err)
	assert.Equal(t, rawMemory-120, availableByRG[meta.DefaultResourceGroupName].memoryBytes)
	assert.Equal(t, rawDisk-80, availableByRG[meta.DefaultResourceGroupName].diskBytes)
	assert.Equal(t, capacityMemory, capacityByRG[meta.DefaultResourceGroupName].memoryBytes)
	assert.Equal(t, capacityDisk, capacityByRG[meta.DefaultResourceGroupName].diskBytes)
}

func TestCurrentQueryNodeInflightLoadResourceUsesSelectedNodes(t *testing.T) {
	scheduler := task.NewMockScheduler(t)
	scheduler.EXPECT().
		GetAutoscaleInflightLoadResource([]int64{1, 2}).
		Return(autoscale.ResourceUsage{MemoryBytes: 150, DiskBytes: 100}).
		Once()
	server := &Server{taskScheduler: scheduler}
	nodes := []*session.NodeInfo{
		session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: 1}),
		session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: 2}),
	}

	usage := server.currentQueryNodeInflightLoadResource(nodes)

	assert.Equal(t, int64(150), usage.memoryBytes)
	assert.Equal(t, int64(100), usage.diskBytes)
}

func TestCheckLoadResourceSkipsEstimateDependencyErrors(t *testing.T) {
	enableAutoscalePrecheckForTest(t)

	tests := []struct {
		name        string
		recoveryErr error
		indexErr    error
	}{
		{
			name:        "recovery info error",
			recoveryErr: errors.New("recovery info unavailable"),
		},
		{
			name:     "index info error",
			indexErr: errors.New("index info unavailable"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := &Server{broker: newAutoscalePrecheckTestBroker(t, test.recoveryErr, test.indexErr)}

			err := server.checkLoadResource(context.Background(), newAutoscalePrecheckTestRequest())

			require.NoError(t, err)
		})
	}
}

func TestCheckLoadResourceSkipsEstimateError(t *testing.T) {
	enableAutoscalePrecheckForTest(t)

	req := newAutoscalePrecheckTestRequest()
	req.CollectionInfo.Schema = nil
	server := &Server{broker: newAutoscalePrecheckTestBroker(t, nil, nil)}

	require.NoError(t, server.checkLoadResource(context.Background(), req))
}

func TestCheckLoadResourceDisabledSkipsPrecheck(t *testing.T) {
	params := paramtable.Get()
	params.Save(params.QueryCoordCfg.AutoscalePrecheckEnabled.Key, "false")
	t.Cleanup(func() {
		params.Reset(params.QueryCoordCfg.AutoscalePrecheckEnabled.Key)
	})

	server := &Server{}
	require.NoError(t, server.checkLoadResource(context.Background(), newAutoscalePrecheckTestRequest()))
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
			name: "missing disk metrics",
			resp: &milvuspb.GetMetricsResponse{
				Status: merr.Success(),
				Response: `{
					"hardware_infos": {
						"memory": 1024,
						"memory_usage": 512
					}
				}`,
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
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cluster := session.NewMockCluster(t)
			cluster.EXPECT().
				GetMetrics(mock.Anything, int64(1), mock.Anything).
				Return(test.resp, test.err).
				Once()
			server := newAutoscalePrecheckTestServer(t, cluster)
			server.broker = newAutoscalePrecheckTestBroker(t, nil, nil)

			err := server.checkLoadResource(context.Background(), newAutoscalePrecheckTestRequest())

			require.NoError(t, err)
		})
	}
}

func TestCheckLoadResourceIncludesInflightOvercommit(t *testing.T) {
	enableAutoscalePrecheckForTest(t)

	params := paramtable.Get()
	params.Save(params.QueryCoordCfg.AutoscaleEnabled.Key, "false")
	defer params.Reset(params.QueryCoordCfg.AutoscaleEnabled.Key)

	metricsPayload := newAutoscalePrecheckMetrics(t, 4096, 0, 1)
	cluster := session.NewMockCluster(t)
	cluster.EXPECT().GetMetrics(mock.Anything, int64(1), mock.Anything).
		Return(&milvuspb.GetMetricsResponse{Status: merr.Success(), Response: metricsPayload}, nil).
		Once()
	server := newAutoscalePrecheckTestServer(t, cluster)
	server.broker = newAutoscalePrecheckTestBroker(t, nil, nil)

	rawMemory, _ := memoryResourceFromMetrics(4096, 0)
	scheduler := task.NewMockScheduler(t)
	scheduler.EXPECT().
		GetAutoscaleInflightLoadResource([]int64{1}).
		Return(autoscale.ResourceUsage{MemoryBytes: rawMemory}).
		Once()
	server.taskScheduler = scheduler

	err := server.checkLoadResource(context.Background(), newAutoscalePrecheckTestRequest())

	require.Error(t, err)
	var precheckErr *loadResourcePrecheckError
	require.ErrorAs(t, err, &precheckErr)
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
	server.broker = newAutoscalePrecheckTestBroker(t, nil, nil)

	err := server.checkLoadResource(context.Background(), newAutoscalePrecheckTestRequest())

	require.NoError(t, err)
}

func TestCheckLoadResourcePreservesResourceInsufficientDecision(t *testing.T) {
	enableAutoscalePrecheckForTest(t)

	params := paramtable.Get()
	params.Save(params.QueryCoordCfg.AutoscaleEnabled.Key, "false")
	defer params.Reset(params.QueryCoordCfg.AutoscaleEnabled.Key)

	metrics := newAutoscalePrecheckMetrics(t, 1024, 1024, 1)
	cluster := session.NewMockCluster(t)
	cluster.EXPECT().
		GetMetrics(mock.Anything, int64(1), mock.Anything).
		Return(&milvuspb.GetMetricsResponse{Status: merr.Success(), Response: metrics}, nil).
		Once()
	server := newAutoscalePrecheckTestServer(t, cluster)
	server.broker = newAutoscalePrecheckTestBroker(t, nil, nil)

	err := server.checkLoadResource(context.Background(), newAutoscalePrecheckTestRequest())

	require.Error(t, err)
	var precheckErr *loadResourcePrecheckError
	require.ErrorAs(t, err, &precheckErr)
	status := statusWithLoadResourcePrecheckSuggestion(err)
	assert.Equal(t, merr.Code(merr.ErrServiceResourceInsufficient), status.GetCode())
	assert.False(t, status.GetRetriable())
	assert.Contains(t, status.GetExtraInfo(), loadResourcePrecheckSuggestKey)
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
	params.Save(params.QueryNodeCfg.DiskCacheCapacityLimit.Key, "100")
	defer params.Reset(params.QueryNodeCfg.DiskCacheCapacityLimit.Key)

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
	assert.Equal(t, int64((1024+512)*1024*1024/2), capacity.memoryBytes)
	assert.Equal(t, int64(200), capacity.diskBytes)
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

	_, capacityByRG, globalCapacity, err := server.currentQueryNodeResourceStatus(
		context.Background(),
		map[string]autoscaleResourceUsage{meta.DefaultResourceGroupName: {memoryBytes: 1}},
		true,
	)

	require.NoError(t, err)
	assert.Equal(t, capacityByRG[meta.DefaultResourceGroupName], globalCapacity)
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

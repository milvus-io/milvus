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

package rootcoord

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	mockrootcoord "github.com/milvus-io/milvus/internal/rootcoord/mocks"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/ratelimitutil"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type dataCoordMockForQuota struct {
	mockDataCoord
	retErr        bool
	retFailStatus bool
}

func (d *dataCoordMockForQuota) GetMetrics(ctx context.Context, request *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	if d.retErr {
		return nil, fmt.Errorf("mock err")
	}
	if d.retFailStatus {
		return &milvuspb.GetMetricsResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "mock failure status"),
		}, nil
	}
	return &milvuspb.GetMetricsResponse{
		Status: succStatus(),
	}, nil
}

func TestQuotaCenter(t *testing.T) {
	Params.Init()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	core, err := NewCore(ctx, nil)
	assert.NoError(t, err)
	core.tsoAllocator = newMockTsoAllocator()

	pcm := newProxyClientManager(core.proxyCreator)

	t.Run("test QuotaCenter", func(t *testing.T) {
		qc := types.NewMockQueryCoord(t)
		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().GetCollectionByID(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, merr.ErrCollectionNotFound).Maybe()
		quotaCenter := NewQuotaCenter(pcm, qc, &dataCoordMockForQuota{}, core.tsoAllocator, meta)
		go quotaCenter.run()
		time.Sleep(10 * time.Millisecond)
		quotaCenter.stop()
	})

	t.Run("test syncMetrics", func(t *testing.T) {
		qc := types.NewMockQueryCoord(t)
		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().GetCollectionByID(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, merr.ErrCollectionNotFound).Maybe()
		qc.EXPECT().GetMetrics(mock.Anything, mock.Anything).Return(&milvuspb.GetMetricsResponse{Status: succStatus()}, nil)
		quotaCenter := NewQuotaCenter(pcm, qc, &dataCoordMockForQuota{}, core.tsoAllocator, meta)
		err = quotaCenter.syncMetrics()
		assert.Error(t, err) // for empty response

		quotaCenter = NewQuotaCenter(pcm, qc, &dataCoordMockForQuota{}, core.tsoAllocator, meta)
		err = quotaCenter.syncMetrics()
		assert.Error(t, err)

		quotaCenter = NewQuotaCenter(pcm, qc, &dataCoordMockForQuota{retFailStatus: true}, core.tsoAllocator, meta)
		err = quotaCenter.syncMetrics()
		assert.Error(t, err)

		qc.EXPECT().GetMetrics(mock.Anything, mock.Anything).Return(nil, fmt.Errorf("mock err"))
		quotaCenter = NewQuotaCenter(pcm, qc, &dataCoordMockForQuota{retErr: true}, core.tsoAllocator, meta)
		err = quotaCenter.syncMetrics()
		assert.Error(t, err)

		qc.EXPECT().GetMetrics(mock.Anything, mock.Anything).Return(&milvuspb.GetMetricsResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "mock failure status"),
		}, nil)
		quotaCenter = NewQuotaCenter(pcm, qc, &dataCoordMockForQuota{}, core.tsoAllocator, meta)
		err = quotaCenter.syncMetrics()
		assert.Error(t, err)
	})

	t.Run("test forceDeny", func(t *testing.T) {
		qc := types.NewMockQueryCoord(t)
		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().GetCollectionByID(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, merr.ErrCollectionNotFound).Maybe()
		quotaCenter := NewQuotaCenter(pcm, qc, &dataCoordMockForQuota{}, core.tsoAllocator, meta)
		quotaCenter.readableCollections = []int64{1, 2, 3}
		quotaCenter.resetAllCurrentRates()
		quotaCenter.forceDenyReading(commonpb.ErrorCode_ForceDeny, 1, 2, 3, 4)
		for _, collection := range quotaCenter.readableCollections {
			assert.Equal(t, Limit(0), quotaCenter.currentRates[collection][internalpb.RateType_DQLSearch])
			assert.Equal(t, Limit(0), quotaCenter.currentRates[collection][internalpb.RateType_DQLQuery])
		}
		assert.Equal(t, Limit(0), quotaCenter.currentRates[4][internalpb.RateType_DQLSearch])
		assert.Equal(t, Limit(0), quotaCenter.currentRates[4][internalpb.RateType_DQLQuery])

		quotaCenter.writableCollections = []int64{1, 2, 3}
		quotaCenter.resetAllCurrentRates()
		quotaCenter.forceDenyWriting(commonpb.ErrorCode_ForceDeny, 1, 2, 3, 4)
		for _, collection := range quotaCenter.writableCollections {
			assert.Equal(t, Limit(0), quotaCenter.currentRates[collection][internalpb.RateType_DMLInsert])
			assert.Equal(t, Limit(0), quotaCenter.currentRates[collection][internalpb.RateType_DMLDelete])
			assert.Equal(t, Limit(0), quotaCenter.currentRates[collection][internalpb.RateType_DMLBulkLoad])
		}
		assert.Equal(t, Limit(0), quotaCenter.currentRates[4][internalpb.RateType_DMLInsert])
		assert.Equal(t, Limit(0), quotaCenter.currentRates[4][internalpb.RateType_DMLDelete])
		assert.Equal(t, Limit(0), quotaCenter.currentRates[4][internalpb.RateType_DMLBulkLoad])
	})

	t.Run("test calculateRates", func(t *testing.T) {
		qc := types.NewMockQueryCoord(t)
		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().GetCollectionByID(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, merr.ErrCollectionNotFound).Maybe()
		quotaCenter := NewQuotaCenter(pcm, qc, &dataCoordMockForQuota{}, core.tsoAllocator, meta)
		err = quotaCenter.calculateRates()
		assert.NoError(t, err)
		alloc := newMockTsoAllocator()
		alloc.GenerateTSOF = func(count uint32) (typeutil.Timestamp, error) {
			return 0, fmt.Errorf("mock err")
		}
		quotaCenter.tsoAllocator = alloc
		err = quotaCenter.calculateRates()
		assert.Error(t, err)
	})

	t.Run("test getTimeTickDelayFactor factors", func(t *testing.T) {
		qc := types.NewMockQueryCoord(t)
		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().GetCollectionByID(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, merr.ErrCollectionNotFound).Maybe()
		quotaCenter := NewQuotaCenter(pcm, qc, &dataCoordMockForQuota{}, core.tsoAllocator, meta)
		type ttCase struct {
			maxTtDelay     time.Duration
			curTt          time.Time
			fgTt           time.Time
			expectedFactor float64
		}
		t0 := time.Now()
		ttCases := []ttCase{
			{10 * time.Second, t0, t0.Add(1 * time.Second), 1},
			{10 * time.Second, t0, t0, 1},
			{10 * time.Second, t0.Add(1 * time.Second), t0, 0.9},
			{10 * time.Second, t0.Add(2 * time.Second), t0, 0.8},
			{10 * time.Second, t0.Add(5 * time.Second), t0, 0.5},
			{10 * time.Second, t0.Add(7 * time.Second), t0, 0.3},
			{10 * time.Second, t0.Add(9 * time.Second), t0, 0.1},
			{10 * time.Second, t0.Add(10 * time.Second), t0, 0},
			{10 * time.Second, t0.Add(100 * time.Second), t0, 0},
		}

		backup := Params.QuotaConfig.MaxTimeTickDelay

		for _, c := range ttCases {
			paramtable.Get().Save(Params.QuotaConfig.MaxTimeTickDelay.Key, fmt.Sprintf("%f", c.maxTtDelay.Seconds()))
			fgTs := tsoutil.ComposeTSByTime(c.fgTt, 0)
			quotaCenter.queryNodeMetrics = map[UniqueID]*metricsinfo.QueryNodeQuotaMetrics{
				1: {
					Fgm: metricsinfo.FlowGraphMetric{
						NumFlowGraph:        1,
						MinFlowGraphTt:      fgTs,
						MinFlowGraphChannel: "dml",
					},
				},
			}
			curTs := tsoutil.ComposeTSByTime(c.curTt, 0)
			factors := quotaCenter.getTimeTickDelayFactor(curTs)
			for _, factor := range factors {
				assert.True(t, math.Abs(factor-c.expectedFactor) < 0.01)
			}
		}

		Params.QuotaConfig.MaxTimeTickDelay = backup
	})

	t.Run("test TimeTickDelayFactor factors", func(t *testing.T) {
		qc := types.NewMockQueryCoord(t)
		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().GetCollectionByID(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, merr.ErrCollectionNotFound).Maybe()
		quotaCenter := NewQuotaCenter(pcm, qc, &dataCoordMockForQuota{}, core.tsoAllocator, meta)
		type ttCase struct {
			delay          time.Duration
			expectedFactor float64
		}
		ttCases := []ttCase{
			{0 * time.Second, 1},
			{1 * time.Second, 0.9},
			{2 * time.Second, 0.8},
			{5 * time.Second, 0.5},
			{7 * time.Second, 0.3},
			{9 * time.Second, 0.1},
			{10 * time.Second, 0},
			{100 * time.Second, 0},
		}

		backup := Params.QuotaConfig.MaxTimeTickDelay
		paramtable.Get().Save(Params.QuotaConfig.DMLLimitEnabled.Key, "true")
		paramtable.Get().Save(Params.QuotaConfig.TtProtectionEnabled.Key, "true")
		paramtable.Get().Save(Params.QuotaConfig.MaxTimeTickDelay.Key, "10.0")
		paramtable.Get().Save(Params.QuotaConfig.DMLMaxInsertRatePerCollection.Key, "100.0")
		paramtable.Get().Save(Params.QuotaConfig.DMLMinInsertRatePerCollection.Key, "0.0")
		paramtable.Get().Save(Params.QuotaConfig.DMLMaxDeleteRatePerCollection.Key, "100.0")
		paramtable.Get().Save(Params.QuotaConfig.DMLMinDeleteRatePerCollection.Key, "0.0")

		quotaCenter.writableCollections = []int64{1, 2, 3}

		alloc := newMockTsoAllocator()
		quotaCenter.tsoAllocator = alloc
		for _, c := range ttCases {
			minTS := tsoutil.ComposeTSByTime(time.Now(), 0)
			hackCurTs := tsoutil.ComposeTSByTime(time.Now().Add(c.delay), 0)
			alloc.GenerateTSOF = func(count uint32) (typeutil.Timestamp, error) {
				return hackCurTs, nil
			}
			quotaCenter.queryNodeMetrics = map[UniqueID]*metricsinfo.QueryNodeQuotaMetrics{
				1: {
					Fgm: metricsinfo.FlowGraphMetric{
						NumFlowGraph:        1,
						MinFlowGraphTt:      minTS,
						MinFlowGraphChannel: "dml",
					},
					Effect: metricsinfo.NodeEffect{
						CollectionIDs: []int64{1, 2, 3},
					},
				},
			}
			quotaCenter.dataNodeMetrics = map[UniqueID]*metricsinfo.DataNodeQuotaMetrics{
				11: {
					Fgm: metricsinfo.FlowGraphMetric{
						NumFlowGraph:        1,
						MinFlowGraphTt:      minTS,
						MinFlowGraphChannel: "dml",
					},
					Effect: metricsinfo.NodeEffect{
						CollectionIDs: []int64{1, 2, 3},
					},
				},
			}
			quotaCenter.resetAllCurrentRates()
			quotaCenter.calculateWriteRates()
			deleteFactor := float64(quotaCenter.currentRates[1][internalpb.RateType_DMLDelete]) / Params.QuotaConfig.DMLMaxInsertRatePerCollection.GetAsFloat()
			assert.Equal(t, c.expectedFactor, deleteFactor)
		}
		Params.QuotaConfig.MaxTimeTickDelay = backup
	})

	t.Run("test calculateReadRates", func(t *testing.T) {
		qc := types.NewMockQueryCoord(t)
		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().GetCollectionByID(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, merr.ErrCollectionNotFound).Maybe()
		quotaCenter := NewQuotaCenter(pcm, qc, &dataCoordMockForQuota{}, core.tsoAllocator, meta)
		quotaCenter.readableCollections = []int64{1, 2, 3}
		quotaCenter.proxyMetrics = map[UniqueID]*metricsinfo.ProxyQuotaMetrics{
			1: {Rms: []metricsinfo.RateMetric{
				{Label: internalpb.RateType_DQLSearch.String(), Rate: 100},
				{Label: internalpb.RateType_DQLQuery.String(), Rate: 100},
			}}}

		paramtable.Get().Save(Params.QuotaConfig.ForceDenyReading.Key, "false")
		paramtable.Get().Save(Params.QuotaConfig.QueueProtectionEnabled.Key, "true")
		paramtable.Get().Save(Params.QuotaConfig.QueueLatencyThreshold.Key, "100")
		paramtable.Get().Save(Params.QuotaConfig.DQLLimitEnabled.Key, "true")
		paramtable.Get().Save(Params.QuotaConfig.DQLMaxQueryRatePerCollection.Key, "500")
		paramtable.Get().Save(Params.QuotaConfig.DQLMaxSearchRatePerCollection.Key, "500")
		quotaCenter.resetAllCurrentRates()
		quotaCenter.queryNodeMetrics = map[UniqueID]*metricsinfo.QueryNodeQuotaMetrics{
			1: {SearchQueue: metricsinfo.ReadInfoInQueue{
				AvgQueueDuration: Params.QuotaConfig.QueueLatencyThreshold.GetAsDuration(time.Second),
			}, Effect: metricsinfo.NodeEffect{
				NodeID:        1,
				CollectionIDs: []int64{1, 2, 3},
			}}}
		quotaCenter.calculateReadRates()
		for _, collection := range quotaCenter.readableCollections {
			assert.Equal(t, Limit(100.0*0.9), quotaCenter.currentRates[collection][internalpb.RateType_DQLSearch])
			assert.Equal(t, Limit(100.0*0.9), quotaCenter.currentRates[collection][internalpb.RateType_DQLQuery])
		}

		paramtable.Get().Save(Params.QuotaConfig.NQInQueueThreshold.Key, "100")
		quotaCenter.queryNodeMetrics = map[UniqueID]*metricsinfo.QueryNodeQuotaMetrics{
			1: {SearchQueue: metricsinfo.ReadInfoInQueue{
				UnsolvedQueue: Params.QuotaConfig.NQInQueueThreshold.GetAsInt64(),
			}}}
		quotaCenter.calculateReadRates()
		for _, collection := range quotaCenter.readableCollections {
			assert.Equal(t, Limit(100.0*0.9), quotaCenter.currentRates[collection][internalpb.RateType_DQLSearch])
			assert.Equal(t, Limit(100.0*0.9), quotaCenter.currentRates[collection][internalpb.RateType_DQLQuery])
		}

		paramtable.Get().Save(Params.QuotaConfig.ResultProtectionEnabled.Key, "true")
		paramtable.Get().Save(Params.QuotaConfig.MaxReadResultRate.Key, "1")
		quotaCenter.proxyMetrics = map[UniqueID]*metricsinfo.ProxyQuotaMetrics{
			1: {Rms: []metricsinfo.RateMetric{
				{Label: internalpb.RateType_DQLSearch.String(), Rate: 100},
				{Label: internalpb.RateType_DQLQuery.String(), Rate: 100},
				{Label: metricsinfo.ReadResultThroughput, Rate: 1.2},
			}}}
		quotaCenter.queryNodeMetrics = map[UniqueID]*metricsinfo.QueryNodeQuotaMetrics{1: {SearchQueue: metricsinfo.ReadInfoInQueue{}}}
		quotaCenter.calculateReadRates()
		for _, collection := range quotaCenter.readableCollections {
			assert.Equal(t, Limit(100.0*0.9), quotaCenter.currentRates[collection][internalpb.RateType_DQLSearch])
			assert.Equal(t, Limit(100.0*0.9), quotaCenter.currentRates[collection][internalpb.RateType_DQLQuery])
		}
	})

	t.Run("test calculateWriteRates", func(t *testing.T) {
		qc := types.NewMockQueryCoord(t)
		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().GetCollectionByID(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, merr.ErrCollectionNotFound).Maybe()
		quotaCenter := NewQuotaCenter(pcm, qc, &dataCoordMockForQuota{}, core.tsoAllocator, meta)
		err = quotaCenter.calculateWriteRates()
		assert.NoError(t, err)

		// force deny
		forceBak := Params.QuotaConfig.ForceDenyWriting.GetValue()
		paramtable.Get().Save(Params.QuotaConfig.ForceDenyWriting.Key, "true")
		quotaCenter.writableCollections = []int64{1, 2, 3}
		quotaCenter.resetAllCurrentRates()
		err = quotaCenter.calculateWriteRates()
		assert.NoError(t, err)
		for _, collection := range quotaCenter.writableCollections {
			assert.Equal(t, Limit(0), quotaCenter.currentRates[collection][internalpb.RateType_DMLInsert])
			assert.Equal(t, Limit(0), quotaCenter.currentRates[collection][internalpb.RateType_DMLDelete])
		}
		paramtable.Get().Save(Params.QuotaConfig.ForceDenyWriting.Key, forceBak)

		// disable tt delay protection
		disableTtBak := Params.QuotaConfig.TtProtectionEnabled.GetValue()
		paramtable.Get().Save(Params.QuotaConfig.TtProtectionEnabled.Key, "false")
		quotaCenter.resetAllCurrentRates()
		quotaCenter.queryNodeMetrics = make(map[UniqueID]*metricsinfo.QueryNodeQuotaMetrics)
		quotaCenter.queryNodeMetrics[0] = &metricsinfo.QueryNodeQuotaMetrics{
			Hms: metricsinfo.HardwareMetrics{
				Memory:      100,
				MemoryUsage: 100,
			},
			Effect: metricsinfo.NodeEffect{CollectionIDs: []int64{1, 2, 3}},
		}
		err = quotaCenter.calculateWriteRates()
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_MemoryQuotaExhausted, quotaCenter.quotaStates[1][milvuspb.QuotaState_DenyToWrite])
		paramtable.Get().Save(Params.QuotaConfig.TtProtectionEnabled.Key, disableTtBak)
	})

	t.Run("test MemoryFactor factors", func(t *testing.T) {
		qc := types.NewMockQueryCoord(t)
		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().GetCollectionByID(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, merr.ErrCollectionNotFound).Maybe()
		quotaCenter := NewQuotaCenter(pcm, qc, &dataCoordMockForQuota{}, core.tsoAllocator, meta)
		type memCase struct {
			lowWater       float64
			highWater      float64
			memUsage       uint64
			memTotal       uint64
			expectedFactor float64
		}
		memCases := []memCase{
			{0.8, 0.9, 10, 100, 1},
			{0.8, 0.9, 80, 100, 1},
			{0.8, 0.9, 82, 100, 0.8},
			{0.8, 0.9, 85, 100, 0.5},
			{0.8, 0.9, 88, 100, 0.2},
			{0.8, 0.9, 90, 100, 0},

			{0.85, 0.95, 25, 100, 1},
			{0.85, 0.95, 85, 100, 1},
			{0.85, 0.95, 87, 100, 0.8},
			{0.85, 0.95, 90, 100, 0.5},
			{0.85, 0.95, 93, 100, 0.2},
			{0.85, 0.95, 95, 100, 0},
		}

		quotaCenter.writableCollections = append(quotaCenter.writableCollections, 1, 2, 3)
		for _, c := range memCases {
			paramtable.Get().Save(Params.QuotaConfig.QueryNodeMemoryLowWaterLevel.Key, fmt.Sprintf("%f", c.lowWater))
			paramtable.Get().Save(Params.QuotaConfig.QueryNodeMemoryHighWaterLevel.Key, fmt.Sprintf("%f", c.highWater))
			quotaCenter.queryNodeMetrics = map[UniqueID]*metricsinfo.QueryNodeQuotaMetrics{
				1: {
					Hms: metricsinfo.HardwareMetrics{
						MemoryUsage: c.memUsage,
						Memory:      c.memTotal,
					},
					Effect: metricsinfo.NodeEffect{
						NodeID:        1,
						CollectionIDs: []int64{1, 2, 3},
					},
				},
			}
			factors := quotaCenter.getMemoryFactor()

			for _, factor := range factors {
				assert.True(t, math.Abs(factor-c.expectedFactor) < 0.01)
			}
		}

		paramtable.Get().Reset(Params.QuotaConfig.QueryNodeMemoryLowWaterLevel.Key)
		paramtable.Get().Reset(Params.QuotaConfig.QueryNodeMemoryHighWaterLevel.Key)
	})

	t.Run("test GrowingSegmentsSize factors", func(t *testing.T) {
		qc := types.NewMockQueryCoord(t)
		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().GetCollectionByID(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, merr.ErrCollectionNotFound).Maybe()
		quotaCenter := NewQuotaCenter(pcm, qc, &dataCoordMockForQuota{}, core.tsoAllocator, meta)
		tests := []struct {
			low            float64
			high           float64
			growingSize    int64
			memTotal       uint64
			expectedFactor float64
		}{
			{0.8, 0.9, 10, 100, 1},
			{0.8, 0.9, 80, 100, 1},
			{0.8, 0.9, 82, 100, 0.8},
			{0.8, 0.9, 85, 100, 0.5},
			{0.8, 0.9, 88, 100, 0.2},
			{0.8, 0.9, 90, 100, 0},

			{0.85, 0.95, 25, 100, 1},
			{0.85, 0.95, 85, 100, 1},
			{0.85, 0.95, 87, 100, 0.8},
			{0.85, 0.95, 90, 100, 0.5},
			{0.85, 0.95, 93, 100, 0.2},
			{0.85, 0.95, 95, 100, 0},
		}

		quotaCenter.writableCollections = append(quotaCenter.writableCollections, 1, 2, 3)
		paramtable.Get().Save(Params.QuotaConfig.GrowingSegmentsSizeProtectionEnabled.Key, "true")
		for _, test := range tests {
			paramtable.Get().Save(Params.QuotaConfig.GrowingSegmentsSizeLowWaterLevel.Key, fmt.Sprintf("%f", test.low))
			paramtable.Get().Save(Params.QuotaConfig.GrowingSegmentsSizeHighWaterLevel.Key, fmt.Sprintf("%f", test.high))
			quotaCenter.queryNodeMetrics = map[UniqueID]*metricsinfo.QueryNodeQuotaMetrics{
				1: {
					Hms: metricsinfo.HardwareMetrics{
						Memory: test.memTotal,
					},
					Effect: metricsinfo.NodeEffect{
						NodeID:        1,
						CollectionIDs: []int64{1, 2, 3},
					},
					GrowingSegmentsSize: test.growingSize,
				},
			}
			factors := quotaCenter.getGrowingSegmentsSizeFactor()

			for _, factor := range factors {
				assert.True(t, math.Abs(factor-test.expectedFactor) < 0.01)
			}
		}
		paramtable.Get().Reset(Params.QuotaConfig.GrowingSegmentsSizeLowWaterLevel.Key)
		paramtable.Get().Reset(Params.QuotaConfig.GrowingSegmentsSizeHighWaterLevel.Key)
	})

	t.Run("test checkDiskQuota", func(t *testing.T) {
		qc := types.NewMockQueryCoord(t)
		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().GetCollectionByID(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, merr.ErrCollectionNotFound).Maybe()
		quotaCenter := NewQuotaCenter(pcm, qc, &dataCoordMockForQuota{}, core.tsoAllocator, meta)
		quotaCenter.checkDiskQuota()

		// total DiskQuota exceeded
		quotaBackup := Params.QuotaConfig.DiskQuota
		paramtable.Get().Save(Params.QuotaConfig.DiskQuota.Key, "99")
		quotaCenter.dataCoordMetrics = &metricsinfo.DataCoordQuotaMetrics{
			TotalBinlogSize:      200 * 1024 * 1024,
			CollectionBinlogSize: map[int64]int64{1: 100 * 1024 * 1024}}
		quotaCenter.writableCollections = []int64{1, 2, 3}
		quotaCenter.resetAllCurrentRates()
		quotaCenter.checkDiskQuota()
		for _, collection := range quotaCenter.writableCollections {
			assert.Equal(t, Limit(0), quotaCenter.currentRates[collection][internalpb.RateType_DMLInsert])
			assert.Equal(t, Limit(0), quotaCenter.currentRates[collection][internalpb.RateType_DMLDelete])
		}
		paramtable.Get().Save(Params.QuotaConfig.DiskQuota.Key, quotaBackup.GetValue())

		// collection DiskQuota exceeded
		colQuotaBackup := Params.QuotaConfig.DiskQuotaPerCollection
		paramtable.Get().Save(Params.QuotaConfig.DiskQuotaPerCollection.Key, "30")
		quotaCenter.dataCoordMetrics = &metricsinfo.DataCoordQuotaMetrics{CollectionBinlogSize: map[int64]int64{
			1: 20 * 1024 * 1024, 2: 30 * 1024 * 1024, 3: 60 * 1024 * 1024}}
		quotaCenter.writableCollections = []int64{1, 2, 3}
		quotaCenter.resetAllCurrentRates()
		quotaCenter.checkDiskQuota()
		assert.NotEqual(t, Limit(0), quotaCenter.currentRates[1][internalpb.RateType_DMLInsert])
		assert.NotEqual(t, Limit(0), quotaCenter.currentRates[1][internalpb.RateType_DMLDelete])
		assert.Equal(t, Limit(0), quotaCenter.currentRates[2][internalpb.RateType_DMLInsert])
		assert.Equal(t, Limit(0), quotaCenter.currentRates[2][internalpb.RateType_DMLDelete])
		assert.Equal(t, Limit(0), quotaCenter.currentRates[3][internalpb.RateType_DMLInsert])
		assert.Equal(t, Limit(0), quotaCenter.currentRates[3][internalpb.RateType_DMLDelete])
		paramtable.Get().Save(Params.QuotaConfig.DiskQuotaPerCollection.Key, colQuotaBackup.GetValue())
	})

	t.Run("test setRates", func(t *testing.T) {
		qc := types.NewMockQueryCoord(t)
		p1 := types.NewMockProxy(t)
		p1.EXPECT().SetRates(mock.Anything, mock.Anything).Return(nil, nil)
		pcm := &proxyClientManager{proxyClient: map[int64]types.Proxy{
			TestProxyID: p1,
		}}
		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().GetCollectionByID(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, merr.ErrCollectionNotFound).Maybe()
		quotaCenter := NewQuotaCenter(pcm, qc, &dataCoordMockForQuota{}, core.tsoAllocator, meta)
		quotaCenter.resetAllCurrentRates()
		collectionID := int64(1)
		quotaCenter.currentRates[collectionID] = make(map[internalpb.RateType]ratelimitutil.Limit)
		quotaCenter.quotaStates[collectionID] = make(map[milvuspb.QuotaState]commonpb.ErrorCode)
		quotaCenter.currentRates[collectionID][internalpb.RateType_DMLInsert] = 100
		quotaCenter.quotaStates[collectionID][milvuspb.QuotaState_DenyToWrite] = commonpb.ErrorCode_MemoryQuotaExhausted
		quotaCenter.quotaStates[collectionID][milvuspb.QuotaState_DenyToRead] = commonpb.ErrorCode_ForceDeny
		err = quotaCenter.setRates()
		assert.NoError(t, err)
	})

	t.Run("test recordMetrics", func(t *testing.T) {
		qc := types.NewMockQueryCoord(t)
		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().GetCollectionByID(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, merr.ErrCollectionNotFound).Maybe()
		quotaCenter := NewQuotaCenter(pcm, qc, &dataCoordMockForQuota{}, core.tsoAllocator, meta)
		collectionID := int64(1)
		quotaCenter.quotaStates[collectionID] = make(map[milvuspb.QuotaState]commonpb.ErrorCode)
		quotaCenter.quotaStates[collectionID][milvuspb.QuotaState_DenyToWrite] = commonpb.ErrorCode_MemoryQuotaExhausted
		quotaCenter.quotaStates[collectionID][milvuspb.QuotaState_DenyToRead] = commonpb.ErrorCode_ForceDeny
		quotaCenter.recordMetrics()
	})

	t.Run("test guaranteeMinRate", func(t *testing.T) {
		qc := types.NewMockQueryCoord(t)
		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().GetCollectionByID(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, merr.ErrCollectionNotFound).Maybe()
		quotaCenter := NewQuotaCenter(pcm, qc, &dataCoordMockForQuota{}, core.tsoAllocator, meta)
		quotaCenter.resetAllCurrentRates()
		minRate := Limit(100)
		collectionID := int64(1)
		quotaCenter.currentRates[collectionID] = make(map[internalpb.RateType]ratelimitutil.Limit)
		quotaCenter.currentRates[collectionID][internalpb.RateType_DQLSearch] = Limit(50)
		quotaCenter.guaranteeMinRate(float64(minRate), internalpb.RateType_DQLSearch, 1)
		assert.Equal(t, minRate, quotaCenter.currentRates[collectionID][internalpb.RateType_DQLSearch])
	})

	t.Run("test diskAllowance", func(t *testing.T) {
		tests := []struct {
			name            string
			totalDiskQuota  string
			collDiskQuota   string
			totalDiskUsage  int64   // in MB
			collDiskUsage   int64   // in MB
			expectAllowance float64 // in bytes
		}{
			{"test max", "-1", "-1", 100, 100, math.MaxFloat64},
			{"test total quota exceeded", "100", "-1", 100, 100, 0},
			{"test coll quota exceeded", "-1", "20", 100, 20, 0},
			{"test not exceeded", "100", "20", 80, 10, 10 * 1024 * 1024},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				collection := UniqueID(0)
				meta := mockrootcoord.NewIMetaTable(t)
				meta.EXPECT().GetCollectionByID(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, merr.ErrCollectionNotFound).Maybe()
				quotaCenter := NewQuotaCenter(pcm, nil, &dataCoordMockForQuota{}, core.tsoAllocator, meta)
				quotaCenter.resetAllCurrentRates()
				quotaBackup := Params.QuotaConfig.DiskQuota
				colQuotaBackup := Params.QuotaConfig.DiskQuotaPerCollection
				paramtable.Get().Save(Params.QuotaConfig.DiskQuota.Key, test.totalDiskQuota)
				paramtable.Get().Save(Params.QuotaConfig.DiskQuotaPerCollection.Key, test.collDiskQuota)
				quotaCenter.diskMu.Lock()
				quotaCenter.dataCoordMetrics = &metricsinfo.DataCoordQuotaMetrics{}
				quotaCenter.dataCoordMetrics.CollectionBinlogSize = map[int64]int64{collection: test.collDiskUsage * 1024 * 1024}
				quotaCenter.totalBinlogSize = test.totalDiskUsage * 1024 * 1024
				quotaCenter.diskMu.Unlock()
				allowance := quotaCenter.diskAllowance(collection)
				assert.Equal(t, test.expectAllowance, allowance)
				paramtable.Get().Save(Params.QuotaConfig.DiskQuota.Key, quotaBackup.GetValue())
				paramtable.Get().Save(Params.QuotaConfig.DiskQuotaPerCollection.Key, colQuotaBackup.GetValue())
			})
		}
	})

	t.Run("test reset current rates", func(t *testing.T) {
		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().GetCollectionByID(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, merr.ErrCollectionNotFound).Maybe()
		quotaCenter := NewQuotaCenter(pcm, nil, &dataCoordMockForQuota{}, core.tsoAllocator, meta)
		quotaCenter.readableCollections = []int64{1}
		quotaCenter.writableCollections = []int64{1}
		quotaCenter.resetAllCurrentRates()

		assert.Equal(t, float64(quotaCenter.currentRates[1][internalpb.RateType_DMLInsert]), Params.QuotaConfig.DMLMaxInsertRatePerCollection.GetAsFloat())
		assert.Equal(t, float64(quotaCenter.currentRates[1][internalpb.RateType_DMLDelete]), Params.QuotaConfig.DMLMaxDeleteRatePerCollection.GetAsFloat())
		assert.Equal(t, float64(quotaCenter.currentRates[1][internalpb.RateType_DMLBulkLoad]), Params.QuotaConfig.DMLMaxBulkLoadRatePerCollection.GetAsFloat())
		assert.Equal(t, float64(quotaCenter.currentRates[1][internalpb.RateType_DQLSearch]), Params.QuotaConfig.DQLMaxSearchRatePerCollection.GetAsFloat())
		assert.Equal(t, float64(quotaCenter.currentRates[1][internalpb.RateType_DQLQuery]), Params.QuotaConfig.DQLMaxQueryRatePerCollection.GetAsFloat())

		meta.ExpectedCalls = nil
		meta.EXPECT().GetCollectionByID(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&model.Collection{
			Properties: []*commonpb.KeyValuePair{
				{
					Key:   common.CollectionInsertRateMaxKey,
					Value: "1",
				},

				{
					Key:   common.CollectionDeleteRateMaxKey,
					Value: "2",
				},

				{
					Key:   common.CollectionBulkLoadRateMaxKey,
					Value: "3",
				},

				{
					Key:   common.CollectionQueryRateMaxKey,
					Value: "4",
				},

				{
					Key:   common.CollectionSearchRateMaxKey,
					Value: "5",
				},
			},
		}, nil)
		quotaCenter.resetAllCurrentRates()
		assert.Equal(t, float64(quotaCenter.currentRates[1][internalpb.RateType_DMLInsert]), float64(1*1024*1024))
		assert.Equal(t, float64(quotaCenter.currentRates[1][internalpb.RateType_DMLDelete]), float64(2*1024*1024))
		assert.Equal(t, float64(quotaCenter.currentRates[1][internalpb.RateType_DMLBulkLoad]), float64(3*1024*1024))
		assert.Equal(t, float64(quotaCenter.currentRates[1][internalpb.RateType_DQLQuery]), float64(4))
		assert.Equal(t, float64(quotaCenter.currentRates[1][internalpb.RateType_DQLSearch]), float64(5))

	})
}

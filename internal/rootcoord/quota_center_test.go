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

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
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
	assert.Nil(t, err)
	core.tsoAllocator = newMockTsoAllocator()

	pcm := newProxyClientManager(core.proxyCreator)

	t.Run("test QuotaCenter", func(t *testing.T) {
		qc := types.NewMockQueryCoord(t)
		quotaCenter := NewQuotaCenter(pcm, qc, &dataCoordMockForQuota{}, core.tsoAllocator)
		go quotaCenter.run()
		time.Sleep(10 * time.Millisecond)
		quotaCenter.stop()
	})

	t.Run("test syncMetrics", func(t *testing.T) {
		qc := types.NewMockQueryCoord(t)
		qc.EXPECT().GetMetrics(mock.Anything, mock.Anything).Return(&milvuspb.GetMetricsResponse{Status: succStatus()}, nil)
		quotaCenter := NewQuotaCenter(pcm, qc, &dataCoordMockForQuota{}, core.tsoAllocator)
		err = quotaCenter.syncMetrics()
		assert.Error(t, err) // for empty response

		quotaCenter = NewQuotaCenter(pcm, qc, &dataCoordMockForQuota{}, core.tsoAllocator)
		err = quotaCenter.syncMetrics()
		assert.Error(t, err)

		quotaCenter = NewQuotaCenter(pcm, qc, &dataCoordMockForQuota{retFailStatus: true}, core.tsoAllocator)
		err = quotaCenter.syncMetrics()
		assert.Error(t, err)

		qc.EXPECT().GetMetrics(mock.Anything, mock.Anything).Return(nil, fmt.Errorf("mock err"))
		quotaCenter = NewQuotaCenter(pcm, qc, &dataCoordMockForQuota{retErr: true}, core.tsoAllocator)
		err = quotaCenter.syncMetrics()
		assert.Error(t, err)

		qc.EXPECT().GetMetrics(mock.Anything, mock.Anything).Return(&milvuspb.GetMetricsResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "mock failure status"),
		}, nil)
		quotaCenter = NewQuotaCenter(pcm, qc, &dataCoordMockForQuota{}, core.tsoAllocator)
		err = quotaCenter.syncMetrics()
		assert.Error(t, err)
	})

	t.Run("test forceDeny", func(t *testing.T) {
		qc := types.NewMockQueryCoord(t)
		quotaCenter := NewQuotaCenter(pcm, qc, &dataCoordMockForQuota{}, core.tsoAllocator)
		quotaCenter.forceDenyReading(commonpb.ErrorCode_ForceDeny)
		assert.Equal(t, Limit(0), quotaCenter.currentRates[internalpb.RateType_DQLQuery])
		assert.Equal(t, Limit(0), quotaCenter.currentRates[internalpb.RateType_DQLQuery])
		quotaCenter.forceDenyWriting(commonpb.ErrorCode_ForceDeny)
		assert.Equal(t, Limit(0), quotaCenter.currentRates[internalpb.RateType_DMLInsert])
		assert.Equal(t, Limit(0), quotaCenter.currentRates[internalpb.RateType_DMLDelete])
	})

	t.Run("test calculateRates", func(t *testing.T) {
		qc := types.NewMockQueryCoord(t)
		quotaCenter := NewQuotaCenter(pcm, qc, &dataCoordMockForQuota{}, core.tsoAllocator)
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

	t.Run("test getTimeTickDelayFactor", func(t *testing.T) {
		qc := types.NewMockQueryCoord(t)
		// test MaxTimestamp
		quotaCenter := NewQuotaCenter(pcm, qc, &dataCoordMockForQuota{}, core.tsoAllocator)
		factor := quotaCenter.getTimeTickDelayFactor(0)
		assert.Equal(t, float64(1), factor)

		now := time.Now()

		paramtable.Get().Save(Params.QuotaConfig.TtProtectionEnabled.Key, "true")
		paramtable.Get().Save(Params.QuotaConfig.MaxTimeTickDelay.Key, "3")

		// test force deny writing
		alloc := newMockTsoAllocator()
		alloc.GenerateTSOF = func(count uint32) (typeutil.Timestamp, error) {
			added := now.Add(Params.QuotaConfig.MaxTimeTickDelay.GetAsDuration(time.Second))
			ts := tsoutil.ComposeTSByTime(added, 0)
			return ts, nil
		}
		quotaCenter.tsoAllocator = alloc
		quotaCenter.queryNodeMetrics = map[UniqueID]*metricsinfo.QueryNodeQuotaMetrics{
			1: {Fgm: metricsinfo.FlowGraphMetric{
				MinFlowGraphTt:      tsoutil.ComposeTSByTime(now, 0),
				NumFlowGraph:        1,
				MinFlowGraphChannel: "dml",
			}}}
		ts, err := quotaCenter.tsoAllocator.GenerateTSO(1)
		assert.NoError(t, err)
		factor = quotaCenter.getTimeTickDelayFactor(ts)
		assert.Equal(t, float64(0), factor)

		// test one-third time tick delay
		alloc.GenerateTSOF = func(count uint32) (typeutil.Timestamp, error) {
			oneThirdDelay := Params.QuotaConfig.MaxTimeTickDelay.GetAsDuration(time.Second) / 3
			added := now.Add(oneThirdDelay)
			oneThirdTs := tsoutil.ComposeTSByTime(added, 0)
			return oneThirdTs, nil
		}
		quotaCenter.queryNodeMetrics = map[UniqueID]*metricsinfo.QueryNodeQuotaMetrics{
			1: {Fgm: metricsinfo.FlowGraphMetric{
				MinFlowGraphTt:      tsoutil.ComposeTSByTime(now, 0),
				NumFlowGraph:        1,
				MinFlowGraphChannel: "dml",
			}}}
		ts, err = quotaCenter.tsoAllocator.GenerateTSO(1)
		assert.NoError(t, err)
		factor = quotaCenter.getTimeTickDelayFactor(ts)
		ok := math.Abs(factor-2.0/3.0) < 0.0001
		assert.True(t, ok)
	})

	t.Run("test getTimeTickDelayFactor factors", func(t *testing.T) {
		qc := types.NewMockQueryCoord(t)
		quotaCenter := NewQuotaCenter(pcm, qc, &dataCoordMockForQuota{}, core.tsoAllocator)
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

		for i, c := range ttCases {
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
			factor := quotaCenter.getTimeTickDelayFactor(curTs)
			if math.Abs(factor-c.expectedFactor) > 0.000001 {
				t.Errorf("case %d failed: curTs[%v], fgTs[%v], expectedFactor: %f, actualFactor: %f",
					i, c.curTt, c.fgTt, c.expectedFactor, factor)
			}
		}

		Params.QuotaConfig.MaxTimeTickDelay = backup
	})

	t.Run("test getNQInQueryFactor", func(t *testing.T) {
		qc := types.NewMockQueryCoord(t)
		quotaCenter := NewQuotaCenter(pcm, qc, &dataCoordMockForQuota{}, core.tsoAllocator)
		factor := quotaCenter.getNQInQueryFactor()
		assert.Equal(t, float64(1), factor)

		// test cool off
		paramtable.Get().Save(Params.QuotaConfig.QueueProtectionEnabled.Key, "true")
		paramtable.Get().Save(Params.QuotaConfig.NQInQueueThreshold.Key, "100")
		quotaCenter.queryNodeMetrics = map[UniqueID]*metricsinfo.QueryNodeQuotaMetrics{
			1: {SearchQueue: metricsinfo.ReadInfoInQueue{
				UnsolvedQueue: Params.QuotaConfig.NQInQueueThreshold.GetAsInt64(),
			}}}
		factor = quotaCenter.getNQInQueryFactor()
		assert.Equal(t, Params.QuotaConfig.CoolOffSpeed.GetAsFloat(), factor)

		// test no cool off
		quotaCenter.queryNodeMetrics = map[UniqueID]*metricsinfo.QueryNodeQuotaMetrics{
			1: {SearchQueue: metricsinfo.ReadInfoInQueue{
				UnsolvedQueue: Params.QuotaConfig.NQInQueueThreshold.GetAsInt64() - 1,
			}}}
		factor = quotaCenter.getNQInQueryFactor()
		assert.Equal(t, 1.0, factor)
		//ok := math.Abs(factor-1.0) < 0.0001
		//assert.True(t, ok)
	})

	t.Run("test getQueryLatencyFactor", func(t *testing.T) {
		qc := types.NewMockQueryCoord(t)
		quotaCenter := NewQuotaCenter(pcm, qc, &dataCoordMockForQuota{}, core.tsoAllocator)
		factor := quotaCenter.getQueryLatencyFactor()
		assert.Equal(t, float64(1), factor)

		// test cool off
		paramtable.Get().Save(Params.QuotaConfig.QueueProtectionEnabled.Key, "true")
		paramtable.Get().Save(Params.QuotaConfig.QueueLatencyThreshold.Key, "3")

		quotaCenter.queryNodeMetrics = map[UniqueID]*metricsinfo.QueryNodeQuotaMetrics{
			1: {SearchQueue: metricsinfo.ReadInfoInQueue{
				AvgQueueDuration: Params.QuotaConfig.QueueLatencyThreshold.GetAsDuration(time.Second),
			}}}
		factor = quotaCenter.getQueryLatencyFactor()
		assert.Equal(t, Params.QuotaConfig.CoolOffSpeed.GetAsFloat(), factor)

		// test no cool off
		quotaCenter.queryNodeMetrics = map[UniqueID]*metricsinfo.QueryNodeQuotaMetrics{
			1: {SearchQueue: metricsinfo.ReadInfoInQueue{
				AvgQueueDuration: 1 * time.Second,
			}}}
		factor = quotaCenter.getQueryLatencyFactor()
		assert.Equal(t, 1.0, factor)
	})

	t.Run("test checkReadResult", func(t *testing.T) {
		qc := types.NewMockQueryCoord(t)
		quotaCenter := NewQuotaCenter(pcm, qc, &dataCoordMockForQuota{}, core.tsoAllocator)
		factor := quotaCenter.getReadResultFactor()
		assert.Equal(t, float64(1), factor)

		// test cool off
		paramtable.Get().Save(Params.QuotaConfig.ResultProtectionEnabled.Key, "true")
		paramtable.Get().Save(Params.QuotaConfig.MaxReadResultRate.Key, fmt.Sprintf("%f", 1.0/1024/1024))

		quotaCenter.proxyMetrics = map[UniqueID]*metricsinfo.ProxyQuotaMetrics{
			1: {Rms: []metricsinfo.RateMetric{
				{Label: metricsinfo.ReadResultThroughput, Rate: 1.2},
			}}}
		factor = quotaCenter.getReadResultFactor()
		assert.Equal(t, Params.QuotaConfig.CoolOffSpeed.GetAsFloat(), factor)

		// test no cool off
		quotaCenter.proxyMetrics = map[UniqueID]*metricsinfo.ProxyQuotaMetrics{
			1: {Rms: []metricsinfo.RateMetric{
				{Label: metricsinfo.ReadResultThroughput, Rate: 0.8},
			}}}
		factor = quotaCenter.getReadResultFactor()
		assert.Equal(t, 1.0, factor)
	})

	t.Run("test calculateReadRates", func(t *testing.T) {
		qc := types.NewMockQueryCoord(t)
		quotaCenter := NewQuotaCenter(pcm, qc, &dataCoordMockForQuota{}, core.tsoAllocator)
		quotaCenter.proxyMetrics = map[UniqueID]*metricsinfo.ProxyQuotaMetrics{
			1: {Rms: []metricsinfo.RateMetric{
				{Label: internalpb.RateType_DQLSearch.String(), Rate: 100},
				{Label: internalpb.RateType_DQLQuery.String(), Rate: 100},
			}}}

		paramtable.Get().Save(Params.QuotaConfig.ForceDenyReading.Key, "false")
		paramtable.Get().Save(Params.QuotaConfig.QueueProtectionEnabled.Key, "true")
		paramtable.Get().Save(Params.QuotaConfig.QueueLatencyThreshold.Key, "100")
		quotaCenter.queryNodeMetrics = map[UniqueID]*metricsinfo.QueryNodeQuotaMetrics{
			1: {SearchQueue: metricsinfo.ReadInfoInQueue{
				AvgQueueDuration: Params.QuotaConfig.QueueLatencyThreshold.GetAsDuration(time.Second),
			}}}
		quotaCenter.calculateReadRates()
		assert.Equal(t, Limit(100.0*0.9), quotaCenter.currentRates[internalpb.RateType_DQLSearch])
		assert.Equal(t, Limit(100.0*0.9), quotaCenter.currentRates[internalpb.RateType_DQLQuery])

		paramtable.Get().Save(Params.QuotaConfig.NQInQueueThreshold.Key, "100")
		quotaCenter.queryNodeMetrics = map[UniqueID]*metricsinfo.QueryNodeQuotaMetrics{
			1: {SearchQueue: metricsinfo.ReadInfoInQueue{
				UnsolvedQueue: Params.QuotaConfig.NQInQueueThreshold.GetAsInt64(),
			}}}
		quotaCenter.calculateReadRates()
		assert.Equal(t, Limit(100.0*0.9), quotaCenter.currentRates[internalpb.RateType_DQLSearch])
		assert.Equal(t, Limit(100.0*0.9), quotaCenter.currentRates[internalpb.RateType_DQLQuery])

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
		assert.Equal(t, Limit(100.0*0.9), quotaCenter.currentRates[internalpb.RateType_DQLSearch])
		assert.Equal(t, Limit(100.0*0.9), quotaCenter.currentRates[internalpb.RateType_DQLQuery])
	})

	t.Run("test calculateWriteRates", func(t *testing.T) {
		qc := types.NewMockQueryCoord(t)
		quotaCenter := NewQuotaCenter(pcm, qc, &dataCoordMockForQuota{}, core.tsoAllocator)
		err = quotaCenter.calculateWriteRates()
		assert.NoError(t, err)

		// DiskQuota exceeded
		quotaBackup := Params.QuotaConfig.DiskQuota
		paramtable.Get().Save(Params.QuotaConfig.DiskQuota.Key, "99")
		quotaCenter.dataCoordMetrics = &metricsinfo.DataCoordQuotaMetrics{TotalBinlogSize: 100}
		err = quotaCenter.calculateWriteRates()
		assert.NoError(t, err)
		assert.Equal(t, Limit(0), quotaCenter.currentRates[internalpb.RateType_DMLInsert])
		assert.Equal(t, Limit(0), quotaCenter.currentRates[internalpb.RateType_DMLDelete])
		Params.QuotaConfig.DiskQuota = quotaBackup

		// force deny
		forceBak := Params.QuotaConfig.ForceDenyWriting
		paramtable.Get().Save(Params.QuotaConfig.ForceDenyWriting.Key, "true")
		err = quotaCenter.calculateWriteRates()
		assert.NoError(t, err)
		assert.Equal(t, Limit(0), quotaCenter.currentRates[internalpb.RateType_DMLInsert])
		assert.Equal(t, Limit(0), quotaCenter.currentRates[internalpb.RateType_DMLDelete])
		Params.QuotaConfig.ForceDenyWriting = forceBak
	})

	t.Run("test getMemoryFactor basic", func(t *testing.T) {
		qc := types.NewMockQueryCoord(t)
		quotaCenter := NewQuotaCenter(pcm, qc, &dataCoordMockForQuota{}, core.tsoAllocator)
		factor := quotaCenter.getMemoryFactor()
		assert.Equal(t, float64(1), factor)
		quotaCenter.dataNodeMetrics = map[UniqueID]*metricsinfo.DataNodeQuotaMetrics{1: {Hms: metricsinfo.HardwareMetrics{MemoryUsage: 100, Memory: 100}}}
		factor = quotaCenter.getMemoryFactor()
		assert.Equal(t, float64(0), factor)
		quotaCenter.queryNodeMetrics = map[UniqueID]*metricsinfo.QueryNodeQuotaMetrics{1: {Hms: metricsinfo.HardwareMetrics{MemoryUsage: 100, Memory: 100}}}
		factor = quotaCenter.getMemoryFactor()
		assert.Equal(t, float64(0), factor)
	})

	t.Run("test getMemoryFactor factors", func(t *testing.T) {
		qc := types.NewMockQueryCoord(t)
		quotaCenter := NewQuotaCenter(pcm, qc, &dataCoordMockForQuota{}, core.tsoAllocator)
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

		for i, c := range memCases {
			paramtable.Get().Save(Params.QuotaConfig.QueryNodeMemoryLowWaterLevel.Key, fmt.Sprintf("%f", c.lowWater))
			paramtable.Get().Save(Params.QuotaConfig.QueryNodeMemoryHighWaterLevel.Key, fmt.Sprintf("%f", c.highWater))
			quotaCenter.queryNodeMetrics = map[UniqueID]*metricsinfo.QueryNodeQuotaMetrics{1: {Hms: metricsinfo.HardwareMetrics{MemoryUsage: c.memUsage, Memory: c.memTotal}}}
			factor := quotaCenter.getMemoryFactor()
			if math.Abs(factor-c.expectedFactor) > 0.000001 {
				t.Errorf("case %d failed: waterLever[low:%v, high:%v], memMetric[used:%d, total:%d], expectedFactor: %f, actualFactor: %f",
					i, c.lowWater, c.highWater, c.memUsage, c.memTotal, c.expectedFactor, factor)
			}
		}

		paramtable.Get().Reset(Params.QuotaConfig.QueryNodeMemoryLowWaterLevel.Key)
		paramtable.Get().Reset(Params.QuotaConfig.QueryNodeMemoryHighWaterLevel.Key)
	})

	t.Run("test ifDiskQuotaExceeded", func(t *testing.T) {
		qc := types.NewMockQueryCoord(t)
		quotaCenter := NewQuotaCenter(pcm, qc, &dataCoordMockForQuota{}, core.tsoAllocator)

		paramtable.Get().Save(Params.QuotaConfig.DiskProtectionEnabled.Key, "false")
		ok := quotaCenter.ifDiskQuotaExceeded()
		assert.False(t, ok)
		paramtable.Get().Save(Params.QuotaConfig.DiskProtectionEnabled.Key, "true")

		quotaBackup := Params.QuotaConfig.DiskQuota
		paramtable.Get().Save(Params.QuotaConfig.DiskQuota.Key, fmt.Sprintf("%f", 99.0/1024/1024))
		quotaCenter.dataCoordMetrics = &metricsinfo.DataCoordQuotaMetrics{TotalBinlogSize: 100}
		ok = quotaCenter.ifDiskQuotaExceeded()
		assert.True(t, ok)

		paramtable.Get().Save(Params.QuotaConfig.DiskQuota.Key, fmt.Sprintf("%f", 101.0/1024/1024))
		quotaCenter.dataCoordMetrics = &metricsinfo.DataCoordQuotaMetrics{TotalBinlogSize: 100}
		ok = quotaCenter.ifDiskQuotaExceeded()
		assert.False(t, ok)
		Params.QuotaConfig.DiskQuota = quotaBackup
	})

	t.Run("test setRates", func(t *testing.T) {
		qc := types.NewMockQueryCoord(t)
		quotaCenter := NewQuotaCenter(pcm, qc, &dataCoordMockForQuota{}, core.tsoAllocator)
		quotaCenter.currentRates[internalpb.RateType_DMLInsert] = 100
		quotaCenter.quotaStates[milvuspb.QuotaState_DenyToWrite] = commonpb.ErrorCode_MemoryQuotaExhausted
		quotaCenter.quotaStates[milvuspb.QuotaState_DenyToRead] = commonpb.ErrorCode_ForceDeny
		err = quotaCenter.setRates()
		assert.NoError(t, err)
	})

	t.Run("test recordMetrics", func(t *testing.T) {
		qc := types.NewMockQueryCoord(t)
		quotaCenter := NewQuotaCenter(pcm, qc, &dataCoordMockForQuota{}, core.tsoAllocator)
		quotaCenter.quotaStates[milvuspb.QuotaState_DenyToWrite] = commonpb.ErrorCode_MemoryQuotaExhausted
		quotaCenter.quotaStates[milvuspb.QuotaState_DenyToRead] = commonpb.ErrorCode_ForceDeny
		quotaCenter.recordMetrics()
	})

	t.Run("test guaranteeMinRate", func(t *testing.T) {
		qc := types.NewMockQueryCoord(t)
		quotaCenter := NewQuotaCenter(pcm, qc, &dataCoordMockForQuota{}, core.tsoAllocator)
		minRate := Limit(100)
		quotaCenter.currentRates[internalpb.RateType_DQLSearch] = Limit(50)
		quotaCenter.guaranteeMinRate(float64(minRate), internalpb.RateType_DQLSearch)
		assert.Equal(t, minRate, quotaCenter.currentRates[internalpb.RateType_DQLSearch])
	})
}

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

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type queryCoordMockForQuota struct {
	mockQueryCoord
	retErr        bool
	retFailStatus bool
}

type dataCoordMockForQuota struct {
	mockDataCoord
	retErr        bool
	retFailStatus bool
}

func (q *queryCoordMockForQuota) GetMetrics(ctx context.Context, request *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	if q.retErr {
		return nil, fmt.Errorf("mock err")
	}
	if q.retFailStatus {
		return &milvuspb.GetMetricsResponse{
			Status: failStatus(commonpb.ErrorCode_UnexpectedError, "mock failure status"),
		}, nil
	}
	return &milvuspb.GetMetricsResponse{
		Status: succStatus(),
	}, nil
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
		quotaCenter := NewQuotaCenter(pcm, &queryCoordMockForQuota{}, &dataCoordMockForQuota{}, core.tsoAllocator)
		go quotaCenter.run()
		time.Sleep(10 * time.Millisecond)
		quotaCenter.stop()
	})

	t.Run("test syncMetrics", func(t *testing.T) {
		quotaCenter := NewQuotaCenter(pcm, &queryCoordMockForQuota{}, &dataCoordMockForQuota{}, core.tsoAllocator)
		err = quotaCenter.syncMetrics()
		assert.Error(t, err) // for empty response

		quotaCenter = NewQuotaCenter(pcm, &queryCoordMockForQuota{retErr: true}, &dataCoordMockForQuota{}, core.tsoAllocator)
		err = quotaCenter.syncMetrics()
		assert.Error(t, err)

		quotaCenter = NewQuotaCenter(pcm, &queryCoordMockForQuota{}, &dataCoordMockForQuota{retErr: true}, core.tsoAllocator)
		err = quotaCenter.syncMetrics()
		assert.Error(t, err)

		quotaCenter = NewQuotaCenter(pcm, &queryCoordMockForQuota{retFailStatus: true}, &dataCoordMockForQuota{}, core.tsoAllocator)
		err = quotaCenter.syncMetrics()
		assert.Error(t, err)

		quotaCenter = NewQuotaCenter(pcm, &queryCoordMockForQuota{}, &dataCoordMockForQuota{retFailStatus: true}, core.tsoAllocator)
		err = quotaCenter.syncMetrics()
		assert.Error(t, err)
	})

	t.Run("test forceDeny", func(t *testing.T) {
		quotaCenter := NewQuotaCenter(pcm, &queryCoordMockForQuota{}, &dataCoordMockForQuota{}, core.tsoAllocator)
		quotaCenter.forceDenyReading(ManualForceDeny)
		assert.Equal(t, Limit(0), quotaCenter.currentRates[internalpb.RateType_DQLQuery])
		assert.Equal(t, Limit(0), quotaCenter.currentRates[internalpb.RateType_DQLQuery])
		quotaCenter.forceDenyWriting(ManualForceDeny)
		assert.Equal(t, Limit(0), quotaCenter.currentRates[internalpb.RateType_DMLInsert])
		assert.Equal(t, Limit(0), quotaCenter.currentRates[internalpb.RateType_DMLDelete])
	})

	t.Run("test calculateRates", func(t *testing.T) {
		quotaCenter := NewQuotaCenter(pcm, &queryCoordMockForQuota{}, &dataCoordMockForQuota{}, core.tsoAllocator)
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
		// test MaxTimestamp
		quotaCenter := NewQuotaCenter(pcm, &queryCoordMockForQuota{}, &dataCoordMockForQuota{}, core.tsoAllocator)
		factor := quotaCenter.getTimeTickDelayFactor(0)
		assert.Equal(t, float64(1), factor)

		now := time.Now()

		Params.QuotaConfig.TtProtectionEnabled = true
		Params.QuotaConfig.MaxTimeTickDelay = 3 * time.Second

		// test force deny writing
		alloc := newMockTsoAllocator()
		alloc.GenerateTSOF = func(count uint32) (typeutil.Timestamp, error) {
			added := now.Add(Params.QuotaConfig.MaxTimeTickDelay)
			ts := tsoutil.ComposeTSByTime(added, 0)
			return ts, nil
		}
		quotaCenter.tsoAllocator = alloc
		quotaCenter.queryNodeMetrics = map[UniqueID]*metricsinfo.QueryNodeQuotaMetrics{
			1: {Fgm: metricsinfo.FlowGraphMetric{
				MinFlowGraphTt: tsoutil.ComposeTSByTime(now, 0),
				NumFlowGraph:   1,
			}}}
		ts, err := quotaCenter.tsoAllocator.GenerateTSO(1)
		assert.NoError(t, err)
		factor = quotaCenter.getTimeTickDelayFactor(ts)
		assert.Equal(t, float64(0), factor)

		// test one-third time tick delay
		alloc.GenerateTSOF = func(count uint32) (typeutil.Timestamp, error) {
			oneThirdDelay := Params.QuotaConfig.MaxTimeTickDelay / 3
			added := now.Add(oneThirdDelay)
			oneThirdTs := tsoutil.ComposeTSByTime(added, 0)
			return oneThirdTs, nil
		}
		quotaCenter.queryNodeMetrics = map[UniqueID]*metricsinfo.QueryNodeQuotaMetrics{
			1: {Fgm: metricsinfo.FlowGraphMetric{
				MinFlowGraphTt: tsoutil.ComposeTSByTime(now, 0),
				NumFlowGraph:   1,
			}}}
		ts, err = quotaCenter.tsoAllocator.GenerateTSO(1)
		assert.NoError(t, err)
		factor = quotaCenter.getTimeTickDelayFactor(ts)
		ok := math.Abs(factor-2.0/3.0) < 0.0001
		assert.True(t, ok)
	})

	t.Run("test getTimeTickDelayFactor factors", func(t *testing.T) {
		quotaCenter := NewQuotaCenter(pcm, &queryCoordMockForQuota{}, &dataCoordMockForQuota{}, core.tsoAllocator)
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
			Params.QuotaConfig.MaxTimeTickDelay = c.maxTtDelay
			fgTs := tsoutil.ComposeTSByTime(c.fgTt, 0)
			quotaCenter.queryNodeMetrics = map[UniqueID]*metricsinfo.QueryNodeQuotaMetrics{1: {Fgm: metricsinfo.FlowGraphMetric{NumFlowGraph: 1, MinFlowGraphTt: fgTs}}}
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
		quotaCenter := NewQuotaCenter(pcm, &queryCoordMockForQuota{}, &dataCoordMockForQuota{}, core.tsoAllocator)
		factor := quotaCenter.getNQInQueryFactor()
		assert.Equal(t, float64(1), factor)

		// test cool off
		Params.QuotaConfig.QueueProtectionEnabled = true
		Params.QuotaConfig.NQInQueueThreshold = 100
		quotaCenter.queryNodeMetrics = map[UniqueID]*metricsinfo.QueryNodeQuotaMetrics{
			1: {SearchQueue: metricsinfo.ReadInfoInQueue{
				UnsolvedQueue: Params.QuotaConfig.NQInQueueThreshold,
			}}}
		factor = quotaCenter.getNQInQueryFactor()
		assert.Equal(t, Params.QuotaConfig.CoolOffSpeed, factor)

		// test no cool off
		quotaCenter.queryNodeMetrics = map[UniqueID]*metricsinfo.QueryNodeQuotaMetrics{
			1: {SearchQueue: metricsinfo.ReadInfoInQueue{
				UnsolvedQueue: Params.QuotaConfig.NQInQueueThreshold - 1,
			}}}
		factor = quotaCenter.getNQInQueryFactor()
		assert.Equal(t, 1.0, factor)
		//ok := math.Abs(factor-1.0) < 0.0001
		//assert.True(t, ok)
	})

	t.Run("test getQueryLatencyFactor", func(t *testing.T) {
		quotaCenter := NewQuotaCenter(pcm, &queryCoordMockForQuota{}, &dataCoordMockForQuota{}, core.tsoAllocator)
		factor := quotaCenter.getQueryLatencyFactor()
		assert.Equal(t, float64(1), factor)

		// test cool off
		Params.QuotaConfig.QueueProtectionEnabled = true
		Params.QuotaConfig.QueueLatencyThreshold = float64(3 * time.Second)

		quotaCenter.queryNodeMetrics = map[UniqueID]*metricsinfo.QueryNodeQuotaMetrics{
			1: {SearchQueue: metricsinfo.ReadInfoInQueue{
				AvgQueueDuration: time.Duration(Params.QuotaConfig.QueueLatencyThreshold),
			}}}
		factor = quotaCenter.getQueryLatencyFactor()
		assert.Equal(t, Params.QuotaConfig.CoolOffSpeed, factor)

		// test no cool off
		quotaCenter.queryNodeMetrics = map[UniqueID]*metricsinfo.QueryNodeQuotaMetrics{
			1: {SearchQueue: metricsinfo.ReadInfoInQueue{
				AvgQueueDuration: 1 * time.Second,
			}}}
		factor = quotaCenter.getQueryLatencyFactor()
		assert.Equal(t, 1.0, factor)
	})

	t.Run("test checkReadResult", func(t *testing.T) {
		quotaCenter := NewQuotaCenter(pcm, &queryCoordMockForQuota{}, &dataCoordMockForQuota{}, core.tsoAllocator)
		factor := quotaCenter.getReadResultFactor()
		assert.Equal(t, float64(1), factor)

		// test cool off
		Params.QuotaConfig.ResultProtectionEnabled = true
		Params.QuotaConfig.MaxReadResultRate = 1

		quotaCenter.proxyMetrics = map[UniqueID]*metricsinfo.ProxyQuotaMetrics{
			1: {Rms: []metricsinfo.RateMetric{
				{Label: metricsinfo.ReadResultThroughput, Rate: 1.2},
			}}}
		factor = quotaCenter.getReadResultFactor()
		assert.Equal(t, Params.QuotaConfig.CoolOffSpeed, factor)

		// test no cool off
		quotaCenter.proxyMetrics = map[UniqueID]*metricsinfo.ProxyQuotaMetrics{
			1: {Rms: []metricsinfo.RateMetric{
				{Label: metricsinfo.ReadResultThroughput, Rate: 0.8},
			}}}
		factor = quotaCenter.getReadResultFactor()
		assert.Equal(t, 1.0, factor)
	})

	t.Run("test calculateReadRates", func(t *testing.T) {
		quotaCenter := NewQuotaCenter(pcm, &queryCoordMockForQuota{}, &dataCoordMockForQuota{}, core.tsoAllocator)
		quotaCenter.proxyMetrics = map[UniqueID]*metricsinfo.ProxyQuotaMetrics{
			1: {Rms: []metricsinfo.RateMetric{
				{Label: internalpb.RateType_DQLSearch.String(), Rate: 100},
				{Label: internalpb.RateType_DQLQuery.String(), Rate: 100},
			}}}

		Params.QuotaConfig.ForceDenyReading = false
		Params.QuotaConfig.QueueProtectionEnabled = true
		Params.QuotaConfig.QueueLatencyThreshold = 100
		quotaCenter.queryNodeMetrics = map[UniqueID]*metricsinfo.QueryNodeQuotaMetrics{
			1: {SearchQueue: metricsinfo.ReadInfoInQueue{
				AvgQueueDuration: time.Duration(Params.QuotaConfig.QueueLatencyThreshold),
			}}}
		quotaCenter.calculateReadRates()
		assert.Equal(t, Limit(100.0*0.9), quotaCenter.currentRates[internalpb.RateType_DQLSearch])
		assert.Equal(t, Limit(100.0*0.9), quotaCenter.currentRates[internalpb.RateType_DQLQuery])

		Params.QuotaConfig.NQInQueueThreshold = 100
		quotaCenter.queryNodeMetrics = map[UniqueID]*metricsinfo.QueryNodeQuotaMetrics{
			1: {SearchQueue: metricsinfo.ReadInfoInQueue{
				UnsolvedQueue: Params.QuotaConfig.NQInQueueThreshold,
			}}}
		quotaCenter.calculateReadRates()
		assert.Equal(t, Limit(100.0*0.9), quotaCenter.currentRates[internalpb.RateType_DQLSearch])
		assert.Equal(t, Limit(100.0*0.9), quotaCenter.currentRates[internalpb.RateType_DQLQuery])

		Params.QuotaConfig.ResultProtectionEnabled = true
		Params.QuotaConfig.MaxReadResultRate = 1
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
		quotaCenter := NewQuotaCenter(pcm, &queryCoordMockForQuota{}, &dataCoordMockForQuota{}, core.tsoAllocator)
		err = quotaCenter.calculateWriteRates()
		assert.NoError(t, err)

		// DiskQuota exceeded
		quotaBackup := Params.QuotaConfig.DiskQuota
		Params.QuotaConfig.DiskQuota = 99
		quotaCenter.dataCoordMetrics = &metricsinfo.DataCoordQuotaMetrics{TotalBinlogSize: 100}
		err = quotaCenter.calculateWriteRates()
		assert.NoError(t, err)
		assert.Equal(t, Limit(0), quotaCenter.currentRates[internalpb.RateType_DMLInsert])
		assert.Equal(t, Limit(0), quotaCenter.currentRates[internalpb.RateType_DMLDelete])
		Params.QuotaConfig.DiskQuota = quotaBackup

		// force deny
		forceBak := Params.QuotaConfig.ForceDenyWriting
		Params.QuotaConfig.ForceDenyWriting = true
		err = quotaCenter.calculateWriteRates()
		assert.NoError(t, err)
		assert.Equal(t, Limit(0), quotaCenter.currentRates[internalpb.RateType_DMLInsert])
		assert.Equal(t, Limit(0), quotaCenter.currentRates[internalpb.RateType_DMLDelete])
		Params.QuotaConfig.ForceDenyWriting = forceBak
	})

	t.Run("test getMemoryFactor basic", func(t *testing.T) {
		quotaCenter := NewQuotaCenter(pcm, &queryCoordMockForQuota{}, &dataCoordMockForQuota{}, core.tsoAllocator)
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
		quotaCenter := NewQuotaCenter(pcm, &queryCoordMockForQuota{}, &dataCoordMockForQuota{}, core.tsoAllocator)
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

		lowBackup := Params.QuotaConfig.DataNodeMemoryLowWaterLevel
		highBackup := Params.QuotaConfig.DataNodeMemoryHighWaterLevel

		for i, c := range memCases {
			Params.QuotaConfig.QueryNodeMemoryLowWaterLevel = c.lowWater
			Params.QuotaConfig.QueryNodeMemoryHighWaterLevel = c.highWater
			quotaCenter.queryNodeMetrics = map[UniqueID]*metricsinfo.QueryNodeQuotaMetrics{1: {Hms: metricsinfo.HardwareMetrics{MemoryUsage: c.memUsage, Memory: c.memTotal}}}
			factor := quotaCenter.getMemoryFactor()
			if math.Abs(factor-c.expectedFactor) > 0.000001 {
				t.Errorf("case %d failed: waterLever[low:%v, high:%v], memMetric[used:%d, total:%d], expectedFactor: %f, actualFactor: %f",
					i, c.lowWater, c.highWater, c.memUsage, c.memTotal, c.expectedFactor, factor)
			}
		}

		Params.QuotaConfig.QueryNodeMemoryLowWaterLevel = lowBackup
		Params.QuotaConfig.QueryNodeMemoryHighWaterLevel = highBackup
	})

	t.Run("test ifDiskQuotaExceeded", func(t *testing.T) {
		quotaCenter := NewQuotaCenter(pcm, &queryCoordMockForQuota{}, &dataCoordMockForQuota{}, core.tsoAllocator)

		Params.QuotaConfig.DiskProtectionEnabled = false
		ok := quotaCenter.ifDiskQuotaExceeded()
		assert.False(t, ok)
		Params.QuotaConfig.DiskProtectionEnabled = true

		quotaBackup := Params.QuotaConfig.DiskQuota
		Params.QuotaConfig.DiskQuota = 99
		quotaCenter.dataCoordMetrics = &metricsinfo.DataCoordQuotaMetrics{TotalBinlogSize: 100}
		ok = quotaCenter.ifDiskQuotaExceeded()
		assert.True(t, ok)

		Params.QuotaConfig.DiskQuota = 101
		quotaCenter.dataCoordMetrics = &metricsinfo.DataCoordQuotaMetrics{TotalBinlogSize: 100}
		ok = quotaCenter.ifDiskQuotaExceeded()
		assert.False(t, ok)
		Params.QuotaConfig.DiskQuota = quotaBackup
	})

	t.Run("test setRates", func(t *testing.T) {
		quotaCenter := NewQuotaCenter(pcm, &queryCoordMockForQuota{}, &dataCoordMockForQuota{}, core.tsoAllocator)
		quotaCenter.currentRates[internalpb.RateType_DMLInsert] = 100
		err = quotaCenter.setRates()
		assert.NoError(t, err)
	})

	t.Run("test guaranteeMinRate", func(t *testing.T) {
		quotaCenter := NewQuotaCenter(pcm, &queryCoordMockForQuota{}, &dataCoordMockForQuota{}, core.tsoAllocator)
		minRate := Limit(100)
		quotaCenter.currentRates[internalpb.RateType_DQLSearch] = Limit(50)
		quotaCenter.guaranteeMinRate(float64(minRate), internalpb.RateType_DQLSearch)
		assert.Equal(t, minRate, quotaCenter.currentRates[internalpb.RateType_DQLSearch])
	})
}

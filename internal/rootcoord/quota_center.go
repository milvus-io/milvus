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
	"sync"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/tso"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/ratelimitutil"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

const (
	GetMetricsTimeout = 10 * time.Second
	SetRatesTimeout   = 10 * time.Second
)

type ForceDenyTriggerReason string

const (
	ManualForceDeny   ForceDenyTriggerReason = "ManualForceDeny"
	MemoryExhausted   ForceDenyTriggerReason = "MemoryExhausted"
	TimeTickLongDelay ForceDenyTriggerReason = "TimeTickLongDelay"
)

type RateAllocateStrategy int32

const (
	Average      RateAllocateStrategy = 0
	ByRateWeight RateAllocateStrategy = 1
)

var DefaultRateAllocateStrategy = Average

const Inf = ratelimitutil.Inf

type Limit = ratelimitutil.Limit

// QuotaCenter manages the quota and limitations of the whole cluster,
// it receives metrics info from DataNodes, QueryNodes and Proxies, and
// notifies Proxies to limit rate of requests from clients or reject
// all requests when the cluster met resources issues.
// Limitations:
//   1. DML throughput limitation;
//   2. DDL, DQL qps/rps limitation;
// Protections:
//   1. TT protection -> 				dqlRate = maxDQLRate * (maxDelay - ttDelay) / maxDelay
//   2. Memory protection -> 			dmlRate = maxDMLRate * (highMem - curMem) / (highMem - lowMem)
//   3. DQL Queue length protection ->  dqlRate = curDQLRate * CoolOffSpeed
//   4. DQL queue latency protection -> dqlRate = curDQLRate * CoolOffSpeed
// If necessary, user can also manually force to deny RW requests.
type QuotaCenter struct {
	// clients
	proxies    *proxyClientManager
	queryCoord types.QueryCoord
	dataCoord  types.DataCoord

	// metrics
	queryNodeMetrics []*metricsinfo.QueryNodeQuotaMetrics
	dataNodeMetrics  []*metricsinfo.DataNodeQuotaMetrics
	proxyMetrics     []*metricsinfo.ProxyQuotaMetrics

	currentRates map[internalpb.RateType]Limit
	tsoAllocator tso.Allocator

	rateAllocateStrategy RateAllocateStrategy

	stopOnce sync.Once
	stopChan chan struct{}
}

// NewQuotaCenter returns a new QuotaCenter.
func NewQuotaCenter(proxies *proxyClientManager, queryCoord types.QueryCoord, dataCoord types.DataCoord, tsoAllocator tso.Allocator) *QuotaCenter {
	return &QuotaCenter{
		proxies:      proxies,
		queryCoord:   queryCoord,
		dataCoord:    dataCoord,
		currentRates: make(map[internalpb.RateType]Limit),
		tsoAllocator: tsoAllocator,

		rateAllocateStrategy: DefaultRateAllocateStrategy,
		stopChan:             make(chan struct{}),
	}
}

// run starts the service of QuotaCenter.
func (q *QuotaCenter) run() {
	log.Info("Start QuotaCenter", zap.Float64("collectInterval/s", Params.QuotaConfig.QuotaCenterCollectInterval))
	ticker := time.NewTicker(time.Duration(Params.QuotaConfig.QuotaCenterCollectInterval * float64(time.Second)))
	defer ticker.Stop()
	for {
		select {
		case <-q.stopChan:
			log.Info("QuotaCenter exit")
			return
		case <-ticker.C:
			err := q.syncMetrics()
			if err != nil {
				log.Error("quotaCenter sync metrics failed", zap.Error(err))
				break
			}
			err = q.calculateRates()
			if err != nil {
				log.Error("quotaCenter calculate rates failed", zap.Error(err))
				break
			}
			err = q.setRates()
			if err != nil {
				log.Error("quotaCenter setRates failed", zap.Error(err))
			}
		}
	}
}

// stop would stop the service of QuotaCenter.
func (q *QuotaCenter) stop() {
	q.stopOnce.Do(func() {
		q.stopChan <- struct{}{}
	})
}

//  clearMetrics removes all metrics stored in QuotaCenter.
func (q *QuotaCenter) clearMetrics() {
	q.dataNodeMetrics = make([]*metricsinfo.DataNodeQuotaMetrics, 0)
	q.queryNodeMetrics = make([]*metricsinfo.QueryNodeQuotaMetrics, 0)
	q.proxyMetrics = make([]*metricsinfo.ProxyQuotaMetrics, 0)
}

// syncMetrics sends GetMetrics requests to DataCoord and QueryCoord to sync the metrics in DataNodes and QueryNodes.
func (q *QuotaCenter) syncMetrics() error {
	q.clearMetrics()
	ctx, cancel := context.WithTimeout(context.Background(), GetMetricsTimeout)
	defer cancel()

	group := &errgroup.Group{}
	req, err := metricsinfo.ConstructRequestByMetricType(metricsinfo.SystemInfoMetrics)
	if err != nil {
		return err
	}

	// get Query cluster metrics
	group.Go(func() error {
		rsp, err := q.queryCoord.GetMetrics(ctx, req)
		if err != nil {
			return err
		}
		if rsp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
			return fmt.Errorf("quotaCenter get Query cluster failed, err = %s", rsp.GetStatus().GetReason())
		}
		queryCoordTopology := &metricsinfo.QueryCoordTopology{}
		err = metricsinfo.UnmarshalTopology(rsp.GetResponse(), queryCoordTopology)
		if err != nil {
			return err
		}
		for _, queryNodeMetric := range queryCoordTopology.Cluster.ConnectedNodes {
			if queryNodeMetric.QuotaMetrics != nil {
				q.queryNodeMetrics = append(q.queryNodeMetrics, queryNodeMetric.QuotaMetrics)
			}
		}
		return nil
	})
	// get Data cluster metrics
	group.Go(func() error {
		rsp, err := q.dataCoord.GetMetrics(ctx, req)
		if err != nil {
			return err
		}
		if rsp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
			return fmt.Errorf("quotaCenter get Data cluster failed, err = %s", rsp.GetStatus().GetReason())
		}
		dataCoordTopology := &metricsinfo.DataCoordTopology{}
		err = metricsinfo.UnmarshalTopology(rsp.GetResponse(), dataCoordTopology)
		if err != nil {
			return err
		}
		for _, dataNodeMetric := range dataCoordTopology.Cluster.ConnectedNodes {
			if dataNodeMetric.QuotaMetrics != nil {
				q.dataNodeMetrics = append(q.dataNodeMetrics, dataNodeMetric.QuotaMetrics)
			}
		}
		return nil
	})
	// get Proxies metrics
	group.Go(func() error {
		// TODO: get more proxy metrics info
		rsps, err := q.proxies.GetProxyMetrics(ctx)
		if err != nil {
			return err
		}
		for _, rsp := range rsps {
			proxyMetric := &metricsinfo.ProxyInfos{}
			err = metricsinfo.UnmarshalComponentInfos(rsp.GetResponse(), proxyMetric)
			if err != nil {
				return err
			}
			if proxyMetric.QuotaMetrics != nil {
				q.proxyMetrics = append(q.proxyMetrics, proxyMetric.QuotaMetrics)
			}
		}
		return nil
	})
	err = group.Wait()
	if err != nil {
		return err
	}
	log.Debug("QuotaCenter sync metrics done",
		zap.Any("dataNodeMetrics", q.dataNodeMetrics),
		zap.Any("queryNodeMetrics", q.queryNodeMetrics),
		zap.Any("proxyMetrics", q.proxyMetrics))
	return nil
}

// forceDenyWriting sets dml rates to 0 to reject all dml requests.
func (q *QuotaCenter) forceDenyWriting(reason ForceDenyTriggerReason) {
	q.currentRates[internalpb.RateType_DMLInsert] = 0
	q.currentRates[internalpb.RateType_DMLDelete] = 0
	log.Warn("QuotaCenter force to deny writing", zap.String("reason", string(reason)))
}

// forceDenyWriting sets dql rates to 0 to reject all dql requests.
func (q *QuotaCenter) forceDenyReading(reason ForceDenyTriggerReason) {
	q.currentRates[internalpb.RateType_DQLSearch] = 0
	q.currentRates[internalpb.RateType_DQLQuery] = 0
	log.Warn("QuotaCenter force to deny reading", zap.String("reason", string(reason)))
}

// getRealTimeRate return real time rate in Proxy.
func (q *QuotaCenter) getRealTimeRate(rateType internalpb.RateType) float64 {
	var rate float64
	for _, metric := range q.proxyMetrics {
		for _, r := range metric.Rms {
			if r.Label == rateType.String() {
				rate += r.Rate
			}
		}
	}
	return rate
}

// guaranteeMinRate make sure the rate will not be less than the min rate.
func (q *QuotaCenter) guaranteeMinRate(minRate float64, rateType internalpb.RateType) {
	if minRate > 0 && q.currentRates[rateType] < Limit(minRate) {
		q.currentRates[rateType] = Limit(minRate)
	}
}

// calculateReadRates calculates and sets dql rates.
func (q *QuotaCenter) calculateReadRates() {
	if Params.QuotaConfig.ForceDenyReading {
		q.forceDenyReading(ManualForceDeny)
		return
	}
	coolOffSpeed := Params.QuotaConfig.CoolOffSpeed

	coolOff := func(realTimeSearchRate float64, realTimeQueryRate float64) {
		if q.currentRates[internalpb.RateType_DQLSearch] != Inf {
			q.currentRates[internalpb.RateType_DQLSearch] = Limit(realTimeSearchRate * coolOffSpeed)
		}
		if q.currentRates[internalpb.RateType_DQLQuery] != Inf {
			q.currentRates[internalpb.RateType_DQLQuery] = Limit(realTimeQueryRate * coolOffSpeed)
		}
		q.guaranteeMinRate(Params.QuotaConfig.DQLMinSearchRate, internalpb.RateType_DQLSearch)
		q.guaranteeMinRate(Params.QuotaConfig.DQLMinQueryRate, internalpb.RateType_DQLQuery)
	}

	// TODO: unify search and query?
	realTimeSearchRate := q.getRealTimeRate(internalpb.RateType_DQLSearch)
	realTimeQueryRate := q.getRealTimeRate(internalpb.RateType_DQLQuery)

	queueLatencyFactor := q.checkQueryLatency()
	log.Debug("QuotaCenter checkQueryLatency done", zap.Float64("queueLatencyFactor", queueLatencyFactor))
	if Limit(queueLatencyFactor) == Limit(coolOffSpeed) {
		coolOff(realTimeSearchRate, realTimeQueryRate)
		return
	}

	queueLengthFactor := q.checkNQInQuery()
	log.Debug("QuotaCenter checkNQInQuery done", zap.Float64("queueLengthFactor", queueLengthFactor))
	if Limit(queueLengthFactor) == Limit(coolOffSpeed) {
		coolOff(realTimeSearchRate, realTimeQueryRate)
	}
}

// calculateWriteRates calculates and sets dml rates.
func (q *QuotaCenter) calculateWriteRates() error {
	if Params.QuotaConfig.ForceDenyWriting {
		q.forceDenyWriting(ManualForceDeny)
		return nil
	}

	ttFactor, err := q.timeTickDelay()
	if err != nil {
		return err
	}
	if ttFactor <= 0 {
		q.forceDenyWriting(TimeTickLongDelay) // tt protection
		return nil
	}
	log.Debug("QuotaCenter check timeTickDelay done", zap.Float64("ttFactor", ttFactor))

	memFactor := q.memoryToWaterLevel()
	if ttFactor <= 0 {
		q.forceDenyWriting(MemoryExhausted) // memory protection
		return nil
	}
	log.Debug("QuotaCenter check memoryWaterLevel done", zap.Float64("memFactor", memFactor))

	if ttFactor < memFactor {
		ttFactor = memFactor
	}

	if q.currentRates[internalpb.RateType_DMLInsert] != Inf {
		q.currentRates[internalpb.RateType_DMLInsert] *= Limit(ttFactor)
	}
	if q.currentRates[internalpb.RateType_DMLDelete] != Inf {
		q.currentRates[internalpb.RateType_DMLDelete] *= Limit(ttFactor)
	}
	q.guaranteeMinRate(Params.QuotaConfig.DMLMinInsertRate, internalpb.RateType_DMLInsert)
	q.guaranteeMinRate(Params.QuotaConfig.DMLMinDeleteRate, internalpb.RateType_DMLDelete)
	return nil
}

// calculateRates calculates target rates by different strategies.
func (q *QuotaCenter) calculateRates() error {
	q.resetCurrentRates()

	err := q.calculateWriteRates()
	if err != nil {
		return err
	}
	q.calculateReadRates()

	log.Debug("QuotaCenter calculates rate done", zap.Any("rates", q.currentRates))
	return nil
}

// resetCurrentRates resets all current rates to configured rates.
func (q *QuotaCenter) resetCurrentRates() {
	for _, rateType := range internalpb.RateType_value {
		rt := internalpb.RateType(rateType)
		switch rt {
		case internalpb.RateType_DMLInsert:
			q.currentRates[rt] = Limit(Params.QuotaConfig.DMLMaxInsertRate)
		case internalpb.RateType_DMLDelete:
			q.currentRates[rt] = Limit(Params.QuotaConfig.DMLMaxDeleteRate)
		case internalpb.RateType_DQLSearch:
			q.currentRates[rt] = Limit(Params.QuotaConfig.DQLMaxSearchRate)
		case internalpb.RateType_DQLQuery:
			q.currentRates[rt] = Limit(Params.QuotaConfig.DQLMaxQueryRate)
		}
		if q.currentRates[rt] < 0 {
			q.currentRates[rt] = Inf // no limit
		}
	}
}

// timeTickDelay gets time tick delay of DataNodes and QueryNodes,
// and return the factor according to max tolerable time tick delay.
func (q *QuotaCenter) timeTickDelay() (float64, error) {
	maxTt := Params.QuotaConfig.MaxTimeTickDelay
	if maxTt < 0 {
		// < 0 means disable tt protection
		return 1, nil
	}

	minTs := typeutil.MaxTimestamp
	for _, metric := range q.queryNodeMetrics {
		if metric.Fgm.NumFlowGraph > 0 && metric.Fgm.MinFlowGraphTt < minTs {
			minTs = metric.Fgm.MinFlowGraphTt
		}
	}
	for _, metric := range q.dataNodeMetrics {
		if metric.Fgm.NumFlowGraph > 0 && metric.Fgm.MinFlowGraphTt < minTs {
			minTs = metric.Fgm.MinFlowGraphTt
		}
	}
	ts, err := q.tsoAllocator.GenerateTSO(1)
	if err != nil {
		return 0, err
	}
	if minTs >= ts {
		return 1, nil
	}
	t1, _ := tsoutil.ParseTS(minTs)
	t2, _ := tsoutil.ParseTS(ts)
	delay := t2.Sub(t1)
	log.Debug("QuotaCenter check timeTick delay", zap.Time("minTs", t1), zap.Time("curTs", t2), zap.Duration("delay", delay))
	if delay.Nanoseconds() >= maxTt.Nanoseconds() {
		return 0, nil
	}
	return float64(maxTt.Nanoseconds()-delay.Nanoseconds()) / float64(maxTt.Nanoseconds()), nil
}

// checkNQInQuery checks search&query nq in QueryNode,
// and return the factor according to NQInQueueThreshold.
func (q *QuotaCenter) checkNQInQuery() float64 {
	sum := func(ri metricsinfo.ReadInfoInQueue) int64 {
		return ri.UnsolvedQueue + ri.ReadyQueue + ri.ReceiveChan + ri.ExecuteChan
	}

	factor := float64(1)
	nqInQueueThreshold := Params.QuotaConfig.NQInQueueThreshold
	if nqInQueueThreshold < 0 {
		// < 0 means disable queue length protection
		return factor
	}
	for _, metric := range q.queryNodeMetrics {
		searchNQSum := sum(metric.SearchQueue)
		queryTasksSum := sum(metric.QueryQueue)
		nqInQueue := searchNQSum + queryTasksSum // We think of the NQ of query request as 1.
		if nqInQueue >= nqInQueueThreshold {
			return Params.QuotaConfig.CoolOffSpeed
		}
	}
	return factor
}

// checkQueryLatency checks queueing latency in QueryNode for search&query requests,
// and return the factor according to QueueLatencyThreshold.
func (q *QuotaCenter) checkQueryLatency() float64 {
	factor := float64(1)
	queueLatencyThreshold := Params.QuotaConfig.QueueLatencyThreshold
	if queueLatencyThreshold < 0 {
		// < 0 means disable queue latency protection
		return factor
	}
	for _, metric := range q.queryNodeMetrics {
		searchLatency := metric.SearchQueue.AvgQueueDuration
		queryLatency := metric.QueryQueue.AvgQueueDuration
		if float64(searchLatency) >= queueLatencyThreshold || float64(queryLatency) >= queueLatencyThreshold {
			return Params.QuotaConfig.CoolOffSpeed
		}
	}
	return factor
}

// memoryToWaterLevel checks whether any node has memory resource issue,
// and return the factor according to max memory water level.
func (q *QuotaCenter) memoryToWaterLevel() float64 {
	factor := float64(1)
	dataNodeMemoryLowWaterLevel := Params.QuotaConfig.DataNodeMemoryLowWaterLevel
	dataNodeMemoryHighWaterLevel := Params.QuotaConfig.DataNodeMemoryHighWaterLevel
	queryNodeMemoryLowWaterLevel := Params.QuotaConfig.QueryNodeMemoryLowWaterLevel
	queryNodeMemoryHighWaterLevel := Params.QuotaConfig.QueryNodeMemoryHighWaterLevel

	for _, metric := range q.queryNodeMetrics {
		memoryWaterLevel := float64(metric.Hms.MemoryUsage) / float64(metric.Hms.Memory)
		if memoryWaterLevel <= queryNodeMemoryLowWaterLevel {
			continue
		}
		if memoryWaterLevel >= queryNodeMemoryHighWaterLevel {
			log.Debug("QuotaCenter: QueryNode memory to high water level",
				zap.Uint64("UsedMem", metric.Hms.MemoryUsage),
				zap.Uint64("TotalMem", metric.Hms.Memory),
				zap.Float64("QueryNodeMemoryHighWaterLevel", queryNodeMemoryHighWaterLevel))
			return 0
		}
		p := (memoryWaterLevel - queryNodeMemoryLowWaterLevel) / (queryNodeMemoryHighWaterLevel - queryNodeMemoryLowWaterLevel)
		if p < factor {
			factor = p
		}
	}
	for _, metric := range q.dataNodeMetrics {
		memoryWaterLevel := float64(metric.Hms.MemoryUsage) / float64(metric.Hms.Memory)
		if memoryWaterLevel <= dataNodeMemoryLowWaterLevel {
			continue
		}
		if memoryWaterLevel >= dataNodeMemoryHighWaterLevel {
			log.Debug("QuotaCenter: DataNode memory to high water level",
				zap.Uint64("UsedMem", metric.Hms.MemoryUsage),
				zap.Uint64("TotalMem", metric.Hms.Memory),
				zap.Float64("DataNodeMemoryHighWaterLevel", dataNodeMemoryHighWaterLevel))
			return 0
		}
		p := (memoryWaterLevel - dataNodeMemoryLowWaterLevel) / (dataNodeMemoryHighWaterLevel - dataNodeMemoryLowWaterLevel)
		if p < factor {
			factor = p
		}
	}
	return factor
}

// setRates notifies Proxies to set rates for different rate types.
func (q *QuotaCenter) setRates() error {
	ctx, cancel := context.WithTimeout(context.Background(), SetRatesTimeout)
	defer cancel()
	var map2List func() []*internalpb.Rate
	switch q.rateAllocateStrategy {
	case Average:
		map2List = func() []*internalpb.Rate {
			proxyNum := q.proxies.GetProxyNumber()
			if proxyNum == 0 {
				return nil
			}
			rates := make([]*internalpb.Rate, 0, len(q.currentRates))
			for rt, r := range q.currentRates {
				rates = append(rates, &internalpb.Rate{Rt: rt, R: float64(r) / float64(proxyNum)})
			}
			return rates
		}
	case ByRateWeight:
		// TODO: support ByRateWeight
	}
	timestamp := tsoutil.ComposeTSByTime(time.Now(), 0)
	req := &proxypb.SetRatesRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_Undefined,
			MsgID:     int64(timestamp),
			Timestamp: timestamp,
		},
		Rates: map2List(),
	}
	return q.proxies.SetRates(ctx, req)
}

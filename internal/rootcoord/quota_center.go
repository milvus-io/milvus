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
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/tso"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/commonpbutil"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/ratelimitutil"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

const (
	GetMetricsTimeout = 10 * time.Second
	SetRatesTimeout   = 10 * time.Second
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
//  1. DML throughput limitation;
//  2. DDL, DQL qps/rps limitation;
//
// Protections:
//  1. TT protection -> 				dqlRate = maxDQLRate * (maxDelay - ttDelay) / maxDelay
//  2. Memory protection -> 			dmlRate = maxDMLRate * (highMem - curMem) / (highMem - lowMem)
//  3. Disk quota protection ->			force deny writing if exceeded
//  4. DQL Queue length protection ->   dqlRate = curDQLRate * CoolOffSpeed
//  5. DQL queue latency protection ->  dqlRate = curDQLRate * CoolOffSpeed
//  6. Search result protection ->	 	searchRate = curSearchRate * CoolOffSpeed
//
// If necessary, user can also manually force to deny RW requests.
type QuotaCenter struct {
	// clients
	proxies    *proxyClientManager
	queryCoord types.QueryCoord
	dataCoord  types.DataCoord

	// metrics
	queryNodeMetrics map[UniqueID]*metricsinfo.QueryNodeQuotaMetrics
	dataNodeMetrics  map[UniqueID]*metricsinfo.DataNodeQuotaMetrics
	proxyMetrics     map[UniqueID]*metricsinfo.ProxyQuotaMetrics
	dataCoordMetrics *metricsinfo.DataCoordQuotaMetrics

	currentRates map[internalpb.RateType]Limit
	quotaStates  map[milvuspb.QuotaState]commonpb.ErrorCode
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
		quotaStates:  make(map[milvuspb.QuotaState]commonpb.ErrorCode),
		tsoAllocator: tsoAllocator,

		rateAllocateStrategy: DefaultRateAllocateStrategy,
		stopChan:             make(chan struct{}),
	}
}

// run starts the service of QuotaCenter.
func (q *QuotaCenter) run() {
	log.Info("Start QuotaCenter", zap.Float64("collectInterval/s", Params.QuotaConfig.QuotaCenterCollectInterval.GetAsFloat()))
	ticker := time.NewTicker(time.Duration(Params.QuotaConfig.QuotaCenterCollectInterval.GetAsFloat() * float64(time.Second)))
	defer ticker.Stop()
	for {
		select {
		case <-q.stopChan:
			log.Info("QuotaCenter exit")
			return
		case <-ticker.C:
			err := q.syncMetrics()
			if err != nil {
				log.Warn("quotaCenter sync metrics failed", zap.Error(err))
				break
			}
			err = q.calculateRates()
			if err != nil {
				log.Warn("quotaCenter calculate rates failed", zap.Error(err))
				break
			}
			err = q.setRates()
			if err != nil {
				log.Warn("quotaCenter setRates failed", zap.Error(err))
			}
			q.recordMetrics()
		}
	}
}

// stop would stop the service of QuotaCenter.
func (q *QuotaCenter) stop() {
	q.stopOnce.Do(func() {
		q.stopChan <- struct{}{}
	})
}

// clearMetrics removes all metrics stored in QuotaCenter.
func (q *QuotaCenter) clearMetrics() {
	q.dataNodeMetrics = make(map[UniqueID]*metricsinfo.DataNodeQuotaMetrics, 0)
	q.queryNodeMetrics = make(map[UniqueID]*metricsinfo.QueryNodeQuotaMetrics, 0)
	q.proxyMetrics = make(map[UniqueID]*metricsinfo.ProxyQuotaMetrics, 0)
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
				q.queryNodeMetrics[queryNodeMetric.ID] = queryNodeMetric.QuotaMetrics
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
		for _, dataNodeMetric := range dataCoordTopology.Cluster.ConnectedDataNodes {
			if dataNodeMetric.QuotaMetrics != nil {
				q.dataNodeMetrics[dataNodeMetric.ID] = dataNodeMetric.QuotaMetrics
			}
		}
		if dataCoordTopology.Cluster.Self.QuotaMetrics != nil {
			q.dataCoordMetrics = dataCoordTopology.Cluster.Self.QuotaMetrics
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
				q.proxyMetrics[proxyMetric.ID] = proxyMetric.QuotaMetrics
			}
		}
		return nil
	})
	err = group.Wait()
	if err != nil {
		return err
	}
	//log.Debug("QuotaCenter sync metrics done",
	//	zap.Any("dataNodeMetrics", q.dataNodeMetrics),
	//	zap.Any("queryNodeMetrics", q.queryNodeMetrics),
	//	zap.Any("proxyMetrics", q.proxyMetrics),
	//	zap.Any("dataCoordMetrics", q.dataCoordMetrics))
	return nil
}

// forceDenyWriting sets dml rates to 0 to reject all dml requests.
func (q *QuotaCenter) forceDenyWriting(errorCode commonpb.ErrorCode) {
	q.currentRates[internalpb.RateType_DMLInsert] = 0
	q.currentRates[internalpb.RateType_DMLDelete] = 0
	q.currentRates[internalpb.RateType_DMLBulkLoad] = 0
	log.Warn("QuotaCenter force to deny writing", zap.String("reason", errorCode.String()))
	q.quotaStates[milvuspb.QuotaState_DenyToWrite] = errorCode
}

// forceDenyWriting sets dql rates to 0 to reject all dql requests.
func (q *QuotaCenter) forceDenyReading(errorCode commonpb.ErrorCode) {
	q.currentRates[internalpb.RateType_DQLSearch] = 0
	q.currentRates[internalpb.RateType_DQLQuery] = 0
	log.Warn("QuotaCenter force to deny reading", zap.String("reason", errorCode.String()))
	q.quotaStates[milvuspb.QuotaState_DenyToRead] = errorCode
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
	if Params.QuotaConfig.ForceDenyReading.GetAsBool() {
		q.forceDenyReading(commonpb.ErrorCode_ForceDeny)
		return
	}

	coolOffSpeed := Params.QuotaConfig.CoolOffSpeed.GetAsFloat()
	coolOff := func(realTimeSearchRate float64, realTimeQueryRate float64) {
		if q.currentRates[internalpb.RateType_DQLSearch] != Inf && realTimeSearchRate > 0 {
			q.currentRates[internalpb.RateType_DQLSearch] = Limit(realTimeSearchRate * coolOffSpeed)
		}
		if q.currentRates[internalpb.RateType_DQLQuery] != Inf && realTimeSearchRate > 0 {
			q.currentRates[internalpb.RateType_DQLQuery] = Limit(realTimeQueryRate * coolOffSpeed)
		}
		q.guaranteeMinRate(Params.QuotaConfig.DQLMinSearchRate.GetAsFloat(), internalpb.RateType_DQLSearch)
		q.guaranteeMinRate(Params.QuotaConfig.DQLMinQueryRate.GetAsFloat(), internalpb.RateType_DQLQuery)
		log.Warn("QuotaCenter cool read rates off done",
			zap.Any("searchRate", q.currentRates[internalpb.RateType_DQLSearch]),
			zap.Any("queryRate", q.currentRates[internalpb.RateType_DQLQuery]))
		log.Info("QueryNodeMetrics when cool-off", zap.Any("metrics", q.queryNodeMetrics))
	}

	// TODO: unify search and query?
	realTimeSearchRate := q.getRealTimeRate(internalpb.RateType_DQLSearch)
	realTimeQueryRate := q.getRealTimeRate(internalpb.RateType_DQLQuery)

	queueLatencyFactor := q.getQueryLatencyFactor()
	if Limit(queueLatencyFactor) == Limit(coolOffSpeed) {
		coolOff(realTimeSearchRate, realTimeQueryRate)
		return
	}

	queueLengthFactor := q.getNQInQueryFactor()
	if Limit(queueLengthFactor) == Limit(coolOffSpeed) {
		coolOff(realTimeSearchRate, realTimeQueryRate)
		return
	}

	resultRateFactor := q.getReadResultFactor()
	if Limit(resultRateFactor) == Limit(coolOffSpeed) {
		coolOff(realTimeSearchRate, realTimeQueryRate)
	}
}

// calculateWriteRates calculates and sets dml rates.
func (q *QuotaCenter) calculateWriteRates() error {
	if Params.QuotaConfig.ForceDenyWriting.GetAsBool() {
		q.forceDenyWriting(commonpb.ErrorCode_ForceDeny)
		return nil
	}

	exceeded := q.ifDiskQuotaExceeded()
	if exceeded {
		q.forceDenyWriting(commonpb.ErrorCode_DiskQuotaExhausted) // disk quota protection
		return nil
	}

	ts, err := q.tsoAllocator.GenerateTSO(1)
	if err != nil {
		return err
	}
	ttFactor := q.getTimeTickDelayFactor(ts)
	if ttFactor <= 0 {
		q.forceDenyWriting(commonpb.ErrorCode_TimeTickLongDelay) // tt protection
		return nil
	}

	memFactor := q.getMemoryFactor()
	if memFactor <= 0 {
		q.forceDenyWriting(commonpb.ErrorCode_MemoryQuotaExhausted) // memory protection
		return nil
	}

	if memFactor < ttFactor {
		ttFactor = memFactor
	}

	if q.currentRates[internalpb.RateType_DMLInsert] != Inf {
		q.currentRates[internalpb.RateType_DMLInsert] *= Limit(ttFactor)
	}
	if q.currentRates[internalpb.RateType_DMLDelete] != Inf {
		q.currentRates[internalpb.RateType_DMLDelete] *= Limit(ttFactor)
	}
	q.guaranteeMinRate(Params.QuotaConfig.DMLMinInsertRate.GetAsFloat(), internalpb.RateType_DMLInsert)
	q.guaranteeMinRate(Params.QuotaConfig.DMLMinDeleteRate.GetAsFloat(), internalpb.RateType_DMLDelete)
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

	// log.Debug("QuotaCenter calculates rate done", zap.Any("rates", q.currentRates))
	return nil
}

// resetCurrentRates resets all current rates to configured rates.
func (q *QuotaCenter) resetCurrentRates() {
	for _, rateType := range internalpb.RateType_value {
		rt := internalpb.RateType(rateType)
		switch rt {
		case internalpb.RateType_DMLInsert:
			q.currentRates[rt] = Limit(Params.QuotaConfig.DMLMaxInsertRate.GetAsFloat())
		case internalpb.RateType_DMLDelete:
			q.currentRates[rt] = Limit(Params.QuotaConfig.DMLMaxDeleteRate.GetAsFloat())
		case internalpb.RateType_DMLBulkLoad:
			q.currentRates[rt] = Limit(Params.QuotaConfig.DMLMaxBulkLoadRate.GetAsFloat())
		case internalpb.RateType_DQLSearch:
			q.currentRates[rt] = Limit(Params.QuotaConfig.DQLMaxSearchRate.GetAsFloat())
		case internalpb.RateType_DQLQuery:
			q.currentRates[rt] = Limit(Params.QuotaConfig.DQLMaxQueryRate.GetAsFloat())
		}
		if q.currentRates[rt] < 0 {
			q.currentRates[rt] = Inf // no limit
		}
	}
	q.quotaStates = make(map[milvuspb.QuotaState]commonpb.ErrorCode)
}

// getTimeTickDelayFactor gets time tick delay of DataNodes and QueryNodes,
// and return the factor according to max tolerable time tick delay.
func (q *QuotaCenter) getTimeTickDelayFactor(ts Timestamp) float64 {
	var curMaxDelay time.Duration
	var role, vchannel string
	var minTt time.Time

	t1, _ := tsoutil.ParseTS(ts)
	for nodeID, metric := range q.queryNodeMetrics {
		if metric.Fgm.NumFlowGraph > 0 && metric.Fgm.MinFlowGraphChannel != "" {
			t2, _ := tsoutil.ParseTS(metric.Fgm.MinFlowGraphTt)
			delay := t1.Sub(t2)
			if delay.Nanoseconds() > curMaxDelay.Nanoseconds() {
				curMaxDelay = delay
				role = fmt.Sprintf("%s-%d", typeutil.QueryNodeRole, nodeID)
				vchannel = metric.Fgm.MinFlowGraphChannel
				minTt = t2
			}
			metrics.RootCoordTtDelay.WithLabelValues(typeutil.QueryNodeRole, strconv.FormatInt(nodeID, 10)).Set(float64(curMaxDelay.Milliseconds()))
		}
	}
	for nodeID, metric := range q.dataNodeMetrics {
		if metric.Fgm.NumFlowGraph > 0 && metric.Fgm.MinFlowGraphChannel != "" {
			t2, _ := tsoutil.ParseTS(metric.Fgm.MinFlowGraphTt)
			delay := t1.Sub(t2)
			if delay.Nanoseconds() > curMaxDelay.Nanoseconds() {
				curMaxDelay = delay
				role = fmt.Sprintf("%s-%d", typeutil.DataNodeRole, nodeID)
				vchannel = metric.Fgm.MinFlowGraphChannel
				minTt = t2
			}
			metrics.RootCoordTtDelay.WithLabelValues(typeutil.DataNodeRole, strconv.FormatInt(nodeID, 10)).Set(float64(curMaxDelay.Milliseconds()))
		}
	}

	if !Params.QuotaConfig.TtProtectionEnabled.GetAsBool() {
		return 1
	}

	maxDelay := Params.QuotaConfig.MaxTimeTickDelay.GetAsDuration(time.Second)
	if maxDelay < 0 {
		// < 0 means disable tt protection
		return 1
	}

	if curMaxDelay.Nanoseconds() >= maxDelay.Nanoseconds() {
		log.Warn("QuotaCenter force deny writing due to long timeTick delay",
			zap.String("node", role),
			zap.String("vchannel", vchannel),
			zap.Time("curTs", t1),
			zap.Time("minTs", minTt),
			zap.Duration("delay", curMaxDelay),
			zap.Duration("MaxDelay", maxDelay))
		log.Info("DataNode and QueryNode Metrics",
			zap.Any("QueryNodeMetrics", q.queryNodeMetrics),
			zap.Any("DataNodeMetrics", q.dataNodeMetrics))
		return 0
	}
	factor := float64(maxDelay.Nanoseconds()-curMaxDelay.Nanoseconds()) / float64(maxDelay.Nanoseconds())
	if factor <= 0.9 {
		log.Warn("QuotaCenter: limit writing due to long timeTick delay",
			zap.String("node", role),
			zap.String("vchannel", vchannel),
			zap.Time("curTs", t1),
			zap.Time("minTs", minTt),
			zap.Duration("delay", curMaxDelay),
			zap.Duration("MaxDelay", maxDelay),
			zap.Float64("factor", factor))
	}
	return factor
}

// getNQInQueryFactor checks search&query nq in QueryNode,
// and return the factor according to NQInQueueThreshold.
func (q *QuotaCenter) getNQInQueryFactor() float64 {
	if !Params.QuotaConfig.QueueProtectionEnabled.GetAsBool() {
		return 1
	}

	sum := func(ri metricsinfo.ReadInfoInQueue) int64 {
		return ri.UnsolvedQueue + ri.ReadyQueue + ri.ReceiveChan + ri.ExecuteChan
	}

	nqInQueueThreshold := Params.QuotaConfig.NQInQueueThreshold.GetAsInt64()
	if nqInQueueThreshold < 0 {
		// < 0 means disable queue length protection
		return 1
	}
	for _, metric := range q.queryNodeMetrics {
		searchNQSum := sum(metric.SearchQueue)
		queryTasksSum := sum(metric.QueryQueue)
		nqInQueue := searchNQSum + queryTasksSum // We think of the NQ of query request as 1.
		if nqInQueue >= nqInQueueThreshold {
			return Params.QuotaConfig.CoolOffSpeed.GetAsFloat()
		}
	}
	return 1
}

// getQueryLatencyFactor checks queueing latency in QueryNode for search&query requests,
// and return the factor according to QueueLatencyThreshold.
func (q *QuotaCenter) getQueryLatencyFactor() float64 {
	if !Params.QuotaConfig.QueueProtectionEnabled.GetAsBool() {
		return 1
	}

	queueLatencyThreshold := Params.QuotaConfig.QueueLatencyThreshold.GetAsDuration(time.Second)
	if queueLatencyThreshold < 0 {
		// < 0 means disable queue latency protection
		return 1
	}
	for _, metric := range q.queryNodeMetrics {
		searchLatency := metric.SearchQueue.AvgQueueDuration
		queryLatency := metric.QueryQueue.AvgQueueDuration
		if searchLatency >= queueLatencyThreshold || queryLatency >= queueLatencyThreshold {
			return Params.QuotaConfig.CoolOffSpeed.GetAsFloat()
		}
	}
	return 1
}

// getReadResultFactor checks search result rate in Proxy,
// and return the factor according to MaxReadResultRate.
func (q *QuotaCenter) getReadResultFactor() float64 {
	if !Params.QuotaConfig.ResultProtectionEnabled.GetAsBool() {
		return 1
	}

	maxRate := Params.QuotaConfig.MaxReadResultRate.GetAsFloat()
	rateCount := float64(0)
	for _, metric := range q.proxyMetrics {
		for _, rm := range metric.Rms {
			if rm.Label == metricsinfo.ReadResultThroughput {
				rateCount += rm.Rate
			}
		}
	}
	if rateCount >= maxRate {
		return Params.QuotaConfig.CoolOffSpeed.GetAsFloat()
	}
	return 1
}

// getMemoryFactor checks whether any node has memory resource issue,
// and return the factor according to max memory water level.
func (q *QuotaCenter) getMemoryFactor() float64 {
	factor := float64(1)
	if !Params.QuotaConfig.MemProtectionEnabled.GetAsBool() {
		return 1
	}

	dataNodeMemoryLowWaterLevel := Params.QuotaConfig.DataNodeMemoryLowWaterLevel.GetAsFloat()
	dataNodeMemoryHighWaterLevel := Params.QuotaConfig.DataNodeMemoryHighWaterLevel.GetAsFloat()
	queryNodeMemoryLowWaterLevel := Params.QuotaConfig.QueryNodeMemoryLowWaterLevel.GetAsFloat()
	queryNodeMemoryHighWaterLevel := Params.QuotaConfig.QueryNodeMemoryHighWaterLevel.GetAsFloat()

	for nodeID, metric := range q.queryNodeMetrics {
		memoryWaterLevel := float64(metric.Hms.MemoryUsage) / float64(metric.Hms.Memory)
		if memoryWaterLevel <= queryNodeMemoryLowWaterLevel {
			continue
		}
		if memoryWaterLevel >= queryNodeMemoryHighWaterLevel {
			log.Warn("QuotaCenter: QueryNode memory to high water level",
				zap.String("Node", fmt.Sprintf("%s-%d", typeutil.QueryNodeRole, nodeID)),
				zap.Uint64("UsedMem", metric.Hms.MemoryUsage),
				zap.Uint64("TotalMem", metric.Hms.Memory),
				zap.Float64("memoryWaterLevel", memoryWaterLevel),
				zap.Float64("memoryHighWaterLevel", queryNodeMemoryHighWaterLevel))
			return 0
		}
		p := (queryNodeMemoryHighWaterLevel - memoryWaterLevel) / (queryNodeMemoryHighWaterLevel - queryNodeMemoryLowWaterLevel)
		if p < factor {
			log.Warn("QuotaCenter: QueryNode memory to low water level, limit writing rate",
				zap.String("Node", fmt.Sprintf("%s-%d", typeutil.QueryNodeRole, nodeID)),
				zap.Uint64("UsedMem", metric.Hms.MemoryUsage),
				zap.Uint64("TotalMem", metric.Hms.Memory),
				zap.Float64("memoryWaterLevel", memoryWaterLevel),
				zap.Float64("memoryLowWaterLevel", queryNodeMemoryLowWaterLevel))
			factor = p
		}
	}
	for nodeID, metric := range q.dataNodeMetrics {
		memoryWaterLevel := float64(metric.Hms.MemoryUsage) / float64(metric.Hms.Memory)
		if memoryWaterLevel <= dataNodeMemoryLowWaterLevel {
			continue
		}
		if memoryWaterLevel >= dataNodeMemoryHighWaterLevel {
			log.Warn("QuotaCenter: DataNode memory to high water level",
				zap.String("Node", fmt.Sprintf("%s-%d", typeutil.DataNodeRole, nodeID)),
				zap.Uint64("UsedMem", metric.Hms.MemoryUsage),
				zap.Uint64("TotalMem", metric.Hms.Memory),
				zap.Float64("memoryWaterLevel", memoryWaterLevel),
				zap.Float64("memoryHighWaterLevel", dataNodeMemoryHighWaterLevel))
			return 0
		}
		p := (dataNodeMemoryHighWaterLevel - memoryWaterLevel) / (dataNodeMemoryHighWaterLevel - dataNodeMemoryLowWaterLevel)
		if p < factor {
			log.Warn("QuotaCenter: DataNode memory to low water level, limit writing rate",
				zap.String("Node", fmt.Sprintf("%s-%d", typeutil.DataNodeRole, nodeID)),
				zap.Uint64("UsedMem", metric.Hms.MemoryUsage),
				zap.Uint64("TotalMem", metric.Hms.Memory),
				zap.Float64("memoryWaterLevel", memoryWaterLevel),
				zap.Float64("memoryLowWaterLevel", dataNodeMemoryLowWaterLevel))
			factor = p
		}
	}
	return factor
}

// ifDiskQuotaExceeded checks if disk quota exceeded.
func (q *QuotaCenter) ifDiskQuotaExceeded() bool {
	if !Params.QuotaConfig.DiskProtectionEnabled.GetAsBool() {
		return false
	}
	if q.dataCoordMetrics == nil {
		return false
	}
	diskQuota := Params.QuotaConfig.DiskQuota.GetAsFloat()
	totalSize := q.dataCoordMetrics.TotalBinlogSize
	if float64(totalSize) >= diskQuota {
		log.Warn("QuotaCenter: disk quota exceeded",
			zap.Int64("curDiskUsage", totalSize),
			zap.Float64("diskQuota", diskQuota))
		log.Info("DataCoordMetric",
			zap.Any("metric", q.dataCoordMetrics))
		return true
	}
	return false
}

// setRates notifies Proxies to set rates for different rate types.
func (q *QuotaCenter) setRates() error {
	ctx, cancel := context.WithTimeout(context.Background(), SetRatesTimeout)
	defer cancel()
	var map2List func() []*internalpb.Rate
	switch q.rateAllocateStrategy {
	case Average:
		map2List = func() []*internalpb.Rate {
			proxyNum := q.proxies.GetProxyCount()
			if proxyNum == 0 {
				return nil
			}
			rates := make([]*internalpb.Rate, 0, len(q.currentRates))
			for rt, r := range q.currentRates {
				if r == Inf {
					rates = append(rates, &internalpb.Rate{Rt: rt, R: float64(r)})
				} else {
					rates = append(rates, &internalpb.Rate{Rt: rt, R: float64(r) / float64(proxyNum)})
				}
			}
			return rates
		}
	case ByRateWeight:
		// TODO: support ByRateWeight
	}
	states := make([]milvuspb.QuotaState, 0, len(q.quotaStates))
	codes := make([]commonpb.ErrorCode, 0, len(q.quotaStates))
	for k, v := range q.quotaStates {
		states = append(states, k)
		codes = append(codes, v)
	}
	timestamp := tsoutil.ComposeTSByTime(time.Now(), 0)
	req := &proxypb.SetRatesRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgID(int64(timestamp)),
			commonpbutil.WithTimeStamp(timestamp),
		),
		Rates:  map2List(),
		States: states,
		Codes:  codes,
	}
	return q.proxies.SetRates(ctx, req)
}

// recordMetrics records metrics of quota states.
func (q *QuotaCenter) recordMetrics() {
	record := func(errorCode commonpb.ErrorCode) {
		for _, v := range q.quotaStates {
			if v == errorCode {
				metrics.RootCoordQuotaStates.WithLabelValues(errorCode.String()).Set(1)
				return
			}
		}
		metrics.RootCoordQuotaStates.WithLabelValues(errorCode.String()).Set(0)
	}
	record(commonpb.ErrorCode_MemoryQuotaExhausted)
	record(commonpb.ErrorCode_DiskQuotaExhausted)
	record(commonpb.ErrorCode_TimeTickLongDelay)
}

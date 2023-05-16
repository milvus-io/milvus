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
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/tso"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/ratelimitutil"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"github.com/samber/lo"
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

type collectionRates = map[internalpb.RateType]Limit

type collectionStates = map[milvuspb.QuotaState]commonpb.ErrorCode

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
//  7. GrowingSegsSize protection ->    dmlRate = maxDMLRate * (high - cur) / (high - low)
//
// If necessary, user can also manually force to deny RW requests.
type QuotaCenter struct {
	// clients
	proxies    *proxyClientManager
	queryCoord types.QueryCoord
	dataCoord  types.DataCoord
	meta       IMetaTable

	// metrics
	queryNodeMetrics map[UniqueID]*metricsinfo.QueryNodeQuotaMetrics
	dataNodeMetrics  map[UniqueID]*metricsinfo.DataNodeQuotaMetrics
	proxyMetrics     map[UniqueID]*metricsinfo.ProxyQuotaMetrics
	diskMu           sync.Mutex // guards dataCoordMetrics and totalBinlogSize
	dataCoordMetrics *metricsinfo.DataCoordQuotaMetrics
	totalBinlogSize  int64

	readableCollections []int64
	writableCollections []int64

	currentRates map[int64]collectionRates
	quotaStates  map[int64]collectionStates
	tsoAllocator tso.Allocator

	rateAllocateStrategy RateAllocateStrategy

	stopOnce sync.Once
	stopChan chan struct{}
}

// NewQuotaCenter returns a new QuotaCenter.
func NewQuotaCenter(proxies *proxyClientManager, queryCoord types.QueryCoord, dataCoord types.DataCoord, tsoAllocator tso.Allocator, meta IMetaTable) *QuotaCenter {
	return &QuotaCenter{
		proxies:             proxies,
		queryCoord:          queryCoord,
		dataCoord:           dataCoord,
		currentRates:        make(map[int64]map[internalpb.RateType]Limit),
		quotaStates:         make(map[int64]map[milvuspb.QuotaState]commonpb.ErrorCode),
		tsoAllocator:        tsoAllocator,
		meta:                meta,
		readableCollections: make([]int64, 0),
		writableCollections: make([]int64, 0),

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

		collections := typeutil.NewUniqueSet()
		for _, queryNodeMetric := range queryCoordTopology.Cluster.ConnectedNodes {
			if queryNodeMetric.QuotaMetrics != nil {
				q.queryNodeMetrics[queryNodeMetric.ID] = queryNodeMetric.QuotaMetrics
				collections.Insert(queryNodeMetric.QuotaMetrics.Effect.CollectionIDs...)
			}
		}
		q.readableCollections = collections.Collect()
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

		collections := typeutil.NewUniqueSet()
		for _, dataNodeMetric := range dataCoordTopology.Cluster.ConnectedDataNodes {
			if dataNodeMetric.QuotaMetrics != nil {
				q.dataNodeMetrics[dataNodeMetric.ID] = dataNodeMetric.QuotaMetrics
				collections.Insert(dataNodeMetric.QuotaMetrics.Effect.CollectionIDs...)
			}
		}
		q.writableCollections = collections.Collect()
		q.diskMu.Lock()
		if dataCoordTopology.Cluster.Self.QuotaMetrics != nil {
			q.dataCoordMetrics = dataCoordTopology.Cluster.Self.QuotaMetrics
		}
		q.diskMu.Unlock()
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
func (q *QuotaCenter) forceDenyWriting(errorCode commonpb.ErrorCode, collections ...int64) {
	if len(collections) == 0 && len(q.writableCollections) != 0 {
		// default to all writable collections
		collections = q.writableCollections
	}
	for _, collection := range collections {
		if _, ok := q.currentRates[collection]; !ok {
			q.currentRates[collection] = make(map[internalpb.RateType]Limit)
			q.quotaStates[collection] = make(map[milvuspb.QuotaState]commonpb.ErrorCode)
		}
		q.currentRates[collection][internalpb.RateType_DMLInsert] = 0
		q.currentRates[collection][internalpb.RateType_DMLDelete] = 0
		q.currentRates[collection][internalpb.RateType_DMLBulkLoad] = 0
		q.quotaStates[collection][milvuspb.QuotaState_DenyToWrite] = errorCode
	}
	log.RatedWarn(10, "QuotaCenter force to deny writing",
		zap.Int64s("collectionIDs", collections),
		zap.String("reason", errorCode.String()))
}

// forceDenyReading sets dql rates to 0 to reject all dql requests.
func (q *QuotaCenter) forceDenyReading(errorCode commonpb.ErrorCode, collections ...int64) {
	if len(collections) == 0 {
		// default to all readable collections
		collections = q.readableCollections
	}
	for _, collection := range collections {
		if _, ok := q.currentRates[collection]; !ok {
			q.currentRates[collection] = make(map[internalpb.RateType]Limit)
			q.quotaStates[collection] = make(map[milvuspb.QuotaState]commonpb.ErrorCode)
		}
		q.currentRates[collection][internalpb.RateType_DQLSearch] = 0
		q.currentRates[collection][internalpb.RateType_DQLQuery] = 0
		q.quotaStates[collection][milvuspb.QuotaState_DenyToRead] = errorCode
	}
	log.Warn("QuotaCenter force to deny reading",
		zap.Int64s("collectionIDs", collections),
		zap.String("reason", errorCode.String()))
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
func (q *QuotaCenter) guaranteeMinRate(minRate float64, rateType internalpb.RateType, collections ...int64) {
	for _, collection := range collections {
		if minRate > 0 && q.currentRates[collection][rateType] < Limit(minRate) {
			q.currentRates[collection][rateType] = Limit(minRate)
		}
	}
}

// calculateReadRates calculates and sets dql rates.
func (q *QuotaCenter) calculateReadRates() {
	log := log.Ctx(context.Background()).WithRateGroup("rootcoord.QuotaCenter", 1.0, 60.0)
	if Params.QuotaConfig.ForceDenyReading.GetAsBool() {
		q.forceDenyReading(commonpb.ErrorCode_ForceDeny)
		return
	}

	limitCollectionSet := typeutil.NewUniqueSet()
	enableQueueProtection := Params.QuotaConfig.QueueProtectionEnabled.GetAsBool()
	// query latency
	queueLatencyThreshold := Params.QuotaConfig.QueueLatencyThreshold.GetAsDuration(time.Second)
	// enableQueueProtection && queueLatencyThreshold >= 0 means enable queue latency protection
	if queueLatencyThreshold >= 0 {
		for _, metric := range q.queryNodeMetrics {
			searchLatency := metric.SearchQueue.AvgQueueDuration
			queryLatency := metric.QueryQueue.AvgQueueDuration
			if searchLatency >= queueLatencyThreshold || queryLatency >= queueLatencyThreshold {
				limitCollectionSet.Insert(metric.Effect.CollectionIDs...)
			}
		}
	}

	// queue length
	nqInQueueThreshold := Params.QuotaConfig.NQInQueueThreshold.GetAsInt64()
	if enableQueueProtection && nqInQueueThreshold >= 0 {
		// >= 0 means enable queue length protection
		sum := func(ri metricsinfo.ReadInfoInQueue) int64 {
			return ri.UnsolvedQueue + ri.ReadyQueue + ri.ReceiveChan + ri.ExecuteChan
		}
		for _, metric := range q.queryNodeMetrics {
			// We think of the NQ of query request as 1.
			// search use same queue length counter with query
			if sum(metric.SearchQueue) >= nqInQueueThreshold {
				limitCollectionSet.Insert(metric.Effect.CollectionIDs...)
			}
		}
	}

	// read result
	enableResultProtection := Params.QuotaConfig.ResultProtectionEnabled.GetAsBool()
	if enableResultProtection {
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
			limitCollectionSet.Insert(q.readableCollections...)
		}
	}

	coolOffSpeed := Params.QuotaConfig.CoolOffSpeed.GetAsFloat()
	coolOff := func(realTimeSearchRate float64, realTimeQueryRate float64, collections ...int64) {
		for _, collection := range collections {
			if q.currentRates[collection][internalpb.RateType_DQLSearch] != Inf && realTimeSearchRate > 0 {
				q.currentRates[collection][internalpb.RateType_DQLSearch] = Limit(realTimeSearchRate * coolOffSpeed)
				log.RatedWarn(10, "QuotaCenter cool read rates off done",
					zap.Int64("collectionID", collection),
					zap.Any("searchRate", q.currentRates[collection][internalpb.RateType_DQLSearch]))
			}
			if q.currentRates[collection][internalpb.RateType_DQLQuery] != Inf && realTimeQueryRate > 0 {
				q.currentRates[collection][internalpb.RateType_DQLQuery] = Limit(realTimeQueryRate * coolOffSpeed)
				log.RatedWarn(10, "QuotaCenter cool read rates off done",
					zap.Int64("collectionID", collection),
					zap.Any("queryRate", q.currentRates[collection][internalpb.RateType_DQLQuery]))
			}

			collectionProps := q.getCollectionLimitConfig(collection)
			q.guaranteeMinRate(getCollectionRateLimitConfig(collectionProps, common.CollectionSearchRateMinKey), internalpb.RateType_DQLSearch, collection)
			q.guaranteeMinRate(getCollectionRateLimitConfig(collectionProps, common.CollectionQueryRateMinKey), internalpb.RateType_DQLQuery, collection)
		}

		log.RatedInfo(10, "QueryNodeMetrics when cool-off",
			zap.Any("metrics", q.queryNodeMetrics))
	}

	// TODO: unify search and query?
	realTimeSearchRate := q.getRealTimeRate(internalpb.RateType_DQLSearch)
	realTimeQueryRate := q.getRealTimeRate(internalpb.RateType_DQLQuery)
	coolOff(realTimeSearchRate, realTimeQueryRate, limitCollectionSet.Collect()...)
}

// calculateWriteRates calculates and sets dml rates.
func (q *QuotaCenter) calculateWriteRates() error {
	log := log.Ctx(context.Background()).WithRateGroup("rootcoord.QuotaCenter", 1.0, 60.0)
	if Params.QuotaConfig.ForceDenyWriting.GetAsBool() {
		q.forceDenyWriting(commonpb.ErrorCode_ForceDeny)
		return nil
	}

	q.checkDiskQuota()

	ts, err := q.tsoAllocator.GenerateTSO(1)
	if err != nil {
		return err
	}

	collectionFactors := make(map[int64]float64)
	updateCollectionFactor := func(factors map[int64]float64) {
		for collection, factor := range factors {
			_, ok := collectionFactors[collection]
			if !ok || collectionFactors[collection] > factor {
				collectionFactors[collection] = factor
			}
		}
	}

	ttFactors := q.getTimeTickDelayFactor(ts)
	updateCollectionFactor(ttFactors)
	memFactors := q.getMemoryFactor()
	updateCollectionFactor(memFactors)
	growingSegFactors := q.getGrowingSegmentsSizeFactor()
	updateCollectionFactor(growingSegFactors)

	for collection, factor := range collectionFactors {
		if factor <= 0 {
			if _, ok := ttFactors[collection]; ok && factor == ttFactors[collection] {
				// factor comes from ttFactor
				q.forceDenyWriting(commonpb.ErrorCode_TimeTickLongDelay, collection)
			} else {
				// factor comes from memFactor or growingSegFactor, all about mem exhausted
				q.forceDenyWriting(commonpb.ErrorCode_MemoryQuotaExhausted, collection)
			}
		}

		if q.currentRates[collection][internalpb.RateType_DMLInsert] != Inf {
			q.currentRates[collection][internalpb.RateType_DMLInsert] *= Limit(factor)
		}
		if q.currentRates[collection][internalpb.RateType_DMLDelete] != Inf {
			q.currentRates[collection][internalpb.RateType_DMLDelete] *= Limit(factor)
		}

		collectionProps := q.getCollectionLimitConfig(collection)
		q.guaranteeMinRate(getCollectionRateLimitConfig(collectionProps, common.CollectionInsertRateMinKey), internalpb.RateType_DMLInsert, collection)
		q.guaranteeMinRate(getCollectionRateLimitConfig(collectionProps, common.CollectionDeleteRateMinKey), internalpb.RateType_DMLDelete, collection)
		log.RatedDebug(10, "QuotaCenter cool write rates off done",
			zap.Int64("collectionID", collection),
			zap.Float64("factor", factor))
	}

	return nil
}

func (q *QuotaCenter) getTimeTickDelayFactor(ts Timestamp) map[int64]float64 {
	log := log.Ctx(context.Background()).WithRateGroup("rootcoord.QuotaCenter", 1.0, 60.0)
	if !Params.QuotaConfig.TtProtectionEnabled.GetAsBool() {
		return make(map[int64]float64)
	}

	maxDelay := Params.QuotaConfig.MaxTimeTickDelay.GetAsDuration(time.Second)
	if maxDelay < 0 {
		// < 0 means disable tt protection
		return make(map[int64]float64)
	}

	collectionsMaxDelay := make(map[int64]time.Duration)
	updateCollectionDelay := func(delay time.Duration, collections []int64) {
		for _, collection := range collections {
			_, ok := collectionsMaxDelay[collection]
			if !ok || collectionsMaxDelay[collection] < delay {
				collectionsMaxDelay[collection] = delay
			}
		}
	}

	t1, _ := tsoutil.ParseTS(ts)
	for nodeID, metric := range q.queryNodeMetrics {
		if metric.Fgm.NumFlowGraph > 0 && metric.Fgm.MinFlowGraphChannel != "" {
			t2, _ := tsoutil.ParseTS(metric.Fgm.MinFlowGraphTt)
			delay := t1.Sub(t2)
			updateCollectionDelay(delay, metric.Effect.CollectionIDs)
			metrics.RootCoordTtDelay.WithLabelValues(typeutil.QueryNodeRole, strconv.FormatInt(nodeID, 10)).Set(float64(delay.Milliseconds()))
		}
	}
	for nodeID, metric := range q.dataNodeMetrics {
		if metric.Fgm.NumFlowGraph > 0 && metric.Fgm.MinFlowGraphChannel != "" {
			t2, _ := tsoutil.ParseTS(metric.Fgm.MinFlowGraphTt)
			delay := t1.Sub(t2)
			updateCollectionDelay(delay, metric.Effect.CollectionIDs)
			metrics.RootCoordTtDelay.WithLabelValues(typeutil.DataNodeRole, strconv.FormatInt(nodeID, 10)).Set(float64(delay.Milliseconds()))
		}
	}

	collectionFactor := make(map[int64]float64)
	for collectionID, curMaxDelay := range collectionsMaxDelay {
		if curMaxDelay.Nanoseconds() >= maxDelay.Nanoseconds() {
			log.RatedWarn(10, "QuotaCenter force deny writing due to long timeTick delay",
				zap.Int64("collectionID", collectionID),
				zap.Time("curTs", t1),
				zap.Duration("delay", curMaxDelay),
				zap.Duration("MaxDelay", maxDelay))
			log.RatedInfo(10, "DataNode and QueryNode Metrics",
				zap.Any("QueryNodeMetrics", q.queryNodeMetrics),
				zap.Any("DataNodeMetrics", q.dataNodeMetrics))
			collectionFactor[collectionID] = 0
			continue
		}
		factor := float64(maxDelay.Nanoseconds()-curMaxDelay.Nanoseconds()) / float64(maxDelay.Nanoseconds())
		if factor <= 0.9 {
			log.RatedWarn(10, "QuotaCenter: limit writing due to long timeTick delay",
				zap.Int64("collectionID", collectionID),
				zap.Time("curTs", t1),
				zap.Duration("delay", curMaxDelay),
				zap.Duration("MaxDelay", maxDelay),
				zap.Float64("factor", factor))
		}

		collectionFactor[collectionID] = factor
	}

	return collectionFactor
}

// getMemoryFactor checks whether any node has memory resource issue,
// and return the factor according to max memory water level.
func (q *QuotaCenter) getMemoryFactor() map[int64]float64 {
	log := log.Ctx(context.Background()).WithRateGroup("rootcoord.QuotaCenter", 1.0, 60.0)
	if !Params.QuotaConfig.MemProtectionEnabled.GetAsBool() {
		return make(map[int64]float64)
	}

	dataNodeMemoryLowWaterLevel := Params.QuotaConfig.DataNodeMemoryLowWaterLevel.GetAsFloat()
	dataNodeMemoryHighWaterLevel := Params.QuotaConfig.DataNodeMemoryHighWaterLevel.GetAsFloat()
	queryNodeMemoryLowWaterLevel := Params.QuotaConfig.QueryNodeMemoryLowWaterLevel.GetAsFloat()
	queryNodeMemoryHighWaterLevel := Params.QuotaConfig.QueryNodeMemoryHighWaterLevel.GetAsFloat()

	collectionFactor := make(map[int64]float64)
	updateCollectionFactor := func(factor float64, collections []int64) {
		for _, collection := range collections {
			_, ok := collectionFactor[collection]
			if !ok || collectionFactor[collection] > factor {
				collectionFactor[collection] = factor
			}
		}
	}
	for nodeID, metric := range q.queryNodeMetrics {
		memoryWaterLevel := float64(metric.Hms.MemoryUsage) / float64(metric.Hms.Memory)
		if memoryWaterLevel <= queryNodeMemoryLowWaterLevel {
			continue
		}
		if memoryWaterLevel >= queryNodeMemoryHighWaterLevel {
			log.RatedWarn(10, "QuotaCenter: QueryNode memory to high water level",
				zap.String("Node", fmt.Sprintf("%s-%d", typeutil.QueryNodeRole, nodeID)),
				zap.Int64s("collections", metric.Effect.CollectionIDs),
				zap.Uint64("UsedMem", metric.Hms.MemoryUsage),
				zap.Uint64("TotalMem", metric.Hms.Memory),
				zap.Float64("memoryWaterLevel", memoryWaterLevel),
				zap.Float64("memoryHighWaterLevel", queryNodeMemoryHighWaterLevel))
			updateCollectionFactor(0, metric.Effect.CollectionIDs)
			continue
		}
		factor := (queryNodeMemoryHighWaterLevel - memoryWaterLevel) / (queryNodeMemoryHighWaterLevel - queryNodeMemoryLowWaterLevel)
		updateCollectionFactor(factor, metric.Effect.CollectionIDs)
		log.RatedWarn(10, "QuotaCenter: QueryNode memory to low water level, limit writing rate",
			zap.String("Node", fmt.Sprintf("%s-%d", typeutil.QueryNodeRole, nodeID)),
			zap.Int64s("collections", metric.Effect.CollectionIDs),
			zap.Uint64("UsedMem", metric.Hms.MemoryUsage),
			zap.Uint64("TotalMem", metric.Hms.Memory),
			zap.Float64("memoryWaterLevel", memoryWaterLevel),
			zap.Float64("memoryLowWaterLevel", queryNodeMemoryLowWaterLevel))
	}
	for nodeID, metric := range q.dataNodeMetrics {
		memoryWaterLevel := float64(metric.Hms.MemoryUsage) / float64(metric.Hms.Memory)
		if memoryWaterLevel <= dataNodeMemoryLowWaterLevel {
			continue
		}
		if memoryWaterLevel >= dataNodeMemoryHighWaterLevel {
			log.RatedWarn(10, "QuotaCenter: DataNode memory to high water level",
				zap.String("Node", fmt.Sprintf("%s-%d", typeutil.DataNodeRole, nodeID)),
				zap.Int64s("collections", metric.Effect.CollectionIDs),
				zap.Uint64("UsedMem", metric.Hms.MemoryUsage),
				zap.Uint64("TotalMem", metric.Hms.Memory),
				zap.Float64("memoryWaterLevel", memoryWaterLevel),
				zap.Float64("memoryHighWaterLevel", dataNodeMemoryHighWaterLevel))
			updateCollectionFactor(0, metric.Effect.CollectionIDs)
			continue
		}
		factor := (dataNodeMemoryHighWaterLevel - memoryWaterLevel) / (dataNodeMemoryHighWaterLevel - dataNodeMemoryLowWaterLevel)
		log.RatedWarn(10, "QuotaCenter: DataNode memory to low water level, limit writing rate",
			zap.String("Node", fmt.Sprintf("%s-%d", typeutil.DataNodeRole, nodeID)),
			zap.Int64s("collections", metric.Effect.CollectionIDs),
			zap.Uint64("UsedMem", metric.Hms.MemoryUsage),
			zap.Uint64("TotalMem", metric.Hms.Memory),
			zap.Float64("memoryWaterLevel", memoryWaterLevel),
			zap.Float64("memoryLowWaterLevel", dataNodeMemoryLowWaterLevel))
		updateCollectionFactor(factor, metric.Effect.CollectionIDs)
	}
	return collectionFactor
}

func (q *QuotaCenter) getGrowingSegmentsSizeFactor() map[int64]float64 {
	log := log.Ctx(context.Background()).WithRateGroup("rootcoord.QuotaCenter", 1.0, 60.0)
	if !Params.QuotaConfig.GrowingSegmentsSizeProtectionEnabled.GetAsBool() {
		return make(map[int64]float64)
	}

	low := Params.QuotaConfig.GrowingSegmentsSizeLowWaterLevel.GetAsFloat()
	high := Params.QuotaConfig.GrowingSegmentsSizeHighWaterLevel.GetAsFloat()

	collectionFactor := make(map[int64]float64)
	updateCollectionFactor := func(factor float64, collections []int64) {
		for _, collection := range collections {
			_, ok := collectionFactor[collection]
			if !ok || collectionFactor[collection] > factor {
				collectionFactor[collection] = factor
			}
		}
	}
	for nodeID, metric := range q.queryNodeMetrics {
		cur := float64(metric.GrowingSegmentsSize) / float64(metric.Hms.Memory)
		if cur <= low {
			continue
		}
		if cur >= high {
			log.RatedWarn(10, "QuotaCenter: QueryNode growing segments size to high water level",
				zap.String("Node", fmt.Sprintf("%s-%d", typeutil.QueryNodeRole, nodeID)),
				zap.Int64s("collections", metric.Effect.CollectionIDs),
				zap.Int64("segmentsSize", metric.GrowingSegmentsSize),
				zap.Uint64("TotalMem", metric.Hms.Memory),
				zap.Float64("highWaterLevel", high))
			updateCollectionFactor(0, metric.Effect.CollectionIDs)
			continue
		}
		factor := (high - cur) / (high - low)
		updateCollectionFactor(factor, metric.Effect.CollectionIDs)
		log.RatedWarn(10, "QuotaCenter: QueryNode growing segments size to low water level, limit writing rate",
			zap.String("Node", fmt.Sprintf("%s-%d", typeutil.QueryNodeRole, nodeID)),
			zap.Int64s("collections", metric.Effect.CollectionIDs),
			zap.Int64("segmentsSize", metric.GrowingSegmentsSize),
			zap.Uint64("TotalMem", metric.Hms.Memory),
			zap.Float64("lowWaterLevel", low))
	}
	return collectionFactor
}

// calculateRates calculates target rates by different strategies.
func (q *QuotaCenter) calculateRates() error {
	q.resetAllCurrentRates()

	err := q.calculateWriteRates()
	if err != nil {
		return err
	}
	q.calculateReadRates()

	// log.Debug("QuotaCenter calculates rate done", zap.Any("rates", q.currentRates))
	return nil
}

func (q *QuotaCenter) resetAllCurrentRates() {
	q.quotaStates = make(map[int64]map[milvuspb.QuotaState]commonpb.ErrorCode)
	q.currentRates = map[int64]map[internalpb.RateType]ratelimitutil.Limit{}
	for _, collection := range q.writableCollections {
		q.resetCurrentRate(internalpb.RateType_DMLInsert, collection)
		q.resetCurrentRate(internalpb.RateType_DMLDelete, collection)
		q.resetCurrentRate(internalpb.RateType_DMLBulkLoad, collection)
	}

	for _, collection := range q.readableCollections {
		q.resetCurrentRate(internalpb.RateType_DQLSearch, collection)
		q.resetCurrentRate(internalpb.RateType_DQLQuery, collection)
	}
}

// resetCurrentRates resets all current rates to configured rates.
func (q *QuotaCenter) resetCurrentRate(rt internalpb.RateType, collection int64) {
	if q.currentRates[collection] == nil {
		q.currentRates[collection] = make(map[internalpb.RateType]ratelimitutil.Limit)
	}

	if q.quotaStates[collection] == nil {
		q.quotaStates[collection] = make(map[milvuspb.QuotaState]commonpb.ErrorCode)
	}

	collectionProps := q.getCollectionLimitConfig(collection)
	switch rt {
	case internalpb.RateType_DMLInsert:
		q.currentRates[collection][rt] = Limit(getCollectionRateLimitConfig(collectionProps, common.CollectionInsertRateMaxKey))
	case internalpb.RateType_DMLDelete:
		q.currentRates[collection][rt] = Limit(getCollectionRateLimitConfig(collectionProps, common.CollectionDeleteRateMaxKey))
	case internalpb.RateType_DMLBulkLoad:
		q.currentRates[collection][rt] = Limit(getCollectionRateLimitConfig(collectionProps, common.CollectionBulkLoadRateMaxKey))
	case internalpb.RateType_DQLSearch:
		q.currentRates[collection][rt] = Limit(getCollectionRateLimitConfig(collectionProps, common.CollectionSearchRateMaxKey))
	case internalpb.RateType_DQLQuery:
		q.currentRates[collection][rt] = Limit(getCollectionRateLimitConfig(collectionProps, common.CollectionQueryRateMaxKey))
	}
	if q.currentRates[collection][rt] < 0 {
		q.currentRates[collection][rt] = Inf // no limit
	}
}

func (q *QuotaCenter) getCollectionLimitConfig(collection int64) map[string]string {
	log := log.Ctx(context.Background()).WithRateGroup("rootcoord.QuotaCenter", 1.0, 60.0)
	collectionInfo, err := q.meta.GetCollectionByID(context.TODO(), collection, typeutil.MaxTimestamp, false)
	if err != nil {
		log.RatedWarn(10, "failed to get collection rate limit config",
			zap.Int64("collectionID", collection),
			zap.Error(err))
		return make(map[string]string)
	}

	properties := make(map[string]string)
	for _, pair := range collectionInfo.Properties {
		properties[pair.GetKey()] = pair.GetValue()
	}

	return properties
}

// checkDiskQuota checks if disk quota exceeded.
func (q *QuotaCenter) checkDiskQuota() {
	q.diskMu.Lock()
	defer q.diskMu.Unlock()
	if !Params.QuotaConfig.DiskProtectionEnabled.GetAsBool() {
		return
	}
	if q.dataCoordMetrics == nil {
		return
	}
	collections := typeutil.NewUniqueSet()
	totalDiskQuota := Params.QuotaConfig.DiskQuota.GetAsFloat()
	for collection, binlogSize := range q.dataCoordMetrics.CollectionBinlogSize {
		collectionProps := q.getCollectionLimitConfig(collection)
		colDiskQuota := getCollectionRateLimitConfig(collectionProps, common.CollectionDiskQuotaKey)
		if float64(binlogSize) >= colDiskQuota {
			log.RatedWarn(10, "collection disk quota exceeded",
				zap.Int64("collection", collection),
				zap.Int64("coll disk usage", binlogSize),
				zap.Float64("coll disk quota", colDiskQuota))
			collections.Insert(collection)
		}
	}
	if collections.Len() > 0 {
		q.forceDenyWriting(commonpb.ErrorCode_DiskQuotaExhausted, collections.Collect()...)
	}
	total := q.dataCoordMetrics.TotalBinlogSize
	if float64(total) >= totalDiskQuota {
		log.RatedWarn(10, "total disk quota exceeded",
			zap.Int64("total disk usage", total),
			zap.Float64("total disk quota", totalDiskQuota))
		q.forceDenyWriting(commonpb.ErrorCode_DiskQuotaExhausted)
	}
	q.totalBinlogSize = total
}

// setRates notifies Proxies to set rates for different rate types.
func (q *QuotaCenter) setRates() error {
	ctx, cancel := context.WithTimeout(context.Background(), SetRatesTimeout)
	defer cancel()

	toCollectionRate := func(collection int64, currentRates map[internalpb.RateType]ratelimitutil.Limit) *proxypb.CollectionRate {
		rates := make([]*internalpb.Rate, 0, len(q.currentRates))
		switch q.rateAllocateStrategy {
		case Average:
			proxyNum := q.proxies.GetProxyCount()
			if proxyNum == 0 {
				return nil
			}
			for rt, r := range currentRates {
				if r == Inf {
					rates = append(rates, &internalpb.Rate{Rt: rt, R: float64(r)})
				} else {
					rates = append(rates, &internalpb.Rate{Rt: rt, R: float64(r) / float64(proxyNum)})
				}
			}

		case ByRateWeight:
			// TODO: support ByRateWeight
		}

		return &proxypb.CollectionRate{
			Collection: collection,
			Rates:      rates,
			States:     lo.Keys(q.quotaStates[collection]),
			Codes:      lo.Values(q.quotaStates[collection]),
		}
	}

	collectionRates := make([]*proxypb.CollectionRate, 0)
	for collection, rates := range q.currentRates {
		collectionRates = append(collectionRates, toCollectionRate(collection, rates))
	}
	timestamp := tsoutil.ComposeTSByTime(time.Now(), 0)
	req := &proxypb.SetRatesRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgID(int64(timestamp)),
			commonpbutil.WithTimeStamp(timestamp),
		),
		Rates: collectionRates,
	}
	return q.proxies.SetRates(ctx, req)
}

// recordMetrics records metrics of quota states.
func (q *QuotaCenter) recordMetrics() {
	record := func(errorCode commonpb.ErrorCode) {
		for _, states := range q.quotaStates {
			for _, state := range states {
				if state == errorCode {
					metrics.RootCoordQuotaStates.WithLabelValues(errorCode.String()).Set(1)
				}
			}
			return
		}
		metrics.RootCoordQuotaStates.WithLabelValues(errorCode.String()).Set(0)
	}
	record(commonpb.ErrorCode_MemoryQuotaExhausted)
	record(commonpb.ErrorCode_DiskQuotaExhausted)
	record(commonpb.ErrorCode_TimeTickLongDelay)
}

func (q *QuotaCenter) diskAllowance(collection UniqueID) float64 {
	q.diskMu.Lock()
	q.diskMu.Unlock()
	if !Params.QuotaConfig.DiskProtectionEnabled.GetAsBool() {
		return math.MaxInt64
	}
	if q.dataCoordMetrics == nil {
		return math.MaxInt64
	}
	totalDiskQuota := Params.QuotaConfig.DiskQuota.GetAsFloat()
	colDiskQuota := Params.QuotaConfig.DiskQuotaPerCollection.GetAsFloat()
	allowance := math.Min(totalDiskQuota, colDiskQuota)
	if binlogSize, ok := q.dataCoordMetrics.CollectionBinlogSize[collection]; ok {
		allowance = math.Min(allowance, colDiskQuota-float64(binlogSize))
	}
	allowance = math.Min(allowance, totalDiskQuota-float64(q.totalBinlogSize))
	return allowance
}

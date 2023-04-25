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
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/tso"
	"github.com/milvus-io/milvus/internal/types"
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
//
// If necessary, user can also manually force to deny RW requests.
type QuotaCenter struct {
	// clients
	proxies    *proxyClientManager
	queryCoord types.QueryCoord
	dataCoord  types.DataCoord

	// metrics
	queryNodeMetrics    map[UniqueID]*metricsinfo.QueryNodeQuotaMetrics
	dataNodeMetrics     map[UniqueID]*metricsinfo.DataNodeQuotaMetrics
	proxyMetrics        map[UniqueID]*metricsinfo.ProxyQuotaMetrics
	dataCoordMetrics    *metricsinfo.DataCoordQuotaMetrics
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
func NewQuotaCenter(proxies *proxyClientManager, queryCoord types.QueryCoord, dataCoord types.DataCoord, tsoAllocator tso.Allocator) *QuotaCenter {
	return &QuotaCenter{
		proxies:             proxies,
		queryCoord:          queryCoord,
		dataCoord:           dataCoord,
		currentRates:        make(map[int64]map[internalpb.RateType]Limit),
		quotaStates:         make(map[int64]map[milvuspb.QuotaState]commonpb.ErrorCode),
		tsoAllocator:        tsoAllocator,
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
func (q *QuotaCenter) forceDenyWriting(errorCode commonpb.ErrorCode, collections ...int64) {
	if len(collections) == 0 && len(q.writableCollections) != 0 {
		// default to all writable collections
		collections = q.writableCollections
	}
	for _, collection := range collections {
		q.currentRates[collection][internalpb.RateType_DMLInsert] = 0
		q.currentRates[collection][internalpb.RateType_DMLDelete] = 0
		q.currentRates[collection][internalpb.RateType_DMLBulkLoad] = 0
		q.quotaStates[collection][milvuspb.QuotaState_DenyToWrite] = errorCode
	}
	log.Warn("QuotaCenter force to deny writing",
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
	if Params.QuotaConfig.ForceDenyReading.GetAsBool() {
		q.forceDenyReading(commonpb.ErrorCode_ForceDeny)
		return
	}

	enableQueueProtection := Params.QuotaConfig.QueueProtectionEnabled.GetAsBool()
	if !enableQueueProtection {
		return
	}

	limitCollectionSet := typeutil.NewUniqueSet()

	// query latency
	queueLatencyThreshold := Params.QuotaConfig.QueueLatencyThreshold.GetAsDuration(time.Second)
	// queueLatencyThreshold >= 0 means enable queue latency protection
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
	if nqInQueueThreshold >= 0 {
		// >= 0 means enable queue length protection
		sum := func(ri metricsinfo.ReadInfoInQueue) int64 {
			return ri.UnsolvedQueue + ri.ReadyQueue + ri.ReceiveChan + ri.ExecuteChan
		}
		for _, metric := range q.queryNodeMetrics {
			searchNQSum := sum(metric.SearchQueue)
			queryTasksSum := sum(metric.QueryQueue)
			nqInQueue := searchNQSum + queryTasksSum // We think of the NQ of query request as 1.
			if nqInQueue >= nqInQueueThreshold {
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
				log.Warn("QuotaCenter cool read rates off done",
					zap.Int64("collectionID", collection),
					zap.Any("searchRate", q.currentRates[collection][internalpb.RateType_DQLSearch]))
			}
			if q.currentRates[collection][internalpb.RateType_DQLQuery] != Inf && realTimeQueryRate > 0 {
				q.currentRates[collection][internalpb.RateType_DQLQuery] = Limit(realTimeQueryRate * coolOffSpeed)
				log.Warn("QuotaCenter cool read rates off done",
					zap.Int64("collectionID", collection),
					zap.Any("queryRate", q.currentRates[collection][internalpb.RateType_DQLQuery]))
			}
		}

		q.guaranteeMinRate(Params.QuotaConfig.DQLMinSearchRate.GetAsFloat(), internalpb.RateType_DQLSearch, collections...)
		q.guaranteeMinRate(Params.QuotaConfig.DQLMinQueryRate.GetAsFloat(), internalpb.RateType_DQLQuery, collections...)
		log.Info("QueryNodeMetrics when cool-off",
			zap.Any("metrics", q.queryNodeMetrics))
	}

	// TODO: unify search and query?
	realTimeSearchRate := q.getRealTimeRate(internalpb.RateType_DQLSearch)
	realTimeQueryRate := q.getRealTimeRate(internalpb.RateType_DQLQuery)
	coolOff(realTimeSearchRate, realTimeQueryRate, limitCollectionSet.Collect()...)
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

	collectionFactor := map[int64]float64{}
	for _, collection := range q.writableCollections {
		collectionFactor[collection] = 1.0
	}

	ts, err := q.tsoAllocator.GenerateTSO(1)
	if err != nil {
		return err
	}
	maxDelay := Params.QuotaConfig.MaxTimeTickDelay.GetAsDuration(time.Second)
	curTs, _ := tsoutil.ParseTS(ts)
	processTtDelay := func(role string, nodeID int64, vchannel string, minTs time.Time, delay time.Duration, effectedCollections []int64) {
		log := log.With(zap.String("role", role),
			zap.Int64("nodeID", nodeID),
			zap.String("vchannel", vchannel),
			zap.Time("curTs", curTs),
			zap.Time("minTs", minTs),
			zap.Duration("delay", delay),
			zap.Duration("MaxDelay", maxDelay),
			zap.Int64s("effectedCollections", effectedCollections),
		)

		factor := float64(maxDelay.Nanoseconds()-delay.Nanoseconds()) / float64(maxDelay.Nanoseconds())

		if factor <= 0 {
			log.Warn("QuotaCenter: force deny writing due to long timeTick delay")
			q.forceDenyWriting(commonpb.ErrorCode_TimeTickLongDelay, effectedCollections...)
		}

		for _, collection := range effectedCollections {
			if factor < collectionFactor[collection] {
				collectionFactor[collection] = factor
			}
		}

	}

	processMemUsage := func(role string, nodeID int64, hms metricsinfo.HardwareMetrics, memLowLevel float64, memHighLevel float64, effectedCollections []int64) {
		memoryWaterLevel := float64(hms.MemoryUsage) / float64(hms.Memory)
		log := log.With(
			zap.String("role", role),
			zap.Int64("nodeID", nodeID),
			zap.Uint64("usedMem", hms.MemoryUsage),
			zap.Uint64("totalMem", hms.Memory),
			zap.Float64("memoryWaterLevel", memoryWaterLevel),
			zap.Float64("memoryHighWaterLevel", memHighLevel),
			zap.Int64s("effectedCollections", effectedCollections),
		)

		factor := (memHighLevel - memoryWaterLevel) / (memHighLevel - memLowLevel)

		if factor <= 0 {
			log.Warn("QuotaCenter: force deny writing due to memory reach high water")
			q.forceDenyWriting(commonpb.ErrorCode_MemoryQuotaExhausted, effectedCollections...)
		}

		if factor < 0.9 {
			log.Warn("QuotaCenter: Node memory to low water level, limit writing rate",
				zap.Float64("factor", factor))
		}

		for _, collection := range effectedCollections {
			if factor < collectionFactor[collection] {
				collectionFactor[collection] = factor
			}
		}
	}

	enableTtProtection := Params.QuotaConfig.TtProtectionEnabled.GetAsBool()
	enableMemProtection := Params.QuotaConfig.MemProtectionEnabled.GetAsBool()

	dataNodeMemoryLowWaterLevel := Params.QuotaConfig.DataNodeMemoryLowWaterLevel.GetAsFloat()
	dataNodeMemoryHighWaterLevel := Params.QuotaConfig.DataNodeMemoryHighWaterLevel.GetAsFloat()
	queryNodeMemoryLowWaterLevel := Params.QuotaConfig.QueryNodeMemoryLowWaterLevel.GetAsFloat()
	queryNodeMemoryHighWaterLevel := Params.QuotaConfig.QueryNodeMemoryHighWaterLevel.GetAsFloat()

	for nodeID, metric := range q.queryNodeMetrics {
		if enableTtProtection && maxDelay >= 0 && metric.Fgm.NumFlowGraph > 0 && metric.Fgm.MinFlowGraphChannel != "" {
			minTs, _ := tsoutil.ParseTS(metric.Fgm.MinFlowGraphTt)
			delay := curTs.Sub(minTs)
			processTtDelay(typeutil.QueryNodeRole, nodeID, metric.Fgm.MinFlowGraphChannel, minTs, delay, metric.Effect.CollectionIDs)
			metrics.RootCoordTtDelay.WithLabelValues(typeutil.QueryNodeRole, strconv.FormatInt(nodeID, 10)).Set(float64(delay.Milliseconds()))
		}

		if enableMemProtection {
			processMemUsage(typeutil.QueryNodeRole, nodeID, metric.Hms, queryNodeMemoryLowWaterLevel, queryNodeMemoryHighWaterLevel, metric.Effect.CollectionIDs)
		}
	}

	for nodeID, metric := range q.dataNodeMetrics {
		if enableTtProtection && maxDelay >= 0 && metric.Fgm.NumFlowGraph > 0 && metric.Fgm.MinFlowGraphChannel != "" {
			minTs, _ := tsoutil.ParseTS(metric.Fgm.MinFlowGraphTt)
			delay := curTs.Sub(minTs)
			processTtDelay(typeutil.DataNodeRole, nodeID, metric.Fgm.MinFlowGraphChannel, minTs, delay, metric.Effect.CollectionIDs)
			metrics.RootCoordTtDelay.WithLabelValues(typeutil.DataNodeRole, strconv.FormatInt(nodeID, 10)).Set(float64(delay.Milliseconds()))
		}

		if enableMemProtection {
			processMemUsage(typeutil.QueryNodeRole, nodeID, metric.Hms, dataNodeMemoryLowWaterLevel, dataNodeMemoryHighWaterLevel, metric.Effect.CollectionIDs)
		}
	}

	for collection, cf := range collectionFactor {
		if q.currentRates[collection][internalpb.RateType_DMLInsert] != Inf && cf > 0 {
			q.currentRates[collection][internalpb.RateType_DMLInsert] *= Limit(cf)
		}
		if q.currentRates[collection][internalpb.RateType_DMLDelete] != Inf && cf > 0 {
			q.currentRates[collection][internalpb.RateType_DMLDelete] *= Limit(cf)
		}
		q.guaranteeMinRate(Params.QuotaConfig.DMLMinInsertRate.GetAsFloat(), internalpb.RateType_DMLInsert)
		q.guaranteeMinRate(Params.QuotaConfig.DMLMinDeleteRate.GetAsFloat(), internalpb.RateType_DMLDelete)
		log.Warn("QuotaCenter cool write rates off done",
			zap.Int64("collectionID", collection),
			zap.Float64("factor", cf))
	}

	return nil
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
	switch rt {
	case internalpb.RateType_DMLInsert:
		q.currentRates[collection][rt] = Limit(Params.QuotaConfig.DMLMaxInsertRate.GetAsFloat())
	case internalpb.RateType_DMLDelete:
		q.currentRates[collection][rt] = Limit(Params.QuotaConfig.DMLMaxDeleteRate.GetAsFloat())
	case internalpb.RateType_DMLBulkLoad:
		q.currentRates[collection][rt] = Limit(Params.QuotaConfig.DMLMaxBulkLoadRate.GetAsFloat())
	case internalpb.RateType_DQLSearch:
		q.currentRates[collection][rt] = Limit(Params.QuotaConfig.DQLMaxSearchRate.GetAsFloat())
	case internalpb.RateType_DQLQuery:
		q.currentRates[collection][rt] = Limit(Params.QuotaConfig.DQLMaxQueryRate.GetAsFloat())
	}
	if q.currentRates[collection][rt] < 0 {
		q.currentRates[collection][rt] = Inf // no limit
	}
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

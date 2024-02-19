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
	"strings"
	"sync"
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/tso"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	rlinternal "github.com/milvus-io/milvus/internal/util/ratelimitutil"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/ratelimitutil"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
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

var (
	// It indicates an infinite limiter with burst is 0
	infLimiter = ratelimitutil.NewLimiter(Inf, 0)

	// It indicates an earliest limiter with burst is 0
	earliestLimiter = ratelimitutil.NewLimiter(0, 0)
)

type opType int

const (
	ddl opType = iota
	dml
	dql
	allOps
)

var ddlRateTypes = typeutil.NewSet(
	internalpb.RateType_DDLCollection,
	internalpb.RateType_DDLPartition,
	internalpb.RateType_DDLIndex,
	internalpb.RateType_DDLFlush,
	internalpb.RateType_DDLCompaction,
)

var dmlRateTypes = typeutil.NewSet(
	internalpb.RateType_DMLInsert,
	internalpb.RateType_DMLUpsert,
	internalpb.RateType_DMLDelete,
	internalpb.RateType_DMLBulkLoad,
)

var dqlRateTypes = typeutil.NewSet(
	internalpb.RateType_DQLSearch,
	internalpb.RateType_DQLQuery,
)

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
	proxies    proxyutil.ProxyClientManagerInterface
	queryCoord types.QueryCoordClient
	dataCoord  types.DataCoordClient
	meta       IMetaTable

	// metrics
	queryNodeMetrics map[UniqueID]*metricsinfo.QueryNodeQuotaMetrics
	dataNodeMetrics  map[UniqueID]*metricsinfo.DataNodeQuotaMetrics
	proxyMetrics     map[UniqueID]*metricsinfo.ProxyQuotaMetrics
	diskMu           sync.Mutex // guards dataCoordMetrics and totalBinlogSize
	dataCoordMetrics *metricsinfo.DataCoordQuotaMetrics
	totalBinlogSize  int64

	readableCollections map[int64]map[int64][]int64 // db id -> collection id -> partition id
	writableCollections map[int64]map[int64][]int64 // db id -> collection id -> partition id

	// this is a transitional data structure to cache db id for each collection.
	// TODO many metrics information only have collection id currently, it can be removed after db id add into all metrics.
	collectionIDToDBID *typeutil.ConcurrentMap[int64, int64] // collection id ->  db id

	rateLimiter *rlinternal.RateLimiterTree

	tsoAllocator tso.Allocator

	rateAllocateStrategy RateAllocateStrategy

	stopOnce sync.Once
	stopChan chan struct{}
}

// NewQuotaCenter returns a new QuotaCenter.
func NewQuotaCenter(proxies proxyutil.ProxyClientManagerInterface, queryCoord types.QueryCoordClient,
	dataCoord types.DataCoordClient, tsoAllocator tso.Allocator, meta IMetaTable,
) *QuotaCenter {
	return &QuotaCenter{
		proxies:              proxies,
		queryCoord:           queryCoord,
		dataCoord:            dataCoord,
		rateLimiter:          rlinternal.NewRateLimiterTree(initInfLimiter(internalpb.RateScope_Cluster, allOps)),
		tsoAllocator:         tsoAllocator,
		meta:                 meta,
		rateAllocateStrategy: DefaultRateAllocateStrategy,
		stopChan:             make(chan struct{}),
	}
}

func initEarliestLimiter(rateScope internalpb.RateScope, opType opType) *rlinternal.RateLimiterNode {
	return initLimiter(earliestLimiter, rateScope, opType)
}

func initInfLimiter(rateScope internalpb.RateScope, opType opType) *rlinternal.RateLimiterNode {
	return initLimiter(infLimiter, rateScope, opType)
}

func initLimiter(limiter *ratelimitutil.Limiter, rateScope internalpb.RateScope, opType opType) *rlinternal.RateLimiterNode {
	rateLimiters := rlinternal.NewRateLimiterNode()
	getRateTypes(rateScope, opType).Range(func(rt internalpb.RateType) bool {
		rateLimiters.GetLimiters().GetOrInsert(rt, limiter)
		return true
	})
	return rateLimiters
}

func getRateTypes(scope internalpb.RateScope, opType opType) typeutil.Set[internalpb.RateType] {
	var allRateTypes typeutil.Set[internalpb.RateType]
	switch scope {
	case internalpb.RateScope_Cluster:
		// TODO add database ddl
		fallthrough
	case internalpb.RateScope_Database:
		allRateTypes = ddlRateTypes.Union(dmlRateTypes).Union(dqlRateTypes)
	case internalpb.RateScope_Collection:
		allRateTypes = typeutil.NewSet(internalpb.RateType_DDLFlush).Union(dmlRateTypes).Union(dqlRateTypes)
	case internalpb.RateScope_Partition:
		allRateTypes = dmlRateTypes.Union(dqlRateTypes)
	default:
		panic("Unknown rate scope:" + scope.String())
	}

	switch opType {
	case ddl:
		return ddlRateTypes.Intersection(allRateTypes)
	case dml:
		return dmlRateTypes.Intersection(allRateTypes)
	case dql:
		return dqlRateTypes.Intersection(allRateTypes)
	default:
		return allRateTypes
	}
}

// run starts the service of QuotaCenter.
func (q *QuotaCenter) run() {
	interval := time.Duration(Params.QuotaConfig.QuotaCenterCollectInterval.GetAsFloat() * float64(time.Second))
	log.Info("Start QuotaCenter", zap.Duration("collectInterval", interval))
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-q.stopChan:
			log.Info("QuotaCenter exit")
			return
		case <-ticker.C:
			err := q.collectMetrics()
			if err != nil {
				log.Warn("quotaCenter collect metrics failed", zap.Error(err))
				break
			}
			err = q.calculateRates()
			if err != nil {
				log.Warn("quotaCenter calculate rates failed", zap.Error(err))
				break
			}
			err = q.sendRatesToProxy()
			if err != nil {
				log.Warn("quotaCenter send rates to proxy failed", zap.Error(err))
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

// collectMetrics sends GetMetrics requests to DataCoord and QueryCoord to collect the metrics in DataNodes and QueryNodes.
func (q *QuotaCenter) collectMetrics() error {
	q.clearMetrics()
	q.collectionIDToDBID = typeutil.NewConcurrentMap[int64, int64]()

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

		q.readableCollections = make(map[int64]map[int64][]int64, 0)
		collections.Range(func(collectionID int64) bool {
			var coll *model.Collection
			coll, err = q.meta.GetCollectionByIDWithMaxTs(context.TODO(), collectionID)
			if err != nil {
				return false
			}
			collIDToPartIDs, ok := q.readableCollections[coll.DBID]
			if !ok {
				collIDToPartIDs = map[int64][]int64{collectionID: {}}
				q.readableCollections[coll.DBID] = collIDToPartIDs
			} else {
				collIDToPartIDs[collectionID] = []int64{}
			}
			q.collectionIDToDBID.Insert(collectionID, coll.DBID)
			return true
		})

		return err
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

		q.diskMu.Lock()
		if dataCoordTopology.Cluster.Self.QuotaMetrics != nil {
			q.dataCoordMetrics = dataCoordTopology.Cluster.Self.QuotaMetrics
		}
		q.diskMu.Unlock()

		q.writableCollections = make(map[int64]map[int64][]int64, 0)
		collections.Range(func(collectionID int64) bool {
			var coll *model.Collection
			coll, err = q.meta.GetCollectionByIDWithMaxTs(context.TODO(), collectionID)
			if err != nil {
				return false
			}

			collIDToPartIDs, ok := q.writableCollections[coll.DBID]
			if !ok {
				collIDToPartIDs = map[int64][]int64{collectionID: {}}
				q.writableCollections[coll.DBID] = collIDToPartIDs
			} else {
				collIDToPartIDs[collectionID] = []int64{}
			}
			q.collectionIDToDBID.Insert(collectionID, coll.DBID)

			return true
		})
		return err
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
	// log.Debug("QuotaCenter sync metrics done",
	//	zap.Any("dataNodeMetrics", q.dataNodeMetrics),
	//	zap.Any("queryNodeMetrics", q.queryNodeMetrics),
	//	zap.Any("proxyMetrics", q.proxyMetrics),
	//	zap.Any("dataCoordMetrics", q.dataCoordMetrics))
	return nil
}

// forceDenyWriting sets dml rates to 0 to reject all dml requests.
func (q *QuotaCenter) forceDenyWritingCollection(errorCode commonpb.ErrorCode, collectionIDs ...int64) error {
	if len(collectionIDs) == 0 && len(q.writableCollections) != 0 {
		// default to all writable collections
		var writableCollectionIDs []int64
		for _, collectionIDToPartIDs := range q.writableCollections {
			for collectionID := range collectionIDToPartIDs {
				writableCollectionIDs = append(writableCollectionIDs, collectionID)
			}
		}
		collectionIDs = writableCollectionIDs
	}
	for _, collectionID := range collectionIDs {
		dbID, ok := q.collectionIDToDBID.Get(collectionID)
		if !ok {
			return fmt.Errorf("db ID not found of collection ID: %d", collectionID)
		}
		collectionLimiter := q.rateLimiter.GetOrCreateCollectionLimiters(dbID, collectionID,
			func() *rlinternal.RateLimiterNode {
				return initInfLimiter(internalpb.RateScope_Database, allOps)
			},
			func() *rlinternal.RateLimiterNode {
				return initEarliestLimiter(internalpb.RateScope_Collection, dml)
			},
		)

		collectionLimiter.GetQuotaStates().Insert(milvuspb.QuotaState_DenyToWrite, errorCode)
	}

	log.RatedWarn(10, "QuotaCenter force to deny writing",
		zap.Int64s("collectionIDs", collectionIDs),
		zap.String("reason", errorCode.String()))

	return nil
}

// forceDenyReading sets dql rates to 0 to reject all dql requests.
func (q *QuotaCenter) forceDenyReading(errorCode commonpb.ErrorCode) {
	var collectionIDs []int64
	for dbID, collectionIDToPartIDs := range q.readableCollections {
		for collectionID := range collectionIDToPartIDs {
			collectionLimiter := q.rateLimiter.GetOrCreateCollectionLimiters(dbID, collectionID,
				func() *rlinternal.RateLimiterNode {
					return initInfLimiter(internalpb.RateScope_Database, allOps)
				},
				func() *rlinternal.RateLimiterNode {
					return initEarliestLimiter(internalpb.RateScope_Collection, dql)
				},
			)

			collectionLimiter.GetQuotaStates().Insert(milvuspb.QuotaState_DenyToRead, errorCode)
			collectionIDs = append(collectionIDs, collectionID)
		}
	}

	log.Warn("QuotaCenter force to deny reading",
		zap.Int64s("collectionIDs", collectionIDs),
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
func (q *QuotaCenter) guaranteeMinRate(minRate float64, rt internalpb.RateType, rln *rlinternal.RateLimiterNode) {
	v, ok := rln.GetLimiters().Get(rt)
	if ok && minRate > 0 && v.Limit() < Limit(minRate) {
		v.SetLimit(Limit(minRate))
	}
}

// calculateReadRates calculates and sets dql rates.
func (q *QuotaCenter) calculateReadRates() error {
	log := log.Ctx(context.Background()).WithRateGroup("rootcoord.QuotaCenter", 1.0, 60.0)
	if Params.QuotaConfig.ForceDenyReading.GetAsBool() {
		q.forceDenyReading(commonpb.ErrorCode_ForceDeny)
		return nil
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
			// add all readable collection
			for _, collections := range q.readableCollections {
				limitCollectionSet.Insert(lo.Keys(collections)...)
			}
		}
	}

	coolOffSpeed := Params.QuotaConfig.CoolOffSpeed.GetAsFloat()
	coolOff := func(realTimeSearchRate float64, realTimeQueryRate float64, collections ...int64) error {
		for _, collection := range collections {
			dbID, ok := q.collectionIDToDBID.Get(collection)
			if !ok {
				return fmt.Errorf("db ID not found of collection ID: %d", collection)
			}
			collectionLimiter := q.rateLimiter.GetCollectionLimiters(dbID, collection)
			if collectionLimiter == nil {
				return fmt.Errorf("collection limiter not found: %d", collection)
			}

			limiter := collectionLimiter.GetLimiters()

			v, ok := limiter.Get(internalpb.RateType_DQLSearch)
			if ok && v.Limit() != Inf && realTimeSearchRate > 0 {
				v.SetLimit(Limit(realTimeSearchRate * coolOffSpeed))
				log.RatedWarn(10, "QuotaCenter cool read rates off done",
					zap.Int64("collectionID", collection),
					zap.Any("searchRate", v.Limit()))
			}

			v, ok = limiter.Get(internalpb.RateType_DQLQuery)
			if ok && v.Limit() != Inf && realTimeQueryRate > 0 {
				v.SetLimit(Limit(realTimeQueryRate * coolOffSpeed))
				log.RatedWarn(10, "QuotaCenter cool read rates off done",
					zap.Int64("collectionID", collection),
					zap.Any("queryRate", v.Limit()))
			}

			collectionProps := q.getCollectionLimitProperties(collection)
			q.guaranteeMinRate(getCollectionRateLimitConfig(collectionProps, common.CollectionSearchRateMinKey),
				internalpb.RateType_DQLSearch, collectionLimiter)
			q.guaranteeMinRate(getCollectionRateLimitConfig(collectionProps, common.CollectionQueryRateMinKey),
				internalpb.RateType_DQLQuery, collectionLimiter)
		}
		return nil
	}

	// TODO: unify search and query?
	realTimeSearchRate := q.getRealTimeRate(internalpb.RateType_DQLSearch)
	realTimeQueryRate := q.getRealTimeRate(internalpb.RateType_DQLQuery)
	return coolOff(realTimeSearchRate, realTimeQueryRate, limitCollectionSet.Collect()...)
}

// calculateWriteRates calculates and sets dml rates.
func (q *QuotaCenter) calculateWriteRates() error {
	log := log.Ctx(context.Background()).WithRateGroup("rootcoord.QuotaCenter", 1.0, 60.0)
	if Params.QuotaConfig.ForceDenyWriting.GetAsBool() {
		return q.forceDenyWritingCollection(commonpb.ErrorCode_ForceDeny)
	}

	if err := q.checkDiskQuota(); err != nil {
		return err
	}

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
		metrics.RootCoordRateLimitRatio.WithLabelValues(fmt.Sprint(collection)).Set(1 - factor)
		if factor <= 0 {
			var err error
			if _, ok := ttFactors[collection]; ok && factor == ttFactors[collection] {
				// factor comes from ttFactor
				err = q.forceDenyWritingCollection(commonpb.ErrorCode_TimeTickLongDelay, collection)
			} else {
				// factor comes from memFactor or growingSegFactor, all about mem exhausted
				err = q.forceDenyWritingCollection(commonpb.ErrorCode_MemoryQuotaExhausted, collection)
			}

			if err != nil {
				return err
			}
		}

		dbID, ok := q.collectionIDToDBID.Get(collection)
		if !ok {
			return fmt.Errorf("db ID not found of collection ID: %d", collection)
		}
		collectionLimiter := q.rateLimiter.GetCollectionLimiters(dbID, collection)
		if collectionLimiter == nil {
			return fmt.Errorf("collection limiter not found: %d", collection)
		}

		limiter := collectionLimiter.GetLimiters()
		for _, rt := range []internalpb.RateType{
			internalpb.RateType_DMLInsert,
			internalpb.RateType_DMLUpsert, internalpb.RateType_DMLDelete,
		} {
			v, ok := limiter.Get(rt)
			if ok {
				if v.Limit() != Inf {
					v.SetLimit(v.Limit() * Limit(factor))
				}
			}
		}

		collectionProps := q.getCollectionLimitProperties(collection)
		q.guaranteeMinRate(getCollectionRateLimitConfig(collectionProps, common.CollectionInsertRateMinKey),
			internalpb.RateType_DMLInsert, collectionLimiter)
		q.guaranteeMinRate(getCollectionRateLimitConfig(collectionProps, common.CollectionUpsertRateMinKey),
			internalpb.RateType_DMLUpsert, collectionLimiter)
		q.guaranteeMinRate(getCollectionRateLimitConfig(collectionProps, common.CollectionDeleteRateMinKey),
			internalpb.RateType_DMLDelete, collectionLimiter)
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
				zap.Float64("curWatermark", memoryWaterLevel),
				zap.Float64("lowWatermark", queryNodeMemoryLowWaterLevel),
				zap.Float64("highWatermark", queryNodeMemoryHighWaterLevel))
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
			zap.Float64("curWatermark", memoryWaterLevel),
			zap.Float64("lowWatermark", queryNodeMemoryLowWaterLevel),
			zap.Float64("highWatermark", queryNodeMemoryHighWaterLevel))
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
				zap.Float64("curWatermark", memoryWaterLevel),
				zap.Float64("lowWatermark", dataNodeMemoryLowWaterLevel),
				zap.Float64("highWatermark", dataNodeMemoryHighWaterLevel))
			updateCollectionFactor(0, metric.Effect.CollectionIDs)
			continue
		}
		factor := (dataNodeMemoryHighWaterLevel - memoryWaterLevel) / (dataNodeMemoryHighWaterLevel - dataNodeMemoryLowWaterLevel)
		log.RatedWarn(10, "QuotaCenter: DataNode memory to low water level, limit writing rate",
			zap.String("Node", fmt.Sprintf("%s-%d", typeutil.DataNodeRole, nodeID)),
			zap.Int64s("collections", metric.Effect.CollectionIDs),
			zap.Uint64("UsedMem", metric.Hms.MemoryUsage),
			zap.Uint64("TotalMem", metric.Hms.Memory),
			zap.Float64("curWatermark", memoryWaterLevel),
			zap.Float64("lowWatermark", dataNodeMemoryLowWaterLevel),
			zap.Float64("highWatermark", dataNodeMemoryHighWaterLevel))
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
		factor := (high - cur) / (high - low)
		if factor < Params.QuotaConfig.GrowingSegmentsSizeMinRateRatio.GetAsFloat() {
			factor = Params.QuotaConfig.GrowingSegmentsSizeMinRateRatio.GetAsFloat()
		}
		updateCollectionFactor(factor, metric.Effect.CollectionIDs)
		log.RatedWarn(10, "QuotaCenter: QueryNode growing segments size exceeds watermark, limit writing rate",
			zap.String("Node", fmt.Sprintf("%s-%d", typeutil.QueryNodeRole, nodeID)),
			zap.Int64s("collections", metric.Effect.CollectionIDs),
			zap.Int64("segmentsSize", metric.GrowingSegmentsSize),
			zap.Uint64("TotalMem", metric.Hms.Memory),
			zap.Float64("highWatermark", high),
			zap.Float64("lowWatermark", low),
			zap.Float64("factor", factor))
	}
	return collectionFactor
}

// calculateRates calculates target rates by different strategies.
func (q *QuotaCenter) calculateRates() error {
	err := q.resetAllCurrentRates()

	err = q.calculateWriteRates()
	if err != nil {
		return err
	}
	q.calculateReadRates()

	// log.Debug("QuotaCenter calculates rate done", zap.Any("rates", q.currentRates))
	return nil
}

func (q *QuotaCenter) resetAllCurrentRates() error {
	q.rateLimiter = rlinternal.NewRateLimiterTree(initInfLimiter(internalpb.RateScope_Cluster, allOps))
	resetRates := func(sourceCollections map[int64]map[int64][]int64, rtSet typeutil.Set[internalpb.RateType]) error {
		for dbID, collections := range sourceCollections {
			for collectionID := range collections {
				collectionLimiter, err := q.getOrCreateNewCollectionRateLimiter(dbID, collectionID)
				if err != nil {
					return err
				}
				rtSet.Range(func(rt internalpb.RateType) bool {
					err = q.updateLimitWithCollectionMaxLimit(collectionLimiter, rt)
					if err != nil {
						return false
					}
					return true
				})
				if err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := resetRates(q.writableCollections, dmlRateTypes); err != nil {
		return err
	}

	if err := resetRates(q.readableCollections, dqlRateTypes); err != nil {
		return err
	}
	return nil
}

func (q *QuotaCenter) getOrCreateNewCollectionRateLimiter(dbID, collectionID int64) (*rlinternal.RateLimiterNode, error) {
	return q.rateLimiter.GetOrCreateCollectionLimiters(dbID, collectionID,
		func() *rlinternal.RateLimiterNode {
			return initInfLimiter(internalpb.RateScope_Database, allOps)
		},
		func() *rlinternal.RateLimiterNode {
			return initInfLimiter(internalpb.RateScope_Collection, allOps)
		},
	), nil
}

func updateLimit(collectionLimiter *rlinternal.RateLimiterNode, rt internalpb.RateType, limit ratelimitutil.Limit) error {
	limiters := collectionLimiter.GetLimiters()
	limiter, ok := limiters.Get(rt)
	if !ok {
		var keys []string
		limiters.Range(func(k internalpb.RateType, v *ratelimitutil.Limiter) bool {
			keys = append(keys, k.String())
			return true
		})

		collectionID := collectionLimiter.GetID()
		return fmt.Errorf("rate type:%s not found within set:%s from collection: %d", rt.String(), strings.Join(keys, ","), collectionID)
	}
	limiter.SetLimit(limit)
	return nil
}

func (q *QuotaCenter) updateLimitWithCollectionMaxLimit(collectionLimiter *rlinternal.RateLimiterNode, rt internalpb.RateType) error {
	limit, err := q.getCollectionMaxLimit(rt, collectionLimiter.GetID())
	if err != nil {
		return err
	}
	return updateLimit(collectionLimiter, rt, limit)
}

// getCollectionMaxLimit get limit value from collection's properties.
func (q *QuotaCenter) getCollectionMaxLimit(rt internalpb.RateType, collectionID int64) (ratelimitutil.Limit, error) {
	collectionProps := q.getCollectionLimitProperties(collectionID)
	switch rt {
	case internalpb.RateType_DMLInsert:
		return Limit(getCollectionRateLimitConfig(collectionProps, common.CollectionInsertRateMaxKey)), nil
	case internalpb.RateType_DMLUpsert:
		return Limit(getCollectionRateLimitConfig(collectionProps, common.CollectionUpsertRateMaxKey)), nil
	case internalpb.RateType_DMLDelete:
		return Limit(getCollectionRateLimitConfig(collectionProps, common.CollectionDeleteRateMaxKey)), nil
	case internalpb.RateType_DMLBulkLoad:
		return Limit(getCollectionRateLimitConfig(collectionProps, common.CollectionBulkLoadRateMaxKey)), nil
	case internalpb.RateType_DQLSearch:
		return Limit(getCollectionRateLimitConfig(collectionProps, common.CollectionSearchRateMaxKey)), nil
	case internalpb.RateType_DQLQuery:
		return Limit(getCollectionRateLimitConfig(collectionProps, common.CollectionQueryRateMaxKey)), nil
	default:
		return 0, fmt.Errorf("unsupportd rate type:%s", rt.String())
	}
}

func (q *QuotaCenter) getCollectionLimitProperties(collection int64) map[string]string {
	log := log.Ctx(context.Background()).WithRateGroup("rootcoord.QuotaCenter", 1.0, 60.0)
	collectionInfo, err := q.meta.GetCollectionByIDWithMaxTs(context.TODO(), collection)
	if err != nil {
		log.RatedWarn(10, "failed to get rate limit properties from collection meta",
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
func (q *QuotaCenter) checkDiskQuota() error {
	q.diskMu.Lock()
	defer q.diskMu.Unlock()
	if !Params.QuotaConfig.DiskProtectionEnabled.GetAsBool() {
		return nil
	}
	if q.dataCoordMetrics == nil {
		return nil
	}
	collections := typeutil.NewUniqueSet()
	totalDiskQuota := Params.QuotaConfig.DiskQuota.GetAsFloat()
	for collection, binlogSize := range q.dataCoordMetrics.CollectionBinlogSize {
		collectionProps := q.getCollectionLimitProperties(collection)
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
		if err := q.forceDenyWritingCollection(commonpb.ErrorCode_DiskQuotaExhausted, collections.Collect()...); err != nil {
			return err
		}
	}
	total := q.dataCoordMetrics.TotalBinlogSize
	if float64(total) >= totalDiskQuota {
		log.RatedWarn(10, "total disk quota exceeded",
			zap.Int64("total disk usage", total),
			zap.Float64("total disk quota", totalDiskQuota))
		if err := q.forceDenyWritingCollection(commonpb.ErrorCode_DiskQuotaExhausted); err != nil {
			return err
		}
	}
	q.totalBinlogSize = total
	return nil
}

func (q *QuotaCenter) toRequestLimiter(limiter *rlinternal.RateLimiterNode) *proxypb.Limiter {
	var rates []*internalpb.Rate
	switch q.rateAllocateStrategy {
	case Average:
		proxyNum := q.proxies.GetProxyCount()
		if proxyNum == 0 {
			return nil
		}
		limiter.GetLimiters().Range(func(rt internalpb.RateType, limiter *ratelimitutil.Limiter) bool {
			r := limiter.Limit()
			if r == Inf {
				rates = append(rates, &internalpb.Rate{Rt: rt, R: float64(r)})
			} else {
				rates = append(rates, &internalpb.Rate{Rt: rt, R: float64(r) / float64(proxyNum)})
			}
			return true
		})
	case ByRateWeight:
		// TODO: support ByRateWeight
	}

	size := limiter.GetQuotaStates().Len()
	states := make([]milvuspb.QuotaState, 0, size)
	codes := make([]commonpb.ErrorCode, 0, size)

	limiter.GetQuotaStates().Range(func(state milvuspb.QuotaState, code commonpb.ErrorCode) bool {
		states = append(states, state)
		codes = append(codes, code)
		return true
	})

	return &proxypb.Limiter{
		Rates:  rates,
		States: states,
		Codes:  codes,
	}
}

func (q *QuotaCenter) toRatesRequest() *proxypb.SetRatesRequest {
	clusterRateLimiter := q.rateLimiter.GetRootLimiters()

	// collect db rate limit if clusterRateLimiter has database limiter children
	dbLimiters := make(map[int64]*proxypb.LimiterNode, len(clusterRateLimiter.GetChildren()))
	for dbID, dbRateLimiters := range clusterRateLimiter.GetChildren() {
		dbLimiter := q.toRequestLimiter(dbRateLimiters)

		// collect collection rate limit if dbRateLimiters has collection limiter children
		collectionLimiters := make(map[int64]*proxypb.LimiterNode, len(dbRateLimiters.GetChildren()))
		for collectionID, collectionRateLimiters := range dbRateLimiters.GetChildren() {
			collectionLimiter := q.toRequestLimiter(collectionRateLimiters)

			// collect partitions rate limit if collectionRateLimiters has partition limiter children
			partitionLimiters := make(map[int64]*proxypb.LimiterNode, len(collectionRateLimiters.GetChildren()))
			for partitionID, partitionRateLimiters := range collectionRateLimiters.GetChildren() {
				partitionLimiters[partitionID] = &proxypb.LimiterNode{
					Limiter:  q.toRequestLimiter(partitionRateLimiters),
					Children: make(map[int64]*proxypb.LimiterNode, 0),
				}
			}

			collectionLimiters[collectionID] = &proxypb.LimiterNode{
				Limiter:  collectionLimiter,
				Children: partitionLimiters,
			}
		}

		dbLimiters[dbID] = &proxypb.LimiterNode{
			Limiter:  dbLimiter,
			Children: collectionLimiters,
		}
	}

	clusterLimiter := &proxypb.LimiterNode{
		Limiter:  q.toRequestLimiter(clusterRateLimiter),
		Children: dbLimiters,
	}

	timestamp := tsoutil.ComposeTSByTime(time.Now(), 0)
	return &proxypb.SetRatesRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgID(int64(timestamp)),
			commonpbutil.WithTimeStamp(timestamp),
		),
		Rates:       []*proxypb.CollectionRate{},
		RootLimiter: clusterLimiter,
	}
}

// sendRatesToProxy notifies Proxies to set rates for different rate types.
func (q *QuotaCenter) sendRatesToProxy() error {
	ctx, cancel := context.WithTimeout(context.Background(), SetRatesTimeout)
	defer cancel()
	return q.proxies.SetRates(ctx, q.toRatesRequest())
}

// recordMetrics records metrics of quota states.
func (q *QuotaCenter) recordMetrics() {
	record := func(errorCode commonpb.ErrorCode) {
		var hasException float64 = 0
		rlinternal.TraverseRateLimiterTree(q.rateLimiter.GetRootLimiters(), nil,
			func(state milvuspb.QuotaState, errCode commonpb.ErrorCode) bool {
				if errCode == errorCode {
					hasException = 1
					return false
				}
				return true
			})
		metrics.RootCoordQuotaStates.WithLabelValues(errorCode.String()).Set(hasException)
	}
	record(commonpb.ErrorCode_MemoryQuotaExhausted)
	record(commonpb.ErrorCode_DiskQuotaExhausted)
	record(commonpb.ErrorCode_TimeTickLongDelay)
}

func (q *QuotaCenter) diskAllowance(collection UniqueID) float64 {
	q.diskMu.Lock()
	defer q.diskMu.Unlock()
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

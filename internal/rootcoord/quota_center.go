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
	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/tso"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/internal/util/quota"
	rlinternal "github.com/milvus-io/milvus/internal/util/ratelimitutil"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/config"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
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

func GetInfLimiter(_ internalpb.RateType) *ratelimitutil.Limiter {
	// It indicates an infinite limiter with burst is 0
	return ratelimitutil.NewLimiter(Inf, 0)
}

func GetEarliestLimiter() *ratelimitutil.Limiter {
	// It indicates an earliest limiter with burst is 0
	return ratelimitutil.NewLimiter(0, 0)
}

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
	ctx    context.Context
	cancel context.CancelFunc

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

	readableCollections map[int64]map[int64][]int64            // db id -> collection id -> partition id
	writableCollections map[int64]map[int64][]int64            // db id -> collection id -> partition id
	dbs                 *typeutil.ConcurrentMap[string, int64] // db name -> db id
	collections         *typeutil.ConcurrentMap[string, int64] // db id + collection name -> collection id

	// this is a transitional data structure to cache db id for each collection.
	// TODO many metrics information only have collection id currently, it can be removed after db id add into all metrics.
	collectionIDToDBID *typeutil.ConcurrentMap[int64, int64] // collection id ->  db id

	rateLimiter *rlinternal.RateLimiterTree

	tsoAllocator tso.Allocator

	rateAllocateStrategy RateAllocateStrategy

	stopOnce sync.Once
	stopChan chan struct{}
	wg       sync.WaitGroup
}

// NewQuotaCenter returns a new QuotaCenter.
func NewQuotaCenter(proxies proxyutil.ProxyClientManagerInterface, queryCoord types.QueryCoordClient,
	dataCoord types.DataCoordClient, tsoAllocator tso.Allocator, meta IMetaTable,
) *QuotaCenter {
	ctx, cancel := context.WithCancel(context.TODO())

	q := &QuotaCenter{
		ctx:                  ctx,
		cancel:               cancel,
		proxies:              proxies,
		queryCoord:           queryCoord,
		dataCoord:            dataCoord,
		tsoAllocator:         tsoAllocator,
		meta:                 meta,
		readableCollections:  make(map[int64]map[int64][]int64, 0),
		writableCollections:  make(map[int64]map[int64][]int64, 0),
		rateLimiter:          rlinternal.NewRateLimiterTree(initInfLimiter(internalpb.RateScope_Cluster, allOps)),
		rateAllocateStrategy: DefaultRateAllocateStrategy,
		stopChan:             make(chan struct{}),
	}
	q.clearMetrics()
	return q
}

func initInfLimiter(rateScope internalpb.RateScope, opType opType) *rlinternal.RateLimiterNode {
	return initLimiter(GetInfLimiter, rateScope, opType)
}

func newParamLimiterFunc(rateScope internalpb.RateScope, opType opType) func() *rlinternal.RateLimiterNode {
	return func() *rlinternal.RateLimiterNode {
		return initLimiter(func(rt internalpb.RateType) *ratelimitutil.Limiter {
			limitVal := quota.GetQuotaValue(rateScope, rt, Params)
			return ratelimitutil.NewLimiter(Limit(limitVal), 0)
		}, rateScope, opType)
	}
}

func newParamLimiterFuncWithLimitFunc(rateScope internalpb.RateScope,
	opType opType,
	limitFunc func(internalpb.RateType) Limit,
) func() *rlinternal.RateLimiterNode {
	return func() *rlinternal.RateLimiterNode {
		return initLimiter(func(rt internalpb.RateType) *ratelimitutil.Limiter {
			limitVal := limitFunc(rt)
			return ratelimitutil.NewLimiter(limitVal, 0)
		}, rateScope, opType)
	}
}

func initLimiter(limiterFunc func(internalpb.RateType) *ratelimitutil.Limiter, rateScope internalpb.RateScope, opType opType) *rlinternal.RateLimiterNode {
	rateLimiters := rlinternal.NewRateLimiterNode(rateScope)
	getRateTypes(rateScope, opType).Range(func(rt internalpb.RateType) bool {
		rateLimiters.GetLimiters().GetOrInsert(rt, limiterFunc(rt))
		return true
	})
	return rateLimiters
}

func updateLimiter(node *rlinternal.RateLimiterNode, limiter *ratelimitutil.Limiter, rateScope internalpb.RateScope, opType opType) {
	if node == nil {
		log.Warn("update limiter failed, node is nil", zap.Any("rateScope", rateScope), zap.Any("opType", opType))
		return
	}
	limiters := node.GetLimiters()
	getRateTypes(rateScope, opType).Range(func(rt internalpb.RateType) bool {
		originLimiter, ok := limiters.Get(rt)
		if !ok {
			log.Warn("update limiter failed, limiter not found",
				zap.Any("rateScope", rateScope),
				zap.Any("opType", opType),
				zap.Any("rateType", rt))
			return true
		}
		originLimiter.SetLimit(limiter.Limit())
		return true
	})
}

func getRateTypes(scope internalpb.RateScope, opType opType) typeutil.Set[internalpb.RateType] {
	var allRateTypes typeutil.Set[internalpb.RateType]
	switch scope {
	case internalpb.RateScope_Cluster:
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

func (q *QuotaCenter) Start() {
	q.wg.Add(1)
	go func() {
		defer q.wg.Done()
		q.run()
	}()
}

func (q *QuotaCenter) watchQuotaAndLimit() {
	pt := paramtable.Get()
	pt.Watch(pt.QuotaConfig.QueryNodeMemoryHighWaterLevel.Key, config.NewHandler(pt.QuotaConfig.QueryNodeMemoryHighWaterLevel.Key, func(event *config.Event) {
		metrics.QueryNodeMemoryHighWaterLevel.Set(pt.QuotaConfig.QueryNodeMemoryHighWaterLevel.GetAsFloat())
	}))
	pt.Watch(pt.QuotaConfig.DiskQuota.Key, config.NewHandler(pt.QuotaConfig.DiskQuota.Key, func(event *config.Event) {
		metrics.DiskQuota.WithLabelValues(paramtable.GetStringNodeID(), "cluster").Set(pt.QuotaConfig.DiskQuota.GetAsFloat())
	}))
	pt.Watch(pt.QuotaConfig.DiskQuotaPerDB.Key, config.NewHandler(pt.QuotaConfig.DiskQuotaPerDB.Key, func(event *config.Event) {
		metrics.DiskQuota.WithLabelValues(paramtable.GetStringNodeID(), "db").Set(pt.QuotaConfig.DiskQuotaPerDB.GetAsFloat())
	}))
	pt.Watch(pt.QuotaConfig.DiskQuotaPerCollection.Key, config.NewHandler(pt.QuotaConfig.DiskQuotaPerCollection.Key, func(event *config.Event) {
		metrics.DiskQuota.WithLabelValues(paramtable.GetStringNodeID(), "collection").Set(pt.QuotaConfig.DiskQuotaPerCollection.GetAsFloat())
	}))
	pt.Watch(pt.QuotaConfig.DiskQuotaPerPartition.Key, config.NewHandler(pt.QuotaConfig.DiskQuotaPerPartition.Key, func(event *config.Event) {
		metrics.DiskQuota.WithLabelValues(paramtable.GetStringNodeID(), "collection").Set(pt.QuotaConfig.DiskQuotaPerPartition.GetAsFloat())
	}))
}

// run starts the service of QuotaCenter.
func (q *QuotaCenter) run() {
	interval := Params.QuotaConfig.QuotaCenterCollectInterval.GetAsDuration(time.Second)
	log.Info("Start QuotaCenter", zap.Duration("collectInterval", interval))
	q.watchQuotaAndLimit()
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
	log.Info("stop quota center")
	q.stopOnce.Do(func() {
		// cancel all blocking request to coord
		q.cancel()
		close(q.stopChan)
	})
	q.wg.Wait()
}

// clearMetrics removes all metrics stored in QuotaCenter.
func (q *QuotaCenter) clearMetrics() {
	q.dataNodeMetrics = make(map[UniqueID]*metricsinfo.DataNodeQuotaMetrics, 0)
	q.queryNodeMetrics = make(map[UniqueID]*metricsinfo.QueryNodeQuotaMetrics, 0)
	q.proxyMetrics = make(map[UniqueID]*metricsinfo.ProxyQuotaMetrics, 0)
	q.collectionIDToDBID = typeutil.NewConcurrentMap[int64, int64]()
	q.collections = typeutil.NewConcurrentMap[string, int64]()
	q.dbs = typeutil.NewConcurrentMap[string, int64]()
}

func updateNumEntitiesLoaded(current map[int64]int64, qn *metricsinfo.QueryNodeCollectionMetrics) map[int64]int64 {
	for collectionID, rowNum := range qn.CollectionRows {
		current[collectionID] += rowNum
	}
	return current
}

func FormatCollectionKey(dbID int64, collectionName string) string {
	return fmt.Sprintf("%d.%s", dbID, collectionName)
}

func SplitCollectionKey(key string) (dbID int64, collectionName string) {
	splits := strings.Split(key, ".")
	if len(splits) == 2 {
		dbID, _ = strconv.ParseInt(splits[0], 10, 64)
		collectionName = splits[1]
	}
	return
}

// collectMetrics sends GetMetrics requests to DataCoord and QueryCoord to sync the metrics in DataNodes and QueryNodes.
func (q *QuotaCenter) collectMetrics() error {
	oldDataNodes := typeutil.NewSet(lo.Keys(q.dataNodeMetrics)...)
	oldQueryNodes := typeutil.NewSet(lo.Keys(q.queryNodeMetrics)...)
	q.clearMetrics()

	ctx, cancel := context.WithTimeout(q.ctx, GetMetricsTimeout)
	defer cancel()

	group := &errgroup.Group{}

	// get Query cluster metrics
	group.Go(func() error {
		queryCoordTopology, err := getQueryCoordMetrics(ctx, q.queryCoord)
		if err != nil {
			return err
		}

		collections := typeutil.NewUniqueSet()
		numEntitiesLoaded := make(map[int64]int64)
		for _, queryNodeMetric := range queryCoordTopology.Cluster.ConnectedNodes {
			if queryNodeMetric.QuotaMetrics != nil {
				oldQueryNodes.Remove(queryNodeMetric.ID)
				q.queryNodeMetrics[queryNodeMetric.ID] = queryNodeMetric.QuotaMetrics
				collections.Insert(queryNodeMetric.QuotaMetrics.Effect.CollectionIDs...)
			}
			if queryNodeMetric.CollectionMetrics != nil {
				numEntitiesLoaded = updateNumEntitiesLoaded(numEntitiesLoaded, queryNodeMetric.CollectionMetrics)
			}
		}

		q.readableCollections = make(map[int64]map[int64][]int64, 0)
		var rangeErr error
		collections.Range(func(collectionID int64) bool {
			coll, getErr := q.meta.GetCollectionByIDWithMaxTs(context.TODO(), collectionID)
			if getErr != nil {
				// skip limit check if the collection meta has been removed from rootcoord meta
				return true
			}
			collIDToPartIDs, ok := q.readableCollections[coll.DBID]
			if !ok {
				collIDToPartIDs = make(map[int64][]int64)
				q.readableCollections[coll.DBID] = collIDToPartIDs
			}
			collIDToPartIDs[collectionID] = append(collIDToPartIDs[collectionID],
				lo.Map(coll.Partitions, func(part *model.Partition, _ int) int64 { return part.PartitionID })...)
			q.collectionIDToDBID.Insert(collectionID, coll.DBID)
			q.collections.Insert(FormatCollectionKey(coll.DBID, coll.Name), collectionID)
			if numEntity, ok := numEntitiesLoaded[collectionID]; ok {
				metrics.RootCoordNumEntities.WithLabelValues(coll.Name, metrics.LoadedLabel).Set(float64(numEntity))
			}
			return true
		})

		return rangeErr
	})
	// get Data cluster metrics
	group.Go(func() error {
		dataCoordTopology, err := getDataCoordMetrics(ctx, q.dataCoord)
		if err != nil {
			return err
		}

		collections := typeutil.NewUniqueSet()
		for _, dataNodeMetric := range dataCoordTopology.Cluster.ConnectedDataNodes {
			if dataNodeMetric.QuotaMetrics != nil {
				oldDataNodes.Remove(dataNodeMetric.ID)
				q.dataNodeMetrics[dataNodeMetric.ID] = dataNodeMetric.QuotaMetrics
				collections.Insert(dataNodeMetric.QuotaMetrics.Effect.CollectionIDs...)
			}
		}

		datacoordQuotaCollections := make([]int64, 0)
		q.diskMu.Lock()
		if dataCoordTopology.Cluster.Self.QuotaMetrics != nil {
			q.dataCoordMetrics = dataCoordTopology.Cluster.Self.QuotaMetrics
			for metricCollection := range q.dataCoordMetrics.PartitionsBinlogSize {
				datacoordQuotaCollections = append(datacoordQuotaCollections, metricCollection)
			}
		}
		q.diskMu.Unlock()

		q.writableCollections = make(map[int64]map[int64][]int64, 0)
		var collectionMetrics map[int64]*metricsinfo.DataCoordCollectionInfo
		cm := dataCoordTopology.Cluster.Self.CollectionMetrics
		if cm != nil {
			collectionMetrics = cm.Collections
		}

		collections.Range(func(collectionID int64) bool {
			coll, getErr := q.meta.GetCollectionByIDWithMaxTs(context.TODO(), collectionID)
			if getErr != nil {
				// skip limit check if the collection meta has been removed from rootcoord meta
				return true
			}

			collIDToPartIDs, ok := q.writableCollections[coll.DBID]
			if !ok {
				collIDToPartIDs = make(map[int64][]int64)
				q.writableCollections[coll.DBID] = collIDToPartIDs
			}
			collIDToPartIDs[collectionID] = append(collIDToPartIDs[collectionID],
				lo.Map(coll.Partitions, func(part *model.Partition, _ int) int64 { return part.PartitionID })...)
			q.collectionIDToDBID.Insert(collectionID, coll.DBID)
			q.collections.Insert(FormatCollectionKey(coll.DBID, coll.Name), collectionID)
			if collectionMetrics == nil {
				return true
			}
			if datacoordCollectionMetric, ok := collectionMetrics[collectionID]; ok {
				metrics.RootCoordNumEntities.WithLabelValues(coll.Name, metrics.TotalLabel).Set(float64(datacoordCollectionMetric.NumEntitiesTotal))
				fields := lo.KeyBy(coll.Fields, func(v *model.Field) int64 { return v.FieldID })
				for _, indexInfo := range datacoordCollectionMetric.IndexInfo {
					if _, ok := fields[indexInfo.FieldID]; !ok {
						continue
					}
					field := fields[indexInfo.FieldID]
					metrics.RootCoordIndexedNumEntities.WithLabelValues(
						coll.Name,
						indexInfo.IndexName,
						strconv.FormatBool(typeutil.IsVectorType(field.DataType))).Set(float64(indexInfo.NumEntitiesIndexed))
				}
			}
			return true
		})

		for _, collectionID := range datacoordQuotaCollections {
			_, ok := q.collectionIDToDBID.Get(collectionID)
			if ok {
				continue
			}
			coll, getErr := q.meta.GetCollectionByIDWithMaxTs(context.TODO(), collectionID)
			if getErr != nil {
				// skip limit check if the collection meta has been removed from rootcoord meta
				continue
			}
			q.collectionIDToDBID.Insert(collectionID, coll.DBID)
			q.collections.Insert(FormatCollectionKey(coll.DBID, coll.Name), collectionID)
		}

		return nil
	})
	// get Proxies metrics
	group.Go(func() error {
		ret, err := getProxyMetrics(ctx, q.proxies)
		if err != nil {
			return err
		}
		for _, proxyMetric := range ret {
			if proxyMetric.QuotaMetrics != nil {
				q.proxyMetrics[proxyMetric.ID] = proxyMetric.QuotaMetrics
			}
		}
		return nil
	})
	group.Go(func() error {
		dbs, err := q.meta.ListDatabases(ctx, typeutil.MaxTimestamp)
		if err != nil {
			return err
		}
		for _, db := range dbs {
			q.dbs.Insert(db.Name, db.ID)
		}
		return nil
	})

	err := group.Wait()
	if err != nil {
		return err
	}

	for oldDN := range oldDataNodes {
		metrics.RootCoordTtDelay.DeleteLabelValues(typeutil.DataNodeRole, strconv.FormatInt(oldDN, 10))
	}
	for oldQN := range oldQueryNodes {
		metrics.RootCoordTtDelay.DeleteLabelValues(typeutil.QueryNodeRole, strconv.FormatInt(oldQN, 10))
	}
	return nil
}

// forceDenyWriting sets dml rates to 0 to reject all dml requests.
func (q *QuotaCenter) forceDenyWriting(errorCode commonpb.ErrorCode, cluster bool, dbIDs, collectionIDs []int64, col2partitionIDs map[int64][]int64) error {
	log := log.Ctx(context.TODO()).WithRateGroup("quotaCenter.forceDenyWriting", 1.0, 60.0)
	if cluster {
		clusterLimiters := q.rateLimiter.GetRootLimiters()
		updateLimiter(clusterLimiters, GetEarliestLimiter(), internalpb.RateScope_Cluster, dml)
		clusterLimiters.GetQuotaStates().Insert(milvuspb.QuotaState_DenyToWrite, errorCode)
	}

	for _, dbID := range dbIDs {
		dbLimiters := q.rateLimiter.GetDatabaseLimiters(dbID)
		if dbLimiters == nil {
			log.Warn("db limiter not found of db ID", zap.Int64("dbID", dbID))
			continue
		}
		updateLimiter(dbLimiters, GetEarliestLimiter(), internalpb.RateScope_Database, dml)
		dbLimiters.GetQuotaStates().Insert(milvuspb.QuotaState_DenyToWrite, errorCode)
	}

	for _, collectionID := range collectionIDs {
		dbID, ok := q.collectionIDToDBID.Get(collectionID)
		if !ok {
			return fmt.Errorf("db ID not found of collection ID: %d", collectionID)
		}
		collectionLimiter := q.rateLimiter.GetCollectionLimiters(dbID, collectionID)
		if collectionLimiter == nil {
			log.Warn("collection limiter not found of collection ID",
				zap.Int64("dbID", dbID),
				zap.Int64("collectionID", collectionID))
			continue
		}
		updateLimiter(collectionLimiter, GetEarliestLimiter(), internalpb.RateScope_Collection, dml)
		collectionLimiter.GetQuotaStates().Insert(milvuspb.QuotaState_DenyToWrite, errorCode)
	}

	for collectionID, partitionIDs := range col2partitionIDs {
		for _, partitionID := range partitionIDs {
			dbID, ok := q.collectionIDToDBID.Get(collectionID)
			if !ok {
				return fmt.Errorf("db ID not found of collection ID: %d", collectionID)
			}
			partitionLimiter := q.rateLimiter.GetPartitionLimiters(dbID, collectionID, partitionID)
			if partitionLimiter == nil {
				log.Warn("partition limiter not found of partition ID",
					zap.Int64("dbID", dbID),
					zap.Int64("collectionID", collectionID),
					zap.Int64("partitionID", partitionID))
				continue
			}
			updateLimiter(partitionLimiter, GetEarliestLimiter(), internalpb.RateScope_Partition, dml)
			partitionLimiter.GetQuotaStates().Insert(milvuspb.QuotaState_DenyToWrite, errorCode)
		}
	}

	if cluster || len(dbIDs) > 0 || len(collectionIDs) > 0 || len(col2partitionIDs) > 0 {
		log.RatedWarn(30, "QuotaCenter force to deny writing",
			zap.Bool("cluster", cluster),
			zap.Int64s("dbIDs", dbIDs),
			zap.Int64s("collectionIDs", collectionIDs),
			zap.Any("partitionIDs", col2partitionIDs),
			zap.String("reason", errorCode.String()))
	}

	return nil
}

// forceDenyReading sets dql rates to 0 to reject all dql requests.
func (q *QuotaCenter) forceDenyReading(errorCode commonpb.ErrorCode, cluster bool, dbIDs []int64, mlog *log.MLogger) {
	if cluster {
		var collectionIDs []int64
		for dbID, collectionIDToPartIDs := range q.readableCollections {
			for collectionID := range collectionIDToPartIDs {
				collectionLimiter := q.rateLimiter.GetCollectionLimiters(dbID, collectionID)
				updateLimiter(collectionLimiter, GetEarliestLimiter(), internalpb.RateScope_Collection, dql)
				collectionLimiter.GetQuotaStates().Insert(milvuspb.QuotaState_DenyToRead, errorCode)
				collectionIDs = append(collectionIDs, collectionID)
			}
		}

		mlog.RatedWarn(10, "QuotaCenter force to deny reading",
			zap.Int64s("collectionIDs", collectionIDs),
			zap.String("reason", errorCode.String()))
	}

	if len(dbIDs) > 0 {
		for _, dbID := range dbIDs {
			dbLimiters := q.rateLimiter.GetDatabaseLimiters(dbID)
			if dbLimiters == nil {
				log.Warn("db limiter not found of db ID", zap.Int64("dbID", dbID))
				continue
			}
			updateLimiter(dbLimiters, GetEarliestLimiter(), internalpb.RateScope_Database, dql)
			dbLimiters.GetQuotaStates().Insert(milvuspb.QuotaState_DenyToRead, errorCode)
			mlog.RatedWarn(10, "QuotaCenter force to deny reading",
				zap.Int64s("dbIDs", dbIDs),
				zap.String("reason", errorCode.String()))
		}
	}
}

// getRealTimeRate return real time rate in Proxy.
func (q *QuotaCenter) getRealTimeRate(label string) float64 {
	var rate float64
	for _, metric := range q.proxyMetrics {
		for _, r := range metric.Rms {
			if r.Label == label {
				rate += r.Rate
				break
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

func (q *QuotaCenter) getDenyReadingDBs() map[int64]struct{} {
	dbIDs := make(map[int64]struct{})
	for _, dbID := range lo.Uniq(q.collectionIDToDBID.Values()) {
		if db, err := q.meta.GetDatabaseByID(q.ctx, dbID, typeutil.MaxTimestamp); err == nil {
			if v := db.GetProperty(common.DatabaseForceDenyReadingKey); v != "" {
				if dbForceDenyReadingEnabled, _ := strconv.ParseBool(v); dbForceDenyReadingEnabled {
					dbIDs[dbID] = struct{}{}
				}
			}
		}
	}
	return dbIDs
}

// getReadRates get rate information of collections and databases from proxy metrics
func (q *QuotaCenter) getReadRates() (map[string]float64, map[string]map[string]map[string]float64) {
	// label metric
	metricMap := make(map[string]float64)
	// sub label metric, label -> db -> collection -> value
	collectionMetricMap := make(map[string]map[string]map[string]float64)
	for _, metric := range q.proxyMetrics {
		for _, rm := range metric.Rms {
			if !ratelimitutil.IsSubLabel(rm.Label) {
				metricMap[rm.Label] += rm.Rate
				continue
			}
			mainLabel, database, collection, ok := ratelimitutil.SplitCollectionSubLabel(rm.Label)
			if !ok {
				continue
			}
			labelMetric, ok := collectionMetricMap[mainLabel]
			if !ok {
				labelMetric = make(map[string]map[string]float64)
				collectionMetricMap[mainLabel] = labelMetric
			}
			databaseMetric, ok := labelMetric[database]
			if !ok {
				databaseMetric = make(map[string]float64)
				labelMetric[database] = databaseMetric
			}
			databaseMetric[collection] += rm.Rate
		}
	}
	return metricMap, collectionMetricMap
}

func (q *QuotaCenter) getLimitedDBAndCollections(metricMap map[string]float64,
	collectionMetricMap map[string]map[string]map[string]float64,
) (bool, *typeutil.Set[string], *typeutil.Set[string]) {
	limitDBNameSet := typeutil.NewSet[string]()
	limitCollectionNameSet := typeutil.NewSet[string]()
	clusterLimit := false

	formatCollctionRateKey := func(dbName, collectionName string) string {
		return fmt.Sprintf("%s.%s", dbName, collectionName)
	}

	enableResultProtection := Params.QuotaConfig.ResultProtectionEnabled.GetAsBool()
	if enableResultProtection {
		maxRate := Params.QuotaConfig.MaxReadResultRate.GetAsFloat()
		maxDBRate := Params.QuotaConfig.MaxReadResultRatePerDB.GetAsFloat()
		maxCollectionRate := Params.QuotaConfig.MaxReadResultRatePerCollection.GetAsFloat()

		dbRateCount := make(map[string]float64)
		collectionRateCount := make(map[string]float64)
		rateCount := metricMap[metricsinfo.ReadResultThroughput]
		for mainLabel, labelMetric := range collectionMetricMap {
			if mainLabel != metricsinfo.ReadResultThroughput {
				continue
			}
			for database, databaseMetric := range labelMetric {
				for collection, metricValue := range databaseMetric {
					dbRateCount[database] += metricValue
					collectionRateCount[formatCollctionRateKey(database, collection)] = metricValue
				}
			}
		}
		if rateCount >= maxRate {
			clusterLimit = true
		}
		for s, f := range dbRateCount {
			if f >= maxDBRate {
				limitDBNameSet.Insert(s)
			}
		}
		for s, f := range collectionRateCount {
			if f >= maxCollectionRate {
				limitCollectionNameSet.Insert(s)
			}
		}
	}
	return clusterLimit, &limitDBNameSet, &limitCollectionNameSet
}

func (q *QuotaCenter) coolOffDatabaseReading(deniedDatabaseIDs map[int64]struct{}, limitDBNameSet *typeutil.Set[string],
	collectionMetricMap map[string]map[string]map[string]float64, log *log.MLogger,
) error {
	if limitDBNameSet.Len() > 0 {
		databaseSearchRate := make(map[string]float64)
		databaseQueryRate := make(map[string]float64)
		for mainLabel, labelMetric := range collectionMetricMap {
			var databaseRate map[string]float64
			if mainLabel == internalpb.RateType_DQLSearch.String() {
				databaseRate = databaseSearchRate
			} else if mainLabel == internalpb.RateType_DQLQuery.String() {
				databaseRate = databaseQueryRate
			} else {
				continue
			}
			for database, databaseMetric := range labelMetric {
				for _, metricValue := range databaseMetric {
					databaseRate[database] += metricValue
				}
			}
		}

		coolOffSpeed := Params.QuotaConfig.CoolOffSpeed.GetAsFloat()
		limitDBNameSet.Range(func(name string) bool {
			dbID, ok := q.dbs.Get(name)
			if !ok {
				log.Warn("db not found", zap.String("dbName", name))
				return true
			}

			// skip this database because it has been denied access for reading
			_, ok = deniedDatabaseIDs[dbID]
			if ok {
				return true
			}

			dbLimiter := q.rateLimiter.GetDatabaseLimiters(dbID)
			if dbLimiter == nil {
				log.Warn("database limiter not found", zap.Int64("dbID", dbID))
				return true
			}

			realTimeSearchRate := databaseSearchRate[name]
			realTimeQueryRate := databaseQueryRate[name]
			q.coolOffReading(realTimeSearchRate, realTimeQueryRate, coolOffSpeed, dbLimiter, log)
			return true
		})
	}
	return nil
}

func (q *QuotaCenter) coolOffCollectionReading(deniedDatabaseIDs map[int64]struct{}, limitCollectionSet *typeutil.UniqueSet, limitCollectionNameSet *typeutil.Set[string],
	collectionMetricMap map[string]map[string]map[string]float64, log *log.MLogger,
) error {
	var updateLimitErr error
	coolOffSpeed := Params.QuotaConfig.CoolOffSpeed.GetAsFloat()

	splitCollctionRateKey := func(key string) (string, string) {
		parts := strings.Split(key, ".")
		return parts[0], parts[1]
	}

	dbIDs := make(map[int64]string, q.dbs.Len())
	collectionIDs := make(map[int64]string, q.collections.Len())
	q.dbs.Range(func(name string, id int64) bool {
		dbIDs[id] = name
		return true
	})
	q.collections.Range(func(name string, id int64) bool {
		_, collectionName := SplitCollectionKey(name)
		collectionIDs[id] = collectionName
		return true
	})

	limitCollectionNameSet.Range(func(name string) bool {
		dbName, collectionName := splitCollctionRateKey(name)
		dbID, ok := q.dbs.Get(dbName)
		if !ok {
			log.Warn("db not found", zap.String("dbName", dbName))
			updateLimitErr = fmt.Errorf("db not found: %s", dbName)
			return true
		}
		collectionID, ok := q.collections.Get(FormatCollectionKey(dbID, collectionName))
		if !ok {
			log.Warn("collection not found", zap.String("collectionName", name))
			updateLimitErr = fmt.Errorf("collection not found: %s", name)
			return true
		}
		limitCollectionSet.Insert(collectionID)
		return true
	})
	if updateLimitErr != nil {
		return updateLimitErr
	}

	safeGetCollectionRate := func(label, dbName, collectionName string) float64 {
		if labelMetric, ok := collectionMetricMap[label]; ok {
			if dbMetric, ok := labelMetric[dbName]; ok {
				if rate, ok := dbMetric[collectionName]; ok {
					return rate
				}
			}
		}
		return 0
	}

	coolOffCollectionID := func(collections ...int64) error {
		for _, collection := range collections {
			dbID, ok := q.collectionIDToDBID.Get(collection)
			if !ok {
				return fmt.Errorf("db ID not found of collection ID: %d", collection)
			}
			// skip this database because it has been denied access for reading
			_, ok = deniedDatabaseIDs[dbID]
			if ok {
				continue
			}

			collectionLimiter := q.rateLimiter.GetCollectionLimiters(dbID, collection)
			if collectionLimiter == nil {
				return fmt.Errorf("collection limiter not found: %d", collection)
			}
			dbName, ok := dbIDs[dbID]
			if !ok {
				return fmt.Errorf("db name not found of db ID: %d", dbID)
			}
			collectionName, ok := collectionIDs[collection]
			if !ok {
				return fmt.Errorf("collection name not found of collection ID: %d", collection)
			}

			realTimeSearchRate := safeGetCollectionRate(internalpb.RateType_DQLSearch.String(), dbName, collectionName)
			realTimeQueryRate := safeGetCollectionRate(internalpb.RateType_DQLQuery.String(), dbName, collectionName)
			q.coolOffReading(realTimeSearchRate, realTimeQueryRate, coolOffSpeed, collectionLimiter, log)

			collectionProps := q.getCollectionLimitProperties(collection)
			q.guaranteeMinRate(getCollectionRateLimitConfig(collectionProps, common.CollectionSearchRateMinKey),
				internalpb.RateType_DQLSearch, collectionLimiter)
			q.guaranteeMinRate(getCollectionRateLimitConfig(collectionProps, common.CollectionQueryRateMinKey),
				internalpb.RateType_DQLQuery, collectionLimiter)
		}
		return nil
	}

	if updateLimitErr = coolOffCollectionID(limitCollectionSet.Collect()...); updateLimitErr != nil {
		return updateLimitErr
	}
	return nil
}

// calculateReadRates calculates and sets dql rates.
func (q *QuotaCenter) calculateReadRates() error {
	log := log.Ctx(context.Background()).WithRateGroup("rootcoord.QuotaCenter", 1.0, 60.0)
	if Params.QuotaConfig.ForceDenyReading.GetAsBool() {
		q.forceDenyReading(commonpb.ErrorCode_ForceDeny, true, []int64{}, log)
		return nil
	}

	deniedDatabaseIDs := q.getDenyReadingDBs()
	if len(deniedDatabaseIDs) != 0 {
		q.forceDenyReading(commonpb.ErrorCode_ForceDeny, false, maps.Keys(deniedDatabaseIDs), log)
	}

	queueLatencyThreshold := Params.QuotaConfig.QueueLatencyThreshold.GetAsDuration(time.Second)
	limitCollectionSet := typeutil.NewUniqueSet()

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
	enableQueueProtection := Params.QuotaConfig.QueueProtectionEnabled.GetAsBool()
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

	metricMap, collectionMetricMap := q.getReadRates()
	clusterLimit, limitDBNameSet, limitCollectionNameSet := q.getLimitedDBAndCollections(metricMap, collectionMetricMap)

	coolOffSpeed := Params.QuotaConfig.CoolOffSpeed.GetAsFloat()

	if clusterLimit {
		realTimeClusterSearchRate := metricMap[internalpb.RateType_DQLSearch.String()]
		realTimeClusterQueryRate := metricMap[internalpb.RateType_DQLQuery.String()]
		q.coolOffReading(realTimeClusterSearchRate, realTimeClusterQueryRate, coolOffSpeed, q.rateLimiter.GetRootLimiters(), log)
	}

	if updateLimitErr := q.coolOffDatabaseReading(deniedDatabaseIDs, limitDBNameSet, collectionMetricMap,
		log); updateLimitErr != nil {
		return updateLimitErr
	}

	if updateLimitErr := q.coolOffCollectionReading(deniedDatabaseIDs, &limitCollectionSet, limitCollectionNameSet,
		collectionMetricMap, log); updateLimitErr != nil {
		return updateLimitErr
	}

	return nil
}

func (q *QuotaCenter) coolOffReading(realTimeSearchRate, realTimeQueryRate, coolOffSpeed float64,
	node *rlinternal.RateLimiterNode, mlog *log.MLogger,
) {
	limiter := node.GetLimiters()

	v, ok := limiter.Get(internalpb.RateType_DQLSearch)
	if ok && v.Limit() != Inf && realTimeSearchRate > 0 {
		v.SetLimit(Limit(realTimeSearchRate * coolOffSpeed))
		mlog.RatedWarn(10, "QuotaCenter cool read rates off done",
			zap.Any("level", node.Level()),
			zap.Any("id", node.GetID()),
			zap.Any("searchRate", v.Limit()))
	}

	v, ok = limiter.Get(internalpb.RateType_DQLQuery)
	if ok && v.Limit() != Inf && realTimeQueryRate > 0 {
		v.SetLimit(Limit(realTimeQueryRate * coolOffSpeed))
		mlog.RatedWarn(10, "QuotaCenter cool read rates off done",
			zap.Any("level", node.Level()),
			zap.Any("id", node.GetID()),
			zap.Any("queryRate", v.Limit()))
	}
}

func (q *QuotaCenter) getDenyWritingDBs() map[int64]struct{} {
	dbIDs := make(map[int64]struct{})
	for _, dbID := range lo.Uniq(q.collectionIDToDBID.Values()) {
		if db, err := q.meta.GetDatabaseByID(q.ctx, dbID, typeutil.MaxTimestamp); err == nil {
			if v := db.GetProperty(common.DatabaseForceDenyWritingKey); v != "" {
				if dbForceDenyWritingEnabled, _ := strconv.ParseBool(v); dbForceDenyWritingEnabled {
					dbIDs[dbID] = struct{}{}
				}
			}
		}
	}
	return dbIDs
}

// calculateWriteRates calculates and sets dml rates.
func (q *QuotaCenter) calculateWriteRates() error {
	log := log.Ctx(context.Background()).WithRateGroup("rootcoord.QuotaCenter", 1.0, 60.0)
	// check force deny writing of cluster level
	if Params.QuotaConfig.ForceDenyWriting.GetAsBool() {
		return q.forceDenyWriting(commonpb.ErrorCode_ForceDeny, true, nil, nil, nil)
	}

	// check force deny writing of db level
	dbIDs := q.getDenyWritingDBs()
	if len(dbIDs) != 0 {
		if err := q.forceDenyWriting(commonpb.ErrorCode_ForceDeny, false, maps.Keys(dbIDs), nil, nil); err != nil {
			return err
		}
	}

	if err := q.checkDiskQuota(dbIDs); err != nil {
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
	l0Factors := q.getL0SegmentsSizeFactor()
	updateCollectionFactor(l0Factors)

	ttCollections := make([]int64, 0)
	memoryCollections := make([]int64, 0)

	for collection, factor := range collectionFactors {
		metrics.RootCoordRateLimitRatio.WithLabelValues(fmt.Sprint(collection)).Set(1 - factor)
		if factor <= 0 {
			if _, ok := ttFactors[collection]; ok && factor == ttFactors[collection] {
				// factor comes from ttFactor
				ttCollections = append(ttCollections, collection)
			} else {
				memoryCollections = append(memoryCollections, collection)
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
			internalpb.RateType_DMLUpsert,
			internalpb.RateType_DMLDelete,
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

	if len(ttCollections) > 0 {
		if err = q.forceDenyWriting(commonpb.ErrorCode_TimeTickLongDelay, false, nil, ttCollections, nil); err != nil {
			log.Warn("fail to force deny writing for time tick delay", zap.Error(err))
			return err
		}
	}
	if len(memoryCollections) > 0 {
		if err = q.forceDenyWriting(commonpb.ErrorCode_MemoryQuotaExhausted, false, nil, memoryCollections, nil); err != nil {
			log.Warn("fail to force deny writing for memory quota", zap.Error(err))
			return err
		}
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

// getL0SegmentsSizeFactor checks wether any collection
func (q *QuotaCenter) getL0SegmentsSizeFactor() map[int64]float64 {
	if !Params.QuotaConfig.L0SegmentRowCountProtectionEnabled.GetAsBool() {
		return nil
	}

	l0segmentSizeLowWaterLevel := Params.QuotaConfig.L0SegmentRowCountLowWaterLevel.GetAsInt64()
	l0SegmentSizeHighWaterLevel := Params.QuotaConfig.L0SegmentRowCountHighWaterLevel.GetAsInt64()

	collectionFactor := make(map[int64]float64)
	for collectionID, l0RowCount := range q.dataCoordMetrics.CollectionL0RowCount {
		if l0RowCount < l0segmentSizeLowWaterLevel {
			continue
		}
		factor := float64(l0SegmentSizeHighWaterLevel-l0RowCount) / float64(l0SegmentSizeHighWaterLevel-l0segmentSizeLowWaterLevel)
		collectionFactor[collectionID] = factor
	}
	return collectionFactor
}

// calculateRates calculates target rates by different strategies.
func (q *QuotaCenter) calculateRates() error {
	err := q.resetAllCurrentRates()
	if err != nil {
		log.Warn("QuotaCenter resetAllCurrentRates failed", zap.Error(err))
		return err
	}

	err = q.calculateWriteRates()
	if err != nil {
		log.Warn("QuotaCenter calculateWriteRates failed", zap.Error(err))
		return err
	}
	err = q.calculateReadRates()
	if err != nil {
		log.Warn("QuotaCenter calculateReadRates failed", zap.Error(err))
		return err
	}

	// log.Debug("QuotaCenter calculates rate done", zap.Any("rates", q.currentRates))
	return nil
}

func (q *QuotaCenter) resetAllCurrentRates() error {
	clusterLimiter := newParamLimiterFunc(internalpb.RateScope_Cluster, allOps)()
	q.rateLimiter = rlinternal.NewRateLimiterTree(clusterLimiter)
	initLimiters := func(sourceCollections map[int64]map[int64][]int64) {
		for dbID, collections := range sourceCollections {
			for collectionID, partitionIDs := range collections {
				getCollectionLimitVal := func(rateType internalpb.RateType) Limit {
					limitVal, err := q.getCollectionMaxLimit(rateType, collectionID)
					if err != nil {
						return Limit(quota.GetQuotaValue(internalpb.RateScope_Collection, rateType, Params))
					}
					return limitVal
				}

				for _, partitionID := range partitionIDs {
					q.rateLimiter.GetOrCreatePartitionLimiters(dbID, collectionID, partitionID,
						newParamLimiterFunc(internalpb.RateScope_Database, allOps),
						newParamLimiterFuncWithLimitFunc(internalpb.RateScope_Collection, allOps, getCollectionLimitVal),
						newParamLimiterFunc(internalpb.RateScope_Partition, allOps))
				}
				if len(partitionIDs) == 0 {
					q.rateLimiter.GetOrCreateCollectionLimiters(dbID, collectionID,
						newParamLimiterFunc(internalpb.RateScope_Database, allOps),
						newParamLimiterFuncWithLimitFunc(internalpb.RateScope_Collection, allOps, getCollectionLimitVal))
				}
			}
			if len(collections) == 0 {
				q.rateLimiter.GetOrCreateDatabaseLimiters(dbID, newParamLimiterFunc(internalpb.RateScope_Database, allOps))
			}
		}
	}
	initLimiters(q.readableCollections)
	initLimiters(q.writableCollections)
	return nil
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
func (q *QuotaCenter) checkDiskQuota(denyWritingDBs map[int64]struct{}) error {
	log := log.Ctx(context.Background()).WithRateGroup("rootcoord.QuotaCenter", 1.0, 60.0)
	q.diskMu.Lock()
	defer q.diskMu.Unlock()
	if !Params.QuotaConfig.DiskProtectionEnabled.GetAsBool() {
		return nil
	}
	if q.dataCoordMetrics == nil {
		return nil
	}

	// check disk quota of cluster level
	totalDiskQuota := Params.QuotaConfig.DiskQuota.GetAsFloat()
	total := q.dataCoordMetrics.TotalBinlogSize
	if float64(total) >= totalDiskQuota {
		log.RatedWarn(10, "cluster disk quota exceeded", zap.Int64("disk usage", total), zap.Float64("disk quota", totalDiskQuota))
		err := q.forceDenyWriting(commonpb.ErrorCode_DiskQuotaExhausted, true, nil, nil, nil)
		if err != nil {
			log.Warn("fail to force deny writing", zap.Error(err))
		}
		return err
	}

	collectionDiskQuota := Params.QuotaConfig.DiskQuotaPerCollection.GetAsFloat()
	dbSizeInfo := make(map[int64]int64)
	collections := make([]int64, 0)
	for collection, binlogSize := range q.dataCoordMetrics.CollectionBinlogSize {
		collectionProps := q.getCollectionLimitProperties(collection)
		colDiskQuota := getRateLimitConfig(collectionProps, common.CollectionDiskQuotaKey, collectionDiskQuota)
		if float64(binlogSize) >= colDiskQuota {
			log.RatedWarn(10, "collection disk quota exceeded",
				zap.Int64("collection", collection),
				zap.Int64("coll disk usage", binlogSize),
				zap.Float64("coll disk quota", colDiskQuota))
			collections = append(collections, collection)
		}
		dbID, ok := q.collectionIDToDBID.Get(collection)
		if !ok {
			log.Warn("cannot find db id for collection", zap.Int64("collection", collection))
			continue
		}

		// skip db that has already denied writing
		if denyWritingDBs != nil {
			if _, ok = denyWritingDBs[dbID]; ok {
				continue
			}
		}
		dbSizeInfo[dbID] += binlogSize
	}

	col2partitions := make(map[int64][]int64)
	partitionDiskQuota := Params.QuotaConfig.DiskQuotaPerPartition.GetAsFloat()
	for collection, partitions := range q.dataCoordMetrics.PartitionsBinlogSize {
		for partition, binlogSize := range partitions {
			if float64(binlogSize) >= partitionDiskQuota {
				log.RatedWarn(10, "partition disk quota exceeded",
					zap.Int64("collection", collection),
					zap.Int64("partition", partition),
					zap.Int64("part disk usage", binlogSize),
					zap.Float64("part disk quota", partitionDiskQuota))
				col2partitions[collection] = append(col2partitions[collection], partition)
			}
		}
	}

	dbIDs := q.checkDBDiskQuota(dbSizeInfo)
	err := q.forceDenyWriting(commonpb.ErrorCode_DiskQuotaExhausted, false, dbIDs, collections, col2partitions)
	if err != nil {
		log.Warn("fail to force deny writing", zap.Error(err))
		return err
	}
	q.totalBinlogSize = total
	return nil
}

func (q *QuotaCenter) checkDBDiskQuota(dbSizeInfo map[int64]int64) []int64 {
	log := log.Ctx(context.Background()).WithRateGroup("rootcoord.QuotaCenter", 1.0, 60.0)
	dbIDs := make([]int64, 0)
	checkDiskQuota := func(dbID, binlogSize int64, quota float64) {
		if float64(binlogSize) >= quota {
			log.RatedWarn(10, "db disk quota exceeded",
				zap.Int64("db", dbID),
				zap.Int64("db disk usage", binlogSize),
				zap.Float64("db disk quota", quota))
			dbIDs = append(dbIDs, dbID)
		}
	}

	//  DB properties take precedence over quota configuration for disk quota.
	for dbID, binlogSize := range dbSizeInfo {
		db, err := q.meta.GetDatabaseByID(q.ctx, dbID, typeutil.MaxTimestamp)
		if err == nil {
			if dbDiskQuotaStr := db.GetProperty(common.DatabaseDiskQuotaKey); dbDiskQuotaStr != "" {
				if dbDiskQuotaBytes, err := strconv.ParseFloat(dbDiskQuotaStr, 64); err == nil {
					dbDiskQuotaMB := dbDiskQuotaBytes * 1024 * 1024
					checkDiskQuota(dbID, binlogSize, dbDiskQuotaMB)
					continue
				}
			}
		}
		checkDiskQuota(dbID, binlogSize, Params.QuotaConfig.DiskQuotaPerDB.GetAsFloat())
	}
	return dbIDs
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
			if !limiter.HasUpdated() {
				return true
			}
			r := limiter.Limit()
			if r != Inf {
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
	dbLimiters := make(map[int64]*proxypb.LimiterNode, clusterRateLimiter.GetChildren().Len())
	clusterRateLimiter.GetChildren().Range(func(dbID int64, dbRateLimiters *rlinternal.RateLimiterNode) bool {
		dbLimiter := q.toRequestLimiter(dbRateLimiters)

		// collect collection rate limit if dbRateLimiters has collection limiter children
		collectionLimiters := make(map[int64]*proxypb.LimiterNode, dbRateLimiters.GetChildren().Len())
		dbRateLimiters.GetChildren().Range(func(collectionID int64, collectionRateLimiters *rlinternal.RateLimiterNode) bool {
			collectionLimiter := q.toRequestLimiter(collectionRateLimiters)

			// collect partitions rate limit if collectionRateLimiters has partition limiter children
			partitionLimiters := make(map[int64]*proxypb.LimiterNode, collectionRateLimiters.GetChildren().Len())
			collectionRateLimiters.GetChildren().Range(func(partitionID int64, partitionRateLimiters *rlinternal.RateLimiterNode) bool {
				partitionLimiters[partitionID] = &proxypb.LimiterNode{
					Limiter:  q.toRequestLimiter(partitionRateLimiters),
					Children: make(map[int64]*proxypb.LimiterNode, 0),
				}
				return true
			})

			collectionLimiters[collectionID] = &proxypb.LimiterNode{
				Limiter:  collectionLimiter,
				Children: partitionLimiters,
			}
			return true
		})

		dbLimiters[dbID] = &proxypb.LimiterNode{
			Limiter:  dbLimiter,
			Children: collectionLimiters,
		}

		return true
	})

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
	metrics.RootCoordQuotaStates.Reset()
	dbIDs := make(map[int64]string, q.dbs.Len())
	collectionIDs := make(map[int64]string, q.collections.Len())
	q.dbs.Range(func(name string, id int64) bool {
		dbIDs[id] = name
		return true
	})
	q.collections.Range(func(name string, id int64) bool {
		_, collectionName := SplitCollectionKey(name)
		collectionIDs[id] = collectionName
		return true
	})

	record := func(errorCode commonpb.ErrorCode) {
		rlinternal.TraverseRateLimiterTree(q.rateLimiter.GetRootLimiters(), nil,
			func(node *rlinternal.RateLimiterNode, state milvuspb.QuotaState, errCode commonpb.ErrorCode) bool {
				if errCode == errorCode {
					var name string
					switch node.Level() {
					case internalpb.RateScope_Cluster:
						name = "cluster"
					case internalpb.RateScope_Database:
						name = "db_" + dbIDs[node.GetID()]
					case internalpb.RateScope_Collection:
						name = "collection_" + collectionIDs[node.GetID()]
					default:
						return false
					}
					metrics.RootCoordQuotaStates.WithLabelValues(errorCode.String(), name).Set(1.0)
					metrics.RootCoordForceDenyWritingCounter.Inc()
					return false
				}
				return true
			})
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

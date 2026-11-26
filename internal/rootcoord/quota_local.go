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
	"maps"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/util/quota"
	rlinternal "github.com/milvus-io/milvus/internal/util/ratelimitutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type localQuotaSource interface {
	ReadQuotaSnapshot(context.Context, bool) (*metricsinfo.QuotaSnapshot, error)
	AcknowledgeQuotaSnapshot(*metricsinfo.QuotaSnapshot)
}
type localQuotaMetadataSource interface {
	readQuotaMetadata(bool) *quotaMetadataSnapshot
	acknowledgeQuotaMetadata(*quotaMetadataSnapshot)
	quotaMetadataGeneration() uint64
}

// quotaNodeKey locates a node in the calculation tree and in its metrics index.
type quotaNodeKey struct {
	Scope                     internalpb.RateScope
	DB, Collection, Partition int64
}

type localQuotaState struct {
	proxyCount            int
	calculationProxyCount int
	metadata              map[int64]*quotaCollectionMetadata
	rows                  map[int64]int64
	dirty                 map[int64]bool
	deleted               map[quotaNodeKey]bool
	dbBytes               map[int64]int64
	dbCollections         map[int64]int
	exceeded              map[int64]bool
	exceededParts         map[int64][]int64
	previousEffects       map[int64]bool
	metricNodes           map[quotaNodeKey]*rlinternal.RateLimiterNode
	config                map[string]string
	generation            uint64
	initialized           bool
	full                  bool
}

func (q *QuotaCenter) prepareLocalQuota() {
	_, hasSource := q.mixCoord.(localQuotaSource)
	meta, hasMeta := q.meta.(localQuotaMetadataSource)
	if !Params.QuotaConfig.LocalQuotaCalculationEnabled.GetAsBool() || !Params.QuotaConfig.CollectionStatsCacheEnabled.GetAsBool() || !hasSource || !hasMeta {
		q.localQuota = nil
		return
	}
	first := q.localQuota == nil
	if first {
		q.localQuota = &localQuotaState{metadata: make(map[int64]*quotaCollectionMetadata), rows: make(map[int64]int64), dirty: make(map[int64]bool), deleted: make(map[quotaNodeKey]bool), dbBytes: make(map[int64]int64), dbCollections: make(map[int64]int), exceeded: make(map[int64]bool), exceededParts: make(map[int64][]int64), previousEffects: make(map[int64]bool), metricNodes: make(map[quotaNodeKey]*rlinternal.RateLimiterNode)}
		q.collectionIDToDBID = typeutil.NewConcurrentMap[int64, int64]()
		q.collections = typeutil.NewConcurrentMap[string, int64]()
		q.collectionProps = make(map[int64]map[string]string)
		q.diskMu.Lock()
		q.dataCoordMetrics = &metricsinfo.DataCoordQuotaMetrics{CollectionBinlogSize: make(map[int64]int64), PartitionsBinlogSize: make(map[int64]map[int64]int64), CollectionL0RowCount: make(map[int64]int64)}
		q.diskMu.Unlock()
	}
	local := q.localQuota
	if first {
		q.rateLimiter.GetRootLimiters().GetChildren().Range(func(db int64, node *rlinternal.RateLimiterNode) bool {
			node.GetChildren().Range(func(id int64, _ *rlinternal.RateLimiterNode) bool {
				local.deleted[quotaNodeKey{Scope: internalpb.RateScope_Collection, DB: db, Collection: id}] = true
				return true
			})
			return true
		})
	}
	configuration := Params.GetComponentConfigurations("rootcoord", "quota")
	local.calculationProxyCount = q.proxies.GetProxyCount()
	local.full = first || !maps.Equal(local.config, configuration) || local.proxyCount != local.calculationProxyCount
	local.config = configuration
	snapshot := meta.readQuotaMetadata(first)
	if snapshot.Full {
		for id := range local.metadata {
			if _, present := snapshot.Collections[id]; !present {
				snapshot.Collections[id] = &quotaCollectionMetadata{}
			}
		}
	}
	local.generation = snapshot.Generation
	for id, value := range snapshot.Collections {
		local.dirty[id] = true
		old := local.metadata[id]
		if old != nil && old.Collection != nil {
			coll := old.Collection
			if bytes, ok := q.dataCoordMetrics.CollectionBinlogSize[id]; ok {
				q.updateQuotaDB(coll.DBID, bytes, -1)
			}
			q.collections.Remove(FormatCollectionKey(coll.DBID, coll.Name))
			if value.Collection == nil || value.Collection.DBName != coll.DBName || value.Collection.Name != coll.Name {
				metrics.CleanupRootCoordCollectionMetrics(coll.DBName, coll.Name)
			}
			if value.Collection == nil || value.Collection.DBID != coll.DBID {
				local.deleted[quotaNodeKey{Scope: internalpb.RateScope_Collection, DB: coll.DBID, Collection: id}] = true
			}
		}
		delete(q.collectionProps, id)
		if value.Collection == nil {
			delete(local.metadata, id)
			q.collectionIDToDBID.Remove(id)
		} else {
			local.metadata[id] = value
			coll := value.Collection
			q.collectionIDToDBID.Insert(id, coll.DBID)
			q.collections.Insert(FormatCollectionKey(coll.DBID, coll.Name), id)
			if bytes, ok := q.dataCoordMetrics.CollectionBinlogSize[id]; ok {
				q.updateQuotaDB(coll.DBID, bytes, 1)
			}
			properties := make(map[string]string, len(coll.Properties))
			for _, p := range coll.Properties {
				properties[p.Key] = p.Value
			}
			q.collectionProps[id] = properties
		}
	}
	// Consumption acknowledges the copied input, while dirty computation remains
	// independently retained here until a complete calculation is published.
	meta.acknowledgeQuotaMetadata(snapshot)
	if local.full {
		for id := range local.metadata {
			local.dirty[id] = true
		}
	}
}

func (q *QuotaCenter) updateQuotaDB(db, bytes int64, sign int) {
	local := q.localQuota
	local.dbBytes[db] += bytes * int64(sign)
	local.dbCollections[db] += sign
	if local.dbCollections[db] == 0 {
		delete(local.dbCollections, db)
		delete(local.dbBytes, db)
	}
}

func (q *QuotaCenter) collectLocalQuotaData(ctx context.Context, oldNodes typeutil.Set[int64], numEntitiesTotalByDB map[string]int64, aggregate bool) error {
	source := q.mixCoord.(localQuotaSource)
	local := q.localQuota
	nodes, err := getConnectedDataNodeMetrics(ctx, q.mixCoord)
	if err != nil {
		return err
	}
	snapshot, err := source.ReadQuotaSnapshot(ctx, !local.initialized)
	if err != nil {
		return err
	}
	q.diskMu.Lock()
	changes := snapshot.Collections
	if snapshot.Full && local.initialized {
		changes = maps.Clone(changes)
		for _, old := range []map[int64]int64{q.dataCoordMetrics.CollectionBinlogSize, local.rows} {
			for id := range old {
				if _, present := changes[id]; !present {
					changes[id] = &metricsinfo.CollectionQuotaSnapshot{}
				}
			}
		}
	}
	for id, value := range changes {
		local.dirty[id] = true
		db, hasDB := q.collectionIDToDBID.Get(id)
		if old, ok := q.dataCoordMetrics.CollectionBinlogSize[id]; ok && hasDB {
			q.updateQuotaDB(db, old, -1)
		}
		delete(q.dataCoordMetrics.CollectionBinlogSize, id)
		delete(q.dataCoordMetrics.PartitionsBinlogSize, id)
		delete(q.dataCoordMetrics.CollectionL0RowCount, id)
		delete(local.rows, id)
		if value.HasQuota {
			q.dataCoordMetrics.CollectionBinlogSize[id] = value.Bytes
			q.dataCoordMetrics.PartitionsBinlogSize[id] = value.PartitionBytes
			if hasDB {
				q.updateQuotaDB(db, value.Bytes, 1)
			}
		}
		if value.HasL0 {
			q.dataCoordMetrics.CollectionL0RowCount[id] = value.L0Rows
		}
		if value.HasRows {
			local.rows[id] = value.Rows
		}
	}
	q.dataCoordMetrics.TotalBinlogSize = snapshot.TotalBytes
	q.diskMu.Unlock()
	source.AcknowledgeQuotaSnapshot(snapshot)
	local.initialized = true
	q.writableCollections = make(map[int64]map[int64][]int64)
	ids := make(map[int64]bool)
	for _, node := range nodes {
		if node.QuotaMetrics != nil {
			oldNodes.Remove(node.ID)
			q.dataNodeMetrics[node.ID] = node.QuotaMetrics
			for _, id := range node.QuotaMetrics.Effect.CollectionIDs {
				ids[id] = true
			}
		}
	}
	for id := range ids {
		view := local.metadata[id]
		if view == nil || view.Collection == nil {
			continue
		}
		coll := view.Collection
		if q.writableCollections[coll.DBID] == nil {
			q.writableCollections[coll.DBID] = make(map[int64][]int64)
		}
		parts := make([]int64, 0, len(coll.Partitions))
		for _, p := range coll.Partitions {
			parts = append(parts, p.PartitionID)
		}
		q.writableCollections[coll.DBID][id] = parts
		if rows, ok := local.rows[id]; ok {
			if aggregate {
				numEntitiesTotalByDB[coll.DBName] += rows
			} else {
				metrics.RootCoordNumEntities.WithLabelValues(coll.DBName, coll.Name, metrics.TotalLabel).Set(float64(rows))
			}
		}
	}
	return nil
}

func (q *QuotaCenter) quotaDatabaseIDs() []int64 {
	if q.localQuota == nil {
		unique := make(map[int64]bool)
		for _, id := range q.collectionIDToDBID.Values() {
			unique[id] = true
		}
		result := make([]int64, 0, len(unique))
		for id := range unique {
			result = append(result, id)
		}
		return result
	}
	root := q.rateLimiter.GetRootLimiters()
	result := make([]int64, 0, root.GetChildren().Len())
	root.GetChildren().Range(func(db int64, node *rlinternal.RateLimiterNode) bool {
		if node.GetChildren().Len() > 0 {
			result = append(result, db)
		}
		return true
	})
	return result
}

func (q *QuotaCenter) resetLocalQuotaRates() error {
	local := q.localQuota
	// Only collections limited in the previous round need their baseline back;
	// collections that nodes report but no protection limits are left alone.
	maps.Copy(local.dirty, local.previousEffects)
	local.previousEffects = make(map[int64]bool)
	clusterRates := quotaRateValues(internalpb.RateScope_Cluster)
	dbRates := quotaRateValues(internalpb.RateScope_Database)
	collRates := quotaRateValues(internalpb.RateScope_Collection)
	partRates := quotaRateValues(internalpb.RateScope_Partition)
	root := q.rateLimiter.GetRootLimiters()
	resetQuotaLimiter(root, clusterRates, false)
	dbs, err := q.meta.ListDatabases(q.ctx, typeutil.MaxTimestamp)
	if err != nil {
		return err
	}
	liveDBs := make(map[int64]bool)
	for _, db := range dbs {
		liveDBs[db.ID] = true
		node := q.rateLimiter.GetOrCreateDatabaseLimiters(db.ID, newParamLimiterFunc(internalpb.RateScope_Database, allOps))
		resetQuotaLimiter(node, dbRates, false)
	}
	root.GetChildren().Range(func(id int64, node *rlinternal.RateLimiterNode) bool {
		if !liveDBs[id] {
			local.deleted[quotaNodeKey{Scope: internalpb.RateScope_Database, DB: id}] = true
			node.GetChildren().Range(func(collection int64, child *rlinternal.RateLimiterNode) bool {
				local.deleted[quotaNodeKey{Scope: internalpb.RateScope_Collection, DB: id, Collection: collection}] = true
				child.GetChildren().Range(func(part int64, _ *rlinternal.RateLimiterNode) bool {
					local.deleted[quotaNodeKey{Scope: internalpb.RateScope_Partition, DB: id, Collection: collection, Partition: part}] = true
					return true
				})
				return true
			})
		}
		return true
	})
	for key := range local.deleted {
		switch key.Scope {
		case internalpb.RateScope_Collection:
			if db := q.rateLimiter.GetDatabaseLimiters(key.DB); db != nil {
				if node := db.GetChild(key.Collection); node != nil {
					node.GetChildren().Range(func(id int64, _ *rlinternal.RateLimiterNode) bool {
						local.deleted[quotaNodeKey{Scope: internalpb.RateScope_Partition, DB: key.DB, Collection: key.Collection, Partition: id}] = true
						return true
					})
				}
				db.GetChildren().Remove(key.Collection)
			}
		case internalpb.RateScope_Database:
			root.GetChildren().Remove(key.DB)
		}
	}
	partitionEnabled := false
	for _, value := range partRates {
		partitionEnabled = partitionEnabled || value != Inf
	}
	for id := range local.dirty {
		view := local.metadata[id]
		if view == nil || view.Collection == nil {
			delete(local.exceeded, id)
			delete(local.exceededParts, id)
			continue
		}
		coll := view.Collection
		node := q.rateLimiter.GetOrCreateCollectionLimiters(coll.DBID, id, newParamLimiterFunc(internalpb.RateScope_Database, allOps), newParamLimiterFunc(internalpb.RateScope_Collection, allOps))
		for rt := range collRates {
			if rt != internalpb.RateType_DDLFlush {
				value, err := q.getCollectionMaxLimit(rt, id)
				if err != nil {
					value = Limit(quota.GetQuotaValue(internalpb.RateScope_Collection, rt, Params))
				}
				collRates[rt] = value
			}
		}
		resetQuotaLimiter(node, collRates, true)
		parts := make(map[int64]bool)
		if partitionEnabled {
			for _, part := range view.AllPartitions {
				parts[part] = true
				p := q.rateLimiter.GetOrCreatePartitionLimiters(coll.DBID, id, part, newParamLimiterFunc(internalpb.RateScope_Database, allOps), newParamLimiterFunc(internalpb.RateScope_Collection, allOps), newParamLimiterFunc(internalpb.RateScope_Partition, allOps))
				resetQuotaLimiter(p, partRates, true)
			}
		}
		node.GetChildren().Range(func(part int64, _ *rlinternal.RateLimiterNode) bool {
			if !parts[part] {
				node.GetChildren().Remove(part)
				local.deleted[quotaNodeKey{Scope: internalpb.RateScope_Partition, DB: coll.DBID, Collection: id, Partition: part}] = true
			}
			return true
		})
	}
	// Match the reference's special wire flags for databases without collections.
	root.GetChildren().Range(func(_ int64, node *rlinternal.RateLimiterNode) bool {
		if node.GetChildren().Len() == 0 {
			resetQuotaLimiter(node, dbRates, true)
		}
		return true
	})
	return nil
}

// Every collection- or partition-level limiter change must be recorded where it
// is made: the next round resets only recorded collections. A newly limited
// collection is still at its baseline; recording it ensures publication now
// and reset in the next round, including when the limit's cause disappears.
func (q *QuotaCenter) recordLocalQuotaEffect(collection int64) {
	if q.localQuota != nil {
		q.localQuota.dirty[collection] = true
		q.localQuota.previousEffects[collection] = true
	}
}

func (q *QuotaCenter) checkLocalDiskQuota(denied map[int64]struct{}) error {
	q.diskMu.Lock()
	defer q.diskMu.Unlock()
	if !Params.QuotaConfig.DiskProtectionEnabled.GetAsBool() {
		q.localQuota.exceeded = make(map[int64]bool)
		q.localQuota.exceededParts = make(map[int64][]int64)
		return nil
	}
	local := q.localQuota
	previousExceeded, previousParts := len(local.exceeded), len(local.exceededParts)

	for id := range local.dirty {
		delete(local.exceeded, id)
		delete(local.exceededParts, id)
		if bytes, ok := q.dataCoordMetrics.CollectionBinlogSize[id]; ok {
			limit := getRateLimitConfig(q.getCollectionLimitProperties(id), common.CollectionDiskQuotaKey, Params.QuotaConfig.DiskQuotaPerCollection.GetAsFloat())
			if float64(bytes) >= limit {
				local.exceeded[id] = true
			}
		}
		for part, bytes := range q.dataCoordMetrics.PartitionsBinlogSize[id] {
			if float64(bytes) >= Params.QuotaConfig.DiskQuotaPerPartition.GetAsFloat() {
				local.exceededParts[id] = append(local.exceededParts[id], part)
			}
		}
	}
	if len(local.exceeded)*2 < previousExceeded {
		remaining := make(map[int64]bool, len(local.exceeded))
		maps.Copy(remaining, local.exceeded)
		local.exceeded = remaining
	}
	if len(local.exceededParts)*2 < previousParts {
		remaining := make(map[int64][]int64, len(local.exceededParts))
		maps.Copy(remaining, local.exceededParts)
		local.exceededParts = remaining
	}
	total := q.dataCoordMetrics.TotalBinlogSize
	if float64(total) >= Params.QuotaConfig.DiskQuota.GetAsFloat() {
		return q.forceDenyWriting(commonpb.ErrorCode_DiskQuotaExhausted, true, nil, nil, nil, "cluster disk quota exceeded")
	}
	loaded := 0.0
	for _, node := range q.queryNodeMetrics {
		loaded += float64(node.LoadedBinlogSize)
	}
	if loaded >= Params.QuotaConfig.LoadedDiskQuota.GetAsFloat() {
		return q.forceDenyWriting(commonpb.ErrorCode_DiskQuotaExhausted, true, nil, nil, nil, "cluster loaded disk quota exceeded")
	}
	sizes := maps.Clone(local.dbBytes)
	for id := range denied {
		delete(sizes, id)
	}
	collections := make([]int64, 0, len(local.exceeded))
	for id := range local.exceeded {
		collections = append(collections, id)
	}
	if err := q.forceDenyWriting(commonpb.ErrorCode_DiskQuotaExhausted, false, q.checkDBDiskQuota(sizes), collections, local.exceededParts, "disk quota exceeded"); err != nil {
		return err
	}
	q.totalBinlogSize = total
	return nil
}

// No half-round policies are published if config or catalog changed while the
// calculation was reading their values. Segment sampling is already a fixed
// immutable snapshot and may progress normally under continuous ingestion.
func (q *QuotaCenter) finishLocalQuotaCalculation() error {
	if q.localQuota == nil {
		return nil
	}
	if err := q.validateLocalQuotaInputs(); err != nil {
		return err
	}
	local := q.localQuota
	previousMetricScopes := len(local.metricNodes)
	for key := range local.deleted {
		delete(local.metricNodes, key)
	}
	q.visitLocalQuotaNodes(func(key quotaNodeKey, node *rlinternal.RateLimiterNode) {
		metrics.RootCoordQuotaCalculatedScopes.Inc()
		if node.GetQuotaStates().Len() > 0 {
			local.metricNodes[key] = node
		} else {
			delete(local.metricNodes, key)
		}
	})
	if len(local.metricNodes)*2 < previousMetricScopes {
		remaining := make(map[quotaNodeKey]*rlinternal.RateLimiterNode, len(local.metricNodes))
		maps.Copy(remaining, local.metricNodes)
		local.metricNodes = remaining
	}
	return nil
}

func (q *QuotaCenter) validateLocalQuotaInputs() error {
	local := q.localQuota
	if local != nil && (q.meta.(localQuotaMetadataSource).quotaMetadataGeneration() != local.generation ||
		local.calculationProxyCount != q.proxies.GetProxyCount() ||
		!maps.Equal(local.config, Params.GetComponentConfigurations("rootcoord", "quota"))) {
		local.config = nil
		return merr.WrapErrServiceNotReadyMsg("quota inputs changed during calculation")
	}
	return nil
}

func (q *QuotaCenter) visitLocalQuotaNodes(visit func(quotaNodeKey, *rlinternal.RateLimiterNode)) {
	root := q.rateLimiter.GetRootLimiters()
	visit(quotaNodeKey{Scope: internalpb.RateScope_Cluster}, root)
	root.GetChildren().Range(func(db int64, node *rlinternal.RateLimiterNode) bool {
		visit(quotaNodeKey{Scope: internalpb.RateScope_Database, DB: db}, node)
		return true
	})
	for id := range q.localQuota.dirty {
		view := q.localQuota.metadata[id]
		if view == nil || view.Collection == nil {
			continue
		}
		db := view.Collection.DBID
		node := q.rateLimiter.GetCollectionLimiters(db, id)
		if node == nil {
			continue
		}
		visit(quotaNodeKey{Scope: internalpb.RateScope_Collection, DB: db, Collection: id}, node)
		node.GetChildren().Range(func(part int64, p *rlinternal.RateLimiterNode) bool {
			visit(quotaNodeKey{Scope: internalpb.RateScope_Partition, DB: db, Collection: id, Partition: part}, p)
			return true
		})
	}
}

func (q *QuotaCenter) recordLocalQuotaMetrics() {
	metrics.RootCoordQuotaStates.Reset()
	dbNames := make(map[int64]string)
	q.dbs.Range(func(name string, id int64) bool { dbNames[id] = name; return true })
	for key, node := range q.localQuota.metricNodes {
		node.GetQuotaStates().Range(func(_ milvuspb.QuotaState, state *rlinternal.QuotaStateInfo) bool {
			code := state.ErrorCode
			if code != commonpb.ErrorCode_MemoryQuotaExhausted && code != commonpb.ErrorCode_DiskQuotaExhausted && code != commonpb.ErrorCode_TimeTickLongDelay {
				return true
			}
			name := "cluster"
			switch key.Scope {
			case internalpb.RateScope_Database:
				name = "db_" + dbNames[key.DB]
			case internalpb.RateScope_Collection:
				name = "collection_"
				if view := q.localQuota.metadata[key.Collection]; view != nil && view.Collection != nil {
					name += view.Collection.Name
				}
			case internalpb.RateScope_Partition:
				return false
			}
			metrics.RootCoordQuotaStates.WithLabelValues(code.String(), name).Set(1)
			metrics.RootCoordForceDenyWritingCounter.Inc()
			return false
		})
	}
}

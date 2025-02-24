/*
 * Licensed to the LF AI & Data foundation under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quota

import (
	"math"
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/config"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

var (
	initOnce       sync.Once
	limitConfigMap map[internalpb.RateScope]map[internalpb.RateType]*paramtable.ParamItem
)

func initLimitConfigMaps() {
	initOnce.Do(func() {
		quotaConfig := &paramtable.Get().QuotaConfig
		limitConfigMap = map[internalpb.RateScope]map[internalpb.RateType]*paramtable.ParamItem{
			internalpb.RateScope_Cluster: {
				internalpb.RateType_DDLCollection: &quotaConfig.DDLCollectionRate,
				internalpb.RateType_DDLPartition:  &quotaConfig.DDLPartitionRate,
				internalpb.RateType_DDLIndex:      &quotaConfig.MaxIndexRate,
				internalpb.RateType_DDLFlush:      &quotaConfig.MaxFlushRate,
				internalpb.RateType_DDLCompaction: &quotaConfig.MaxCompactionRate,
				internalpb.RateType_DMLInsert:     &quotaConfig.DMLMaxInsertRate,
				internalpb.RateType_DMLUpsert:     &quotaConfig.DMLMaxUpsertRate,
				internalpb.RateType_DMLDelete:     &quotaConfig.DMLMaxDeleteRate,
				internalpb.RateType_DMLBulkLoad:   &quotaConfig.DMLMaxBulkLoadRate,
				internalpb.RateType_DQLSearch:     &quotaConfig.DQLMaxSearchRate,
				internalpb.RateType_DQLQuery:      &quotaConfig.DQLMaxQueryRate,
			},
			internalpb.RateScope_Database: {
				internalpb.RateType_DDLCollection: &quotaConfig.DDLCollectionRatePerDB,
				internalpb.RateType_DDLPartition:  &quotaConfig.DDLPartitionRatePerDB,
				internalpb.RateType_DDLIndex:      &quotaConfig.MaxIndexRatePerDB,
				internalpb.RateType_DDLFlush:      &quotaConfig.MaxFlushRatePerDB,
				internalpb.RateType_DDLCompaction: &quotaConfig.MaxCompactionRatePerDB,
				internalpb.RateType_DMLInsert:     &quotaConfig.DMLMaxInsertRatePerDB,
				internalpb.RateType_DMLUpsert:     &quotaConfig.DMLMaxUpsertRatePerDB,
				internalpb.RateType_DMLDelete:     &quotaConfig.DMLMaxDeleteRatePerDB,
				internalpb.RateType_DMLBulkLoad:   &quotaConfig.DMLMaxBulkLoadRatePerDB,
				internalpb.RateType_DQLSearch:     &quotaConfig.DQLMaxSearchRatePerDB,
				internalpb.RateType_DQLQuery:      &quotaConfig.DQLMaxQueryRatePerDB,
			},
			internalpb.RateScope_Collection: {
				internalpb.RateType_DMLInsert:   &quotaConfig.DMLMaxInsertRatePerCollection,
				internalpb.RateType_DMLUpsert:   &quotaConfig.DMLMaxUpsertRatePerCollection,
				internalpb.RateType_DMLDelete:   &quotaConfig.DMLMaxDeleteRatePerCollection,
				internalpb.RateType_DMLBulkLoad: &quotaConfig.DMLMaxBulkLoadRatePerCollection,
				internalpb.RateType_DQLSearch:   &quotaConfig.DQLMaxSearchRatePerCollection,
				internalpb.RateType_DQLQuery:    &quotaConfig.DQLMaxQueryRatePerCollection,
				internalpb.RateType_DDLFlush:    &quotaConfig.MaxFlushRatePerCollection,
			},
			internalpb.RateScope_Partition: {
				internalpb.RateType_DMLInsert:   &quotaConfig.DMLMaxInsertRatePerPartition,
				internalpb.RateType_DMLUpsert:   &quotaConfig.DMLMaxUpsertRatePerPartition,
				internalpb.RateType_DMLDelete:   &quotaConfig.DMLMaxDeleteRatePerPartition,
				internalpb.RateType_DMLBulkLoad: &quotaConfig.DMLMaxBulkLoadRatePerPartition,
				internalpb.RateType_DQLSearch:   &quotaConfig.DQLMaxSearchRatePerPartition,
				internalpb.RateType_DQLQuery:    &quotaConfig.DQLMaxQueryRatePerPartition,
			},
		}

		pt := paramtable.Get()
		pt.Watch(quotaConfig.DMLMaxInsertRate.Key, config.NewHandler(quotaConfig.DMLMaxInsertRate.Key, func(event *config.Event) {
			metrics.MaxInsertRate.WithLabelValues(paramtable.GetStringNodeID(), "cluster").Set(quotaConfig.DMLMaxInsertRate.GetAsFloat())
		}))
		pt.Watch(quotaConfig.DMLMaxInsertRatePerDB.Key, config.NewHandler(quotaConfig.DMLMaxInsertRatePerDB.Key, func(event *config.Event) {
			metrics.MaxInsertRate.WithLabelValues(paramtable.GetStringNodeID(), "db").Set(quotaConfig.DMLMaxInsertRatePerDB.GetAsFloat())
		}))
		pt.Watch(quotaConfig.DMLMaxInsertRatePerCollection.Key, config.NewHandler(quotaConfig.DMLMaxInsertRatePerCollection.Key, func(event *config.Event) {
			metrics.MaxInsertRate.WithLabelValues(paramtable.GetStringNodeID(), "collection").Set(quotaConfig.DMLMaxInsertRatePerCollection.GetAsFloat())
		}))
		pt.Watch(quotaConfig.DMLMaxInsertRatePerPartition.Key, config.NewHandler(quotaConfig.DMLMaxInsertRatePerPartition.Key, func(event *config.Event) {
			metrics.MaxInsertRate.WithLabelValues(paramtable.GetStringNodeID(), "partition").Set(quotaConfig.DMLMaxInsertRatePerPartition.GetAsFloat())
		}))
	})
}

func GetQuotaConfigMap(scope internalpb.RateScope) map[internalpb.RateType]*paramtable.ParamItem {
	initLimitConfigMaps()
	configMap, ok := limitConfigMap[scope]
	if !ok {
		log.Warn("Unknown rate scope", zap.Any("scope", scope))
		return make(map[internalpb.RateType]*paramtable.ParamItem)
	}
	return configMap
}

func GetQuotaValue(scope internalpb.RateScope, rateType internalpb.RateType, params *paramtable.ComponentParam) float64 {
	configMap := GetQuotaConfigMap(scope)
	config, ok := configMap[rateType]
	if !ok {
		log.Warn("Unknown rate type", zap.Any("rateType", rateType))
		return math.MaxFloat64
	}
	return config.GetAsFloat()
}

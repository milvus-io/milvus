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

package meta

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
)

type quotaDisplayKey struct {
	family                      int
	db, collection, name, state string
}
type quotaDisplayValue struct {
	value        int64
	contributors int
}
type quotaDisplayCollection struct {
	db, name string
	stats    *metricsinfo.CollectionQuotaSnapshot
}
type quotaDisplayCache struct {
	revision    uint64
	aggregate   bool
	collections map[int64]quotaDisplayCollection
	series      map[quotaDisplayKey]quotaDisplayValue
}

// recordQuotaChanges updates the display metrics from snapshot. A full
// snapshot rebuilds them; partial changes update only their contributions.
func (m *Meta) recordQuotaChanges(snapshot *metricsinfo.QuotaSnapshot) {
	m.quotaMetricsMu.Lock()
	defer m.quotaMetricsMu.Unlock()
	if !Params.QuotaConfig.CollectionStatsCacheEnabled.GetAsBool() {
		return
	}
	aggregate := metrics.IsCollectionLevelMetricsAggregateMode()
	if m.quotaDisplay != nil && snapshot.Revision < m.quotaDisplay.revision {
		return
	}
	previous := m.quotaDisplay
	if previous == nil || previous.aggregate != aggregate || snapshot.Full {
		if !snapshot.Full {
			snapshot = m.Segments.ReadQuotaSnapshot(true)
		}
		metrics.DataCoordStoredBinlogSize.Reset()
		metrics.DataCoordNumStoredRows.Reset()
		metrics.DataCoordSegmentBinLogFileCount.Reset()
		metrics.DataCoordL0DeleteEntriesNum.Reset()
		if aggregate {
			metrics.DataCoordL0DeltalogFileNum.Reset()
		}
		m.quotaDisplay = &quotaDisplayCache{aggregate: aggregate, collections: make(map[int64]quotaDisplayCollection), series: make(map[quotaDisplayKey]quotaDisplayValue)}
		if aggregate {
			metrics.DataCoordSegmentBinLogFileCount.WithLabelValues(metrics.AllLabel).Set(0)
		}
	}
	if !aggregate {
		deltalogPublishMu.Lock()
		defer deltalogPublishMu.Unlock()
	}
	cache := m.quotaDisplay
	cache.revision = snapshot.Revision
	changed := make(map[quotaDisplayKey]bool)
	apply := func(id int64, c quotaDisplayCollection, sign int) {
		collection, name := strconv.FormatInt(id, 10), c.name
		if aggregate {
			collection, name = metrics.AllLabel, metrics.AllLabel
		}
		add := func(k quotaDisplayKey, value int64) {
			v := cache.series[k]
			v.value += int64(sign) * value
			v.contributors += sign
			cache.series[k] = v
			changed[k] = true
		}
		for _, state := range c.stats.Display {
			add(quotaDisplayKey{family: 0, db: c.db, collection: collection, state: state.State.String()}, state.Bytes)
			add(quotaDisplayKey{family: 1, db: c.db, collection: collection, name: name, state: state.State.String()}, state.Rows)
			add(quotaDisplayKey{family: 2, collection: collection}, state.Files)
		}
		if c.stats.HasL0 {
			add(quotaDisplayKey{family: 3, db: c.db, collection: collection}, c.stats.L0Rows)
		}
		if c.stats.HasL0 && aggregate {
			add(quotaDisplayKey{family: 4, db: c.db, collection: collection}, c.stats.L0DeltalogFiles)
		}
	}
	for id, value := range snapshot.Collections {
		old, hadOld := cache.collections[id]
		if hadOld {
			apply(id, old, -1)
			delete(cache.collections, id)
		}
		coll, ok := m.MetaStore.GetCollection(id)
		present := ok && value.HasQuota
		if !aggregate {
			collection := strconv.FormatInt(id, 10)
			if previous != nil && previous != cache {
				old, hadOld = previous.collections[id]
			}
			if !present || (hadOld && coll.DatabaseName != old.db) {
				removeCollectionDeltalogMetrics(collection)
			}
			if present {
				publishCollectionDeltalogMetrics(coll.DatabaseName, collection, value.L0DeltalogFiles, value.DeltalogQuantiles)
			}
		}
		if present {
			current := quotaDisplayCollection{db: coll.DatabaseName, name: coll.Schema.GetName(), stats: value}
			cache.collections[id] = current
			apply(id, current, 1)
		}
	}
	if !aggregate && snapshot.Full {
		for collection := range publishedDeltalogLevels {
			id, err := strconv.ParseInt(collection, 10, 64)
			if _, present := cache.collections[id]; err != nil || !present {
				removeCollectionDeltalogMetrics(collection)
			}
		}
	}
	for key := range changed {
		value := cache.series[key]
		if value.contributors == 0 {
			delete(cache.series, key)
			// Cardinality wrappers deliberately ignore collection-scoped deletes
			// in aggregate mode. Each remaining series is owned here, so delete
			// by its non-collection labels after its last contributor leaves.
			if aggregate {
				labels := prometheus.Labels{"db_name": key.db}
				switch key.family {
				case 0:
					labels["segment_state"] = key.state
					metrics.DataCoordStoredBinlogSize.DeletePartialMatch(labels)
				case 1:
					labels["segment_state"] = key.state
					metrics.DataCoordNumStoredRows.DeletePartialMatch(labels)
				case 2:
					metrics.DataCoordSegmentBinLogFileCount.WithLabelValues(metrics.AllLabel).Set(0)
				case 3:
					metrics.DataCoordL0DeleteEntriesNum.DeletePartialMatch(labels)
				case 4:
					metrics.DataCoordL0DeltalogFileNum.DeletePartialMatch(labels)
				}
				continue
			}
			switch key.family {
			case 0:
				metrics.DataCoordStoredBinlogSize.DeleteLabelValues(key.db, key.collection, key.state)
			case 1:
				metrics.DataCoordNumStoredRows.DeleteLabelValues(key.db, key.collection, key.name, key.state)
			case 2:
				metrics.DataCoordSegmentBinLogFileCount.DeleteLabelValues(key.collection)
			case 3:
				metrics.DataCoordL0DeleteEntriesNum.DeleteLabelValues(key.db, key.collection)
			}
			continue
		}
		switch key.family {
		case 0:
			metrics.DataCoordStoredBinlogSize.WithLabelValues(key.db, key.collection, key.state).Set(float64(value.value))
		case 1:
			metrics.DataCoordNumStoredRows.WithLabelValues(key.db, key.collection, key.name, key.state).Set(float64(value.value))
		case 2:
			metrics.DataCoordSegmentBinLogFileCount.WithLabelValues(key.collection).Set(float64(value.value))
		case 3:
			metrics.DataCoordL0DeleteEntriesNum.WithLabelValues(key.db, key.collection).Set(float64(value.value))
		case 4:
			metrics.DataCoordL0DeltalogFileNum.WithLabelValues(key.db, key.collection).Set(float64(value.value))
		}
	}
}

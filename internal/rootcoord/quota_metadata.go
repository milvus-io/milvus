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

	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type quotaCollectionMetadata struct {
	Generation    uint64
	Collection    *model.Collection
	AllPartitions []int64
}
type quotaMetadataSnapshot struct {
	Full        bool
	Generation  uint64
	Collections map[int64]*quotaCollectionMetadata
}

// resetQuotaMetadataLocked makes the next projection read a full one, after a
// bulk load of the metadata.
func (mt *MetaTable) resetQuotaMetadataLocked() {
	mt.quotaGeneration++
	mt.quotaDirty = nil
	mt.quotaReset = true
}

// Called with ddLock after the source metadata was successfully published.
func (mt *MetaTable) invalidateQuotaMetadataLocked(id int64) {
	mt.quotaGeneration++
	if !Params.QuotaConfig.LocalQuotaCalculationEnabled.GetAsBool() || !Params.QuotaConfig.CollectionStatsCacheEnabled.GetAsBool() {
		mt.quotaDirty = nil
		mt.quotaReset = true
		return
	}
	if mt.quotaDirty == nil {
		mt.quotaDirty = make(map[int64]uint64)
	}
	mt.quotaDirty[id] = mt.quotaGeneration
}

func (mt *MetaTable) readQuotaMetadata(full bool) *quotaMetadataSnapshot {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()
	full = full || mt.quotaReset
	mt.quotaReset = false
	result := &quotaMetadataSnapshot{Full: full, Generation: mt.quotaGeneration, Collections: make(map[int64]*quotaCollectionMetadata)}
	add := func(id int64) {
		result.Collections[id] = &quotaCollectionMetadata{Generation: mt.quotaDirty[id]}
		coll := mt.collID2Meta[id]
		if coll == nil || !coll.Available() {
			return
		}
		projected := &model.Collection{CollectionID: id, DBID: normalizeCollectionDBID(coll.DBID), DBName: coll.DBName, Name: coll.Name, Properties: common.CloneKeyValuePairs(coll.Properties), State: coll.State}
		entry := result.Collections[id]
		entry.Collection = projected
		for _, part := range coll.Partitions {
			entry.AllPartitions = append(entry.AllPartitions, part.PartitionID)
			if part.Available() {
				projected.Partitions = append(projected.Partitions, &model.Partition{PartitionID: part.PartitionID})
			}
		}
	}
	if full {
		for id := range mt.collID2Meta {
			add(id)
		}
	} else {
		for id := range mt.quotaDirty {
			add(id)
		}
	}
	return result
}

func (mt *MetaTable) acknowledgeQuotaMetadata(snapshot *quotaMetadataSnapshot) {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()
	previousSize := len(mt.quotaDirty)
	for id, value := range snapshot.Collections {
		if mt.quotaDirty[id] == value.Generation {
			delete(mt.quotaDirty, id)
		}
	}
	if len(mt.quotaDirty)*2 < previousSize {
		dirty := make(map[int64]uint64, len(mt.quotaDirty))
		maps.Copy(dirty, mt.quotaDirty)
		mt.quotaDirty = dirty
	}
}

func (mt *MetaTable) quotaMetadataGeneration() uint64 {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()
	return mt.quotaGeneration
}

func (q *QuotaCenter) quotaCollection(ctx context.Context, id int64) (*model.Collection, error) {
	if q.localQuota != nil {
		if view := q.localQuota.metadata[id]; view != nil && view.Collection != nil {
			return view.Collection, nil
		}
		return nil, merr.WrapErrCollectionNotFound(id)
	}
	return q.meta.GetCollectionByIDWithMaxTs(ctx, id)
}

// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package observers

import (
	"context"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const collectionRowsRefreshInterval = 30 * time.Second

type cachedCollectionRows struct {
	count   meta.CollectionRowCount
	retryAt time.Time
}

// Init validates startup configuration before the coordinator starts serving.
func (ob *ReplicaObserver) Init() error {
	ob.recoveryMu.Lock()
	defer ob.recoveryMu.Unlock()
	raw := paramtable.Get().QueryCoordCfg.CollectionGroups.GetValue()
	groups, err := parseCollectionGroups(raw)
	if err != nil {
		return err
	}
	ob.configRaw, ob.collectionGroups = raw, groups
	return nil
}

func (ob *ReplicaObserver) refreshCollectionGroups() error {
	raw := paramtable.Get().QueryCoordCfg.CollectionGroups.GetValue()
	if raw == ob.configRaw {
		return nil
	}
	groups, err := parseCollectionGroups(raw)
	// Remember invalid values too, to log them once rather than every cycle.
	// The active grouping remains the last successfully parsed configuration.
	ob.configRaw = raw
	if err != nil {
		return err
	}
	ob.collectionGroups = groups
	return nil
}

func parseCollectionGroups(raw string) (map[int64]string, error) {
	var groups []struct {
		ID            string   `json:"id"`
		CollectionIDs []string `json:"collectionIds"`
	}
	if err := json.Unmarshal([]byte(raw), &groups); err != nil {
		return nil, merr.WrapErrParameterInvalidMsg("invalid queryCoord.collectionGroups JSON")
	}
	bindings := make(map[int64]string)
	names := typeutil.NewSet[string]()
	for _, group := range groups {
		if strings.TrimSpace(group.ID) == "" || names.Contain(group.ID) {
			return nil, merr.WrapErrParameterInvalidMsg("collection group ID must be nonempty and unique: %q", group.ID)
		}
		names.Insert(group.ID)
		for _, value := range group.CollectionIDs {
			id, err := strconv.ParseInt(value, 10, 64)
			if err != nil || id <= 0 {
				return nil, merr.WrapErrParameterInvalidMsg("invalid collection ID in group %q: %q", group.ID, value)
			}
			if _, exists := bindings[id]; exists {
				return nil, merr.WrapErrParameterInvalidMsg("collection %d belongs to multiple group entries", id)
			}
			bindings[id] = group.ID
		}
	}
	return bindings, nil
}

// collectionBatches treats each ungrouped collection as an independent batch.
// A configured group is visited once, regardless of its number of members.
func (ob *ReplicaObserver) collectionBatches(collections []int64) [][]int64 {
	batches := make([][]int64, 0)
	byGroup := make(map[string]int)
	for _, id := range collections {
		group := ob.collectionGroups[id]
		if group == "" {
			batches = append(batches, []int64{id})
			continue
		}
		index, exists := byGroup[group]
		if !exists {
			index = len(batches)
			byGroup[group] = index
			batches = append(batches, nil)
		}
		batches[index] = append(batches[index], id)
	}
	return batches
}

// recoverNodes runs only under recoveryMu. Configuration and statistics are
// observer-owned; each call into ReplicaManager receives a complete batch.
func (ob *ReplicaObserver) recoverNodes(ctx context.Context) {
	if err := ob.refreshCollectionGroups(); err != nil {
		mlog.Warn(ctx, "ignoring invalid collection group configuration", mlog.Err(err))
	}
	collections := ob.meta.GetReplicaCollections()
	active := typeutil.NewUniqueSet(collections...)
	for id := range ob.rowStats {
		if !active.Contain(id) {
			delete(ob.rowStats, id)
		}
	}
	for _, batch := range ob.collectionBatches(collections) {
		rows := make(map[int64]meta.CollectionRowCount)
		if len(batch) > 1 {
			for _, id := range batch {
				rows[id] = ob.getCollectionRows(ctx, id)
			}
		}
		if ctx.Err() != nil {
			return
		}
		// A config notification may arrive while DataCoord is being queried.
		// Retry with new batches instead of committing this obsolete batch.
		if paramtable.Get().QueryCoordCfg.CollectionGroups.GetValue() != ob.configRaw {
			ob.meta.RequestReplicaRecovery()
			return
		}
		names := typeutil.NewSet[string]()
		for _, id := range batch {
			names.Insert(ob.meta.GetResourceGroupByCollection(ctx, id).Collect()...)
		}
		rgNames := names.Collect()
		sort.Strings(rgNames)
		rgs, err := ob.meta.GetResourceGroups(ctx, rgNames)
		if err == nil {
			err = ob.meta.RecoverNodesInCollections(ctx, batch, rgs, rows)
		}
		if err != nil {
			mlog.Warn(ctx, "failed to recover replica batch", mlog.Int64s("collections", batch), mlog.Err(err))
		}
	}
}

func (ob *ReplicaObserver) getCollectionRows(ctx context.Context, id int64) meta.CollectionRowCount {
	cached := ob.rowStats[id]
	if time.Now().Before(cached.retryAt) {
		return cached.count
	}
	var err error
	if ob.broker == nil {
		err = merr.WrapErrServiceUnavailableMsg("collection row source is not initialized")
	} else {
		_, segments, fetchErr := ob.broker.GetRecoveryInfoV2(ctx, id)
		err = fetchErr
		if err == nil {
			var rows int64
			seen := typeutil.NewUniqueSet()
			for _, segment := range segments {
				if seen.Contain(segment.GetID()) {
					continue
				}
				seen.Insert(segment.GetID())
				count := segment.GetNumOfRows()
				if count < 0 || count > math.MaxInt64-rows {
					err = merr.WrapErrServiceInternalMsg("invalid recovery row count for collection %d", id)
					break
				}
				rows += count
			}
			if err == nil {
				cached.count = meta.CollectionRowCount{Rows: rows, Valid: true, Fresh: true}
			}
		}
	}
	if err != nil {
		cached.count.Fresh = false
		mlog.Warn(ctx, "failed to refresh collection rows", mlog.FieldCollectionID(id), mlog.Err(err))
	}
	cached.retryAt = time.Now().Add(collectionRowsRefreshInterval)
	ob.rowStats[id] = cached
	return cached.count
}

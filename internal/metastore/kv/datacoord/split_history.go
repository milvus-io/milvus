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

package datacoord

import (
	"context"
	"strconv"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// The task and immutable target identity are part of the key. These local
// physical certificates deliberately do not travel in the replicated message.
func splitHistoryKey(task *datapb.SplitShardTask, target *datapb.SplitShardTaskTarget) string {
	return "datacoord-meta/split-history/" + strconv.FormatInt(task.GetCollectionId(), 10) + "/" + strconv.FormatInt(task.GetTaskId(), 10) + "/" + target.GetVchannel() + "/" + strconv.FormatUint(target.GetGenesisTimeTick(), 10) + "/" + strconv.FormatUint(task.GetSwitchTimeTick(), 10)
}

func (kc *Catalog) LoadSplitHistoryBoundary(ctx context.Context, task *datapb.SplitShardTask, target *datapb.SplitShardTaskTarget) (uint64, error) {
	value, err := kc.MetaKv.Load(ctx, splitHistoryKey(task, target))
	if errors.Is(err, merr.ErrIoKeyNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	through, err := strconv.ParseUint(value, 10, 64)
	if err != nil || through <= max(task.GetSwitchTimeTick(), target.GetGenesisTimeTick()) {
		return 0, merr.WrapErrDataIntegrityMsg("invalid split history boundary for task %d target %s", task.GetTaskId(), target.GetVchannel())
	}
	return through, nil
}

// The collection guard serializes local creation. The storage predicate also
// excludes a previous coordinator's delayed write. Never replace an established F.
func (kc *Catalog) SaveSplitHistoryBoundary(ctx context.Context, task *datapb.SplitShardTask, target *datapb.SplitShardTaskTarget, through uint64) error {
	old, err := kc.LoadSplitHistoryBoundary(ctx, task, target)
	if err != nil {
		return err
	}
	if old != 0 {
		if old != through {
			return merr.WrapErrDataIntegrityMsg("split history boundary is immutable for task %d target %s", task.GetTaskId(), target.GetVchannel())
		}
		return nil
	}
	if through <= max(task.GetSwitchTimeTick(), target.GetGenesisTimeTick()) {
		return merr.WrapErrDataIntegrityMsg("split history boundary precedes target genesis")
	}
	key := splitHistoryKey(task, target)
	return kc.MetaKv.MultiSaveAndRemove(ctx, map[string]string{key: strconv.FormatUint(through, 10)}, nil, predicates.KeyNotExist(key))
}

// DropSplitHistoryBoundaries is called only when the split task itself is
// discarded (including collection drop), never merely when its pin is released.
func (kc *Catalog) DropSplitHistoryBoundaries(ctx context.Context, task *datapb.SplitShardTask) error {
	prefix := "datacoord-meta/split-history/" + strconv.FormatInt(task.GetCollectionId(), 10) + "/" + strconv.FormatInt(task.GetTaskId(), 10) + "/"
	return kc.MetaKv.RemoveWithPrefix(ctx, prefix)
}

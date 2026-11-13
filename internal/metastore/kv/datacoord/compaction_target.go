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
	"math"
	"strconv"

	"github.com/cockroachdb/errors"
	tikverr "github.com/tikv/client-go/v2/error"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore"
	rootcatalog "github.com/milvus-io/milvus/internal/metastore/kv/rootcoord"
	streamingcatalog "github.com/milvus-io/milvus/internal/metastore/kv/streamingcoord"
	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/replicateutil"
)

func (kc *Catalog) FenceCompactionTargetCreates(ctx context.Context) (uint64, error) {
	epoch, guard, err := kc.loadTargetCreateGeneration(ctx, compactionTargetCreateEpoch)
	if err != nil {
		return 0, err
	}
	if epoch == math.MaxUint64 {
		return 0, merr.WrapErrDataIntegrityMsg("compaction target Create epoch exhausted")
	}
	epoch++
	err = kc.MetaKv.MultiSaveAndRemove(ctx, map[string]string{
		compactionTargetCreateEpoch: strconv.FormatUint(epoch, 10),
	}, nil, guard)
	if err != nil {
		return 0, merr.Wrap(err, "advance compaction target Create epoch")
	}
	return epoch, nil
}

func (kc *Catalog) loadTargetCreateGeneration(ctx context.Context, key string) (uint64, predicates.Predicate, error) {
	raw, err := kc.MetaKv.Load(ctx, key)
	if errors.Is(err, merr.ErrIoKeyNotFound) {
		return 0, predicates.KeyNotExist(key), nil
	}
	if err != nil {
		return 0, nil, err
	}
	generation, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || generation == 0 {
		return 0, nil, merr.WrapErrDataIntegrityMsg("invalid compaction target Create generation at %s", key)
	}
	return generation, predicates.ValueEqual(key, raw), nil
}

func (kc *Catalog) ListCompactionTargets(ctx context.Context) ([]*datapb.CompactionTarget, error) {
	records := make([]*datapb.CompactionTarget, 0)

	applyFn := func(key []byte, value []byte) error {
		record := &datapb.CompactionTarget{}
		if err := proto.Unmarshal(value, record); err != nil {
			return merr.WrapErrDataIntegrity(err, "unmarshal compaction target at %s", key)
		}
		path := buildCompactionTargetPath(record.GetTargetID())
		if record.GetTargetID() <= 0 || (string(key) != path && string(key) != kc.MetaKv.GetPath(path)) {
			return merr.WrapErrDataIntegrityMsg("compaction target identity does not match key %s", key)
		}
		records = append(records, record)
		return nil
	}

	err := kc.MetaKv.WalkWithPrefix(ctx, CompactionTargetPrefix+"/", kc.paginationSize, applyFn)
	if err != nil {
		return nil, err
	}
	return records, nil
}

func (kc *Catalog) LoadCompactionTarget(ctx context.Context, targetID int64) (*datapb.CompactionTarget, error) {
	record, _, err := kc.loadCompactionTarget(ctx, targetID)
	return record, err
}

func (kc *Catalog) loadCompactionTarget(ctx context.Context, targetID int64) (*datapb.CompactionTarget, string, error) {
	key := buildCompactionTargetPath(targetID)
	value, err := kc.MetaKv.Load(ctx, key)
	if errors.Is(err, merr.ErrIoKeyNotFound) {
		return nil, "", nil
	}
	if err != nil {
		return nil, "", err
	}
	record := &datapb.CompactionTarget{}
	if err := proto.Unmarshal([]byte(value), record); err != nil {
		return nil, "", merr.WrapErrDataIntegrity(err, "unmarshal compaction target %d", targetID)
	}
	if record.GetTargetID() != targetID {
		return nil, "", merr.WrapErrDataIntegrityMsg("compaction target %d identity does not match its key", targetID)
	}
	return record, value, nil
}

func (kc *Catalog) CreateCompactionTarget(ctx context.Context, record *datapb.CompactionTarget, collection *metastore.CompactionTargetCollectionVersion) (bool, error) {
	key, value, err := buildCompactionTargetKV(record)
	if err != nil {
		return false, err
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	guards := []predicates.Predicate{predicates.KeyNotExist(key)}
	saves := map[string]string{key: value}
	if collectionID := record.GetCollectionID(); collectionID != 0 {
		if collection == nil {
			return false, merr.WrapErrServiceInternalMsg("compaction target creation is missing the collection version")
		}
		scopeGuards, err := kc.targetCreateCollectionGuards(ctx, collectionID, *collection)
		if err != nil {
			return false, err
		}
		if len(scopeGuards) == 0 {
			return false, merr.WrapErrServiceUnavailableMsg("collection changed during compaction target creation")
		}
		epoch, epochGuard, err := kc.loadTargetCreateGeneration(ctx, compactionTargetCreateEpoch)
		if err != nil {
			return false, merr.Wrap(err, "load compaction target Create epoch")
		}
		if epoch != collection.Epoch {
			return false, merr.WrapErrServiceUnavailableMsg("compaction target Create epoch changed")
		}
		replicationGuard, err := kc.targetCreateReplicationGuard(ctx)
		if err != nil {
			return false, err
		}
		fence := targetCreateFenceKey(collectionID)
		generation, fenceGuard, err := kc.loadTargetCreateGeneration(ctx, fence)
		if err != nil {
			return false, merr.Wrap(err, "load compaction target Create fence")
		}
		if record.GetIntent() == datapb.TargetIntent_INTENT_OPTIMIZE {
			if generation == math.MaxUint64 {
				return false, merr.WrapErrDataIntegrityMsg("compaction target Create fence exhausted for collection %d", collectionID)
			}
			// The slot can become empty again. Advance the fence with the
			// winning OPTIMIZE so an older Create cannot use that empty slot.
			saves[fence] = strconv.FormatUint(generation+1, 10)
		}
		guards = append(guards, scopeGuards...)
		guards = append(guards, epochGuard, replicationGuard, fenceGuard)
	}
	if record.GetIntent() != datapb.TargetIntent_INTENT_OPTIMIZE {
		err = kc.MetaKv.MultiSaveAndRemove(ctx, saves, nil, guards...)
		return targetCreateMayHaveCommitted(err), err
	}

	// The active slot fences two different candidate IDs, including a late
	// transaction from a previous coordinator. Its value is the Target ID.
	slot := activeOptimizeTargetKey(record.GetCollectionID())
	saves[slot] = strconv.FormatInt(record.GetTargetID(), 10)
	err = kc.MetaKv.MultiSaveAndRemove(ctx, saves, nil, append(guards, predicates.KeyNotExist(slot))...)
	unknown := targetCreateMayHaveCommitted(err)
	if err != nil && !unknown {
		owner, readErr := kc.MetaKv.Load(ctx, slot)
		if readErr == nil && owner != saves[slot] {
			id, parseErr := strconv.ParseInt(owner, 10, 64)
			if parseErr != nil || id <= 0 {
				return false, merr.WrapErrDataIntegrityMsg("invalid active OPTIMIZE slot for collection %d", record.GetCollectionID())
			}
			return false, merr.Wrapf(merr.ErrCompactionPlanConflict, "collection %d already has active OPTIMIZE target %d; this Create was not admitted", record.GetCollectionID(), id)
		}
		if readErr == nil && owner == saves[slot] {
			// A retry of the same candidate can discover its earlier commit.
			unknown = true
		}
	}
	return unknown, err
}

// ResolveCompactionTargetCreate races an unacknowledged Create against a
// fence for the collection. The fence can win only while the candidate is
// absent. CollectionWrites serializes these resolutions with new Creates.
func (kc *Catalog) ResolveCompactionTargetCreate(ctx context.Context, candidate *datapb.CompactionTarget, collection metastore.CompactionTargetCollectionVersion) (*datapb.CompactionTarget, error) {
	id := candidate.GetTargetID()
	stored, err := kc.LoadCompactionTarget(ctx, id)
	if err != nil || stored != nil {
		return stored, err
	}
	epoch, epochGuard, err := kc.loadTargetCreateGeneration(ctx, compactionTargetCreateEpoch)
	if err != nil {
		return nil, merr.Wrap(err, "load compaction target Create epoch")
	}
	if epoch != collection.Epoch {
		// Any old Create that committed did so before this epoch was read.
		return kc.LoadCompactionTarget(ctx, id)
	}
	scopeGuards, err := kc.targetCreateCollectionGuards(ctx, candidate.GetCollectionID(), collection)
	if err != nil {
		return nil, err
	}
	if len(scopeGuards) == 0 {
		// A changed collection revision excludes the old transaction.
		// Read again to preserve a Create that committed before the change.
		return kc.LoadCompactionTarget(ctx, id)
	}
	fence := targetCreateFenceKey(candidate.GetCollectionID())
	generation, fenceGuard, err := kc.loadTargetCreateGeneration(ctx, fence)
	if err != nil {
		return nil, merr.Wrap(err, "load compaction target Create fence")
	}
	if generation == math.MaxUint64 {
		return nil, merr.WrapErrDataIntegrityMsg("compaction target Create fence exhausted for collection %d", candidate.GetCollectionID())
	}
	guards := append(scopeGuards, predicates.KeyNotExist(buildCompactionTargetPath(id)), epochGuard, fenceGuard)
	err = kc.MetaKv.MultiSaveAndRemove(ctx, map[string]string{fence: strconv.FormatUint(generation+1, 10)}, nil, guards...)
	if err != nil {
		stored, readErr := kc.LoadCompactionTarget(ctx, id)
		if readErr == nil && stored != nil {
			return stored, nil
		}
		return nil, merr.Wrapf(err, "fence compaction target Create %d", id)
	}
	return nil, nil
}

// targetCreateCollectionGuards returns no guards when the candidate's
// collection revision is no longer live.
func (kc *Catalog) targetCreateCollectionGuards(ctx context.Context, id int64, collection metastore.CompactionTargetCollectionVersion) ([]predicates.Predicate, error) {
	key := rootcatalog.BuildCollectionKey(collection.DatabaseID, id)
	raw, err := kc.MetaKv.Load(ctx, key)
	var guards []predicates.Predicate
	if collection.DatabaseID == util.DefaultDBID && errors.Is(err, merr.ErrIoKeyNotFound) {
		// Legacy rows live outside the database namespace. A concurrent
		// migration to the default database must invalidate this guard.
		guards = append(guards, predicates.KeyNotExist(key))
		key = rootcatalog.BuildCollectionKey(util.NonDBID, id)
		raw, err = kc.MetaKv.Load(ctx, key)
	}
	if errors.Is(err, merr.ErrIoKeyNotFound) || (err == nil && rootcatalog.IsTombstone(raw)) {
		return nil, nil
	}
	if err != nil {
		return nil, merr.Wrap(err, "load collection for compaction target creation")
	}
	current, err := decodeTargetCollection(raw, id)
	if err != nil {
		return nil, err
	}
	if current.GetState() != etcdpb.CollectionState_CollectionCreated || current.GetUpdateTimestamp() != collection.Revision {
		return nil, nil
	}
	return append(guards, predicates.ValueEqual(key, raw)), nil
}

func decodeTargetCollection(raw string, id int64) (*etcdpb.CollectionInfo, error) {
	current := &etcdpb.CollectionInfo{}
	if err := proto.Unmarshal([]byte(raw), current); err != nil {
		return nil, merr.WrapErrDataIntegrity(err, "decode collection %d for compaction target creation", id)
	}
	if current.GetID() != id {
		return nil, merr.WrapErrDataIntegrityMsg("collection %d identity does not match its key during compaction target creation", id)
	}
	return current, nil
}

func (kc *Catalog) targetCreateReplicationGuard(ctx context.Context) (predicates.Predicate, error) {
	const key = streamingcatalog.ReplicateConfigurationKey
	raw, err := kc.MetaKv.Load(ctx, key)
	if errors.Is(err, merr.ErrIoKeyNotFound) {
		return predicates.KeyNotExist(key), nil
	}
	if err != nil {
		return nil, merr.Wrap(err, "load replication topology for compaction target creation")
	}
	topology := &streamingpb.ReplicateConfigurationMeta{}
	if err := proto.Unmarshal([]byte(raw), topology); err != nil {
		return nil, merr.WrapErrDataIntegrity(err, "decode replication topology for compaction target creation")
	}
	if replicateutil.HasReplicationTopology(topology.GetReplicateConfiguration()) {
		return nil, merr.WrapErrOperationNotSupportedMsg("compaction targets are not supported on a replicating cluster")
	}
	// The process lock can be released before a timed-out KV request finishes.
	// Compare the durable topology so a late transaction cannot cross a change.
	return predicates.ValueEqual(key, raw), nil
}

func targetCreateFenceKey(collectionID int64) string {
	return compactionTargetCreateFencePrefix + "/" + strconv.FormatInt(collectionID, 10)
}

func activeOptimizeTargetKey(collectionID int64) string {
	return activeOptimizeTargetPrefix + "/" + strconv.FormatInt(collectionID, 10)
}

func targetCreateMayHaveCommitted(err error) bool {
	if err == nil {
		return false
	}
	if merr.IsCanceledOrTimeout(err) || tikverr.IsErrorUndetermined(err) {
		return true
	}
	// etcd normalizes known server errors before its KV wrapper attaches the
	// IO classification. Transport errors still expose their gRPC status.
	var etcdErr rpctypes.EtcdError
	if errors.As(err, &etcdErr) {
		return ambiguousTxnCode(etcdErr.Code())
	}
	rpc, ok := status.FromError(err)
	return ok && ambiguousTxnCode(rpc.Code())
}

func ambiguousTxnCode(code codes.Code) bool {
	switch code {
	case codes.Canceled, codes.DeadlineExceeded, codes.Unavailable, codes.Internal, codes.Unknown:
		return true
	default:
		return false
	}
}

func (kc *Catalog) UpdateStandingCompactionTarget(ctx context.Context, previous, next *datapb.CompactionTarget) error {
	id := previous.GetTargetID()
	current, raw, err := kc.loadCompactionTarget(ctx, id)
	if err != nil {
		return err
	}
	if current == nil || current.GetState() != datapb.TargetState_TARGET_STATE_ACTIVE ||
		!proto.Equal(current, previous) {
		return merr.WrapErrCompactionPlanConflict("standing compaction target changed during condition update")
	}
	key, value, err := buildCompactionTargetKV(next)
	if err != nil {
		return err
	}
	return kc.MetaKv.MultiSaveAndRemove(ctx, map[string]string{key: value}, nil, predicates.ValueEqual(key, raw))
}

func (kc *Catalog) StopCompactionTarget(ctx context.Context, stopped *datapb.CompactionTarget) (*datapb.CompactionTarget, bool, error) {
	id := stopped.GetTargetID()
	current, raw, err := kc.loadCompactionTarget(ctx, id)
	if err != nil || current == nil || current.GetState() == datapb.TargetState_TARGET_STATE_INACTIVE {
		return current, false, err
	}
	if current.GetState() != datapb.TargetState_TARGET_STATE_ACTIVE {
		return nil, false, merr.WrapErrDataIntegrityMsg("compaction target %d has invalid state %d", id, current.GetState())
	}
	current.State = stopped.GetState()
	current.InactivatedAtTS = stopped.GetInactivatedAtTS()
	current.StopReason = stopped.GetStopReason()
	key, value, err := buildCompactionTargetKV(current)
	if err != nil {
		return nil, false, err
	}
	guards := []predicates.Predicate{predicates.ValueEqual(key, raw)}
	var removes []string
	if current.GetIntent() == datapb.TargetIntent_INTENT_OPTIMIZE {
		slot := activeOptimizeTargetKey(current.GetCollectionID())
		owner, err := kc.MetaKv.Load(ctx, slot)
		if err != nil && !errors.Is(err, merr.ErrIoKeyNotFound) {
			return nil, false, merr.Wrap(err, "load active OPTIMIZE slot")
		}
		if err == nil {
			if owner != strconv.FormatInt(id, 10) {
				return nil, false, merr.WrapErrDataIntegrityMsg("active OPTIMIZE slot for collection %d does not match target %d", current.GetCollectionID(), id)
			}
			guards = append(guards, predicates.ValueEqual(slot, owner))
			removes = append(removes, slot)
		} else {
			// Retained target records created before the slot still close safely.
			guards = append(guards, predicates.KeyNotExist(slot))
		}
	}
	if err := kc.MetaKv.MultiSaveAndRemove(ctx, map[string]string{key: value}, removes, guards...); err != nil {
		return nil, false, err
	}
	return current, true, nil
}

func (kc *Catalog) DropCompactionTarget(ctx context.Context, record *datapb.CompactionTarget) error {
	if record == nil {
		return nil
	}
	key := buildCompactionTargetPath(record.GetTargetID())
	return kc.MetaKv.Remove(ctx, key)
}

func (kc *Catalog) DropCompactionTargetCreateFence(ctx context.Context, collectionID int64) error {
	return kc.MetaKv.Remove(ctx, targetCreateFenceKey(collectionID))
}

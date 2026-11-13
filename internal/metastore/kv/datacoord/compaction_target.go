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

	"github.com/cockroachdb/errors"
	tikverr "github.com/tikv/client-go/v2/error"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore"
	rootcatalog "github.com/milvus-io/milvus/internal/metastore/kv/rootcoord"
	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func (kc *Catalog) ListCompactionTargets(ctx context.Context) ([]*datapb.CompactionTarget, error) {
	records := make([]*datapb.CompactionTarget, 0)

	applyFn := func(key []byte, value []byte) error {
		record := &datapb.CompactionTarget{}
		if err := proto.Unmarshal(value, record); err != nil {
			return merr.WrapErrSerializationFailed(err, "unmarshal compaction target at %s", key)
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
		return nil, "", merr.WrapErrSerializationFailed(err, "unmarshal compaction target %d", targetID)
	}
	if record.GetTargetID() != targetID {
		return nil, "", merr.WrapErrDataIntegrityMsg("compaction target %d identity does not match its key", targetID)
	}
	return record, value, nil
}

func (kc *Catalog) CreateCompactionTarget(ctx context.Context, record *datapb.CompactionTarget, scope *metastore.CompactionTargetCollectionVersion) (bool, error) {
	key, value, err := buildCompactionTargetKV(record)
	if err != nil {
		return false, err
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	guards := []predicates.Predicate{predicates.KeyNotExist(key)}
	if id := record.GetCollectionID(); id != 0 {
		if scope == nil {
			return false, merr.WrapErrServiceInternalMsg("compaction target creation is missing the collection version")
		}
		collectionKey := rootcatalog.BuildCollectionKey(scope.DatabaseID, id)
		raw, err := kc.MetaKv.Load(ctx, collectionKey)
		if scope.DatabaseID == util.DefaultDBID && errors.Is(err, merr.ErrIoKeyNotFound) {
			// Collections that predate database namespaces keep their legacy
			// row. RootCoord reports the default DB for their public identity.
			collectionKey = rootcatalog.BuildCollectionKey(util.NonDBID, id)
			raw, err = kc.MetaKv.Load(ctx, collectionKey)
			guards = append(guards, predicates.KeyNotExist(rootcatalog.BuildCollectionKey(scope.DatabaseID, id)))
		}
		if errors.Is(err, merr.ErrIoKeyNotFound) || (err == nil && rootcatalog.IsTombstone(raw)) {
			return false, merr.WrapErrServiceUnavailableMsg("collection changed during compaction target creation")
		}
		if err != nil {
			return false, merr.Wrap(err, "load collection for compaction target creation")
		}
		current := &etcdpb.CollectionInfo{}
		if err := proto.Unmarshal([]byte(raw), current); err != nil {
			return false, merr.Wrap(err, "decode collection for compaction target creation")
		}
		if current.GetID() != id || current.GetState() != etcdpb.CollectionState_CollectionCreated ||
			current.GetUpdateTimestamp() != scope.Revision {
			return false, merr.WrapErrServiceUnavailableMsg("collection changed during compaction target creation")
		}
		guards = append(guards, predicates.ValueEqual(collectionKey, raw))
	}
	err = kc.MetaKv.MultiSaveAndRemove(ctx, map[string]string{key: value}, nil, guards...)
	return targetCreateMayHaveCommitted(err), err
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
	if err := kc.MetaKv.MultiSaveAndRemove(ctx, map[string]string{key: value}, nil, predicates.ValueEqual(key, raw)); err != nil {
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

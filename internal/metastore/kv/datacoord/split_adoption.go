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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func adoptionPrefix(collectionID int64) string {
	return fmt.Sprintf("datacoord-meta/split-adoption/%d/", collectionID)
}

func adoptionKey(collectionID, taskID int64) string {
	return fmt.Sprintf("%s%d", adoptionPrefix(collectionID), taskID)
}

func (kc *Catalog) LoadSplitAdoption(ctx context.Context, collectionID, taskID int64) (*model.SplitAdoption, error) {
	value, err := kc.MetaKv.Load(ctx, adoptionKey(collectionID, taskID))
	if errors.Is(err, merr.ErrIoKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	record, err := kc.decodeAdoption(ctx, []byte(value))
	if err != nil {
		return nil, err
	}
	if record.CollectionID != collectionID || record.TaskID != taskID {
		return nil, merr.WrapErrDataIntegrityMsg("split adoption key identity mismatch")
	}
	return record, nil
}

func (kc *Catalog) decodeAdoption(ctx context.Context, value []byte) (*model.SplitAdoption, error) {
	var record model.SplitAdoption
	if err := json.Unmarshal(value, &record); err != nil {
		return nil, merr.WrapErrDataIntegrityMsg("invalid split adoption root")
	}
	if record.CollectionID <= 0 || record.TaskID <= 0 || record.Source == "" || record.AttemptID == "" || record.Count < 0 || len(record.Digest) != 64 {
		return nil, merr.WrapErrDataIntegrityMsg("invalid split adoption identity")
	}
	if _, err := hex.DecodeString(record.Digest); err != nil {
		return nil, merr.WrapErrDataIntegrityMsg("invalid split adoption digest")
	}
	switch record.State {
	case "Preparing":
		return &record, nil
	case "Prepared", "Committed", "Cleaning", "Done":
	default:
		return nil, merr.WrapErrDataIntegrityMsg("invalid split adoption state %q", record.State)
	}
	moves, err := kc.readPagedValue(ctx, record.MovesValue)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(moves)
	if hex.EncodeToString(sum[:]) != record.Digest {
		return nil, merr.WrapErrDataIntegrityMsg("split adoption checksum mismatch")
	}
	if err := json.Unmarshal(moves, &record.Moves); err != nil {
		return nil, merr.WrapErrDataIntegrityMsg("invalid split adoption moves")
	}
	if len(record.Moves) != record.Count {
		return nil, merr.WrapErrDataIntegrityMsg("split adoption count mismatch")
	}
	seen := make(map[int64]bool, len(record.Moves))
	for _, move := range record.Moves {
		if move.SegmentID <= 0 || move.Target == "" || move.Target == record.Source || move.TransformStart == 0 || seen[move.SegmentID] {
			return nil, merr.WrapErrDataIntegrityMsg("invalid or duplicate split adoption move")
		}
		seen[move.SegmentID] = true
	}
	return &record, nil
}

func encodeAdoption(record *model.SplitAdoption) (string, error) {
	value, err := json.Marshal(record)
	if err != nil {
		return "", err
	}
	if len(value) > metadataRootBytes {
		return "", merr.Wrapf(merr.ErrServiceResourceInsufficient, "split adoption root exceeds storage budget")
	}
	return string(value), nil
}

func (kc *Catalog) saveAdoption(ctx context.Context, record *model.SplitAdoption) error {
	value, err := encodeAdoption(record)
	if err != nil {
		return err
	}
	return kc.MetaKv.Save(ctx, adoptionKey(record.CollectionID, record.TaskID), value)
}

func (kc *Catalog) PrepareSplitAdoption(ctx context.Context, record *model.SplitAdoption) (*model.SplitAdoption, error) {
	if record == nil || record.CollectionID <= 0 || record.TaskID <= 0 || record.Source == "" {
		return nil, merr.WrapErrServiceInternalMsg("invalid split adoption prepare identity")
	}
	seen := make(map[int64]struct{}, len(record.Moves))
	for _, move := range record.Moves {
		_, duplicate := seen[move.SegmentID]
		if move.SegmentID <= 0 || move.Target == "" || move.Target == record.Source || move.TransformStart == 0 || duplicate {
			return nil, merr.WrapErrServiceInternalMsg("invalid split adoption prepare move")
		}
		seen[move.SegmentID] = struct{}{}
	}
	existing, err := kc.LoadSplitAdoption(ctx, record.CollectionID, record.TaskID)
	if err != nil {
		return nil, err
	}
	if existing != nil && (existing.State == "Committed" || existing.State == "Cleaning" || existing.State == "Done") {
		return existing, nil
	}
	moves, err := json.Marshal(record.Moves)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(moves)
	record.Digest = hex.EncodeToString(sum[:])
	record.Count = len(record.Moves)
	// A fresh attempt prevents ABA if an old timed-out commit arrives after a
	// later prepare recreates an otherwise identical root.
	record.AttemptID = uuid.NewString()
	record.State = "Preparing"
	if existing != nil && existing.Source != record.Source {
		return nil, merr.WrapErrDataIntegrityMsg("split adoption source identity changed")
	}
	guard := predicates.KeyNotExist(adoptionKey(record.CollectionID, record.TaskID))
	if existing != nil {
		expected, err := encodeAdoption(existing)
		if err != nil {
			return nil, err
		}
		guard = predicates.ValueEqual(adoptionKey(record.CollectionID, record.TaskID), expected)
	}
	preparing, err := encodeAdoption(record)
	if err != nil {
		return nil, err
	}
	// A previous process's delayed commit may win after the read above. Never
	// overwrite its Committed root with a fresh uncommitted attempt.
	if err := kc.MetaKv.MultiSaveAndRemove(ctx, map[string]string{adoptionKey(record.CollectionID, record.TaskID): preparing}, nil, guard); err != nil {
		return nil, err
	}
	record.MovesValue, err = kc.preparePagedValue(ctx, record.CollectionID, moves)
	if err != nil {
		return nil, err
	}
	record.State = "Prepared"
	prepared, err := encodeAdoption(record)
	if err != nil {
		return nil, err
	}
	if err := kc.MetaKv.MultiSaveAndRemove(ctx, map[string]string{adoptionKey(record.CollectionID, record.TaskID): prepared}, nil,
		predicates.ValueEqual(adoptionKey(record.CollectionID, record.TaskID), preparing)); err != nil {
		return nil, err
	}
	return record, nil
}

func (kc *Catalog) CommitSplitAdoption(ctx context.Context, record *model.SplitAdoption, view *viewpb.DataViewOfCollection) error {
	if record == nil || record.AttemptID == "" || view == nil || view.GetDataVersion() == nil || view.GetCollectionId() != record.CollectionID || record.State != "Prepared" {
		return merr.WrapErrServiceInternalMsg("invalid split adoption publication identity")
	}
	if kc.MetaKv.MaxTxnOps() < 2 {
		return merr.Wrapf(merr.ErrServiceResourceInsufficient, "split adoption requires a two-key commit")
	}
	prepared, err := encodeAdoption(record)
	if err != nil {
		return err
	}
	encoded, err := proto.Marshal(view)
	if err != nil {
		return err
	}
	value, err := kc.preparePagedValue(ctx, record.CollectionID, encoded)
	if err != nil {
		return err
	}
	next := *record
	next.State = "Committed"
	root, err := encodeAdoption(&next)
	if err != nil {
		return err
	}
	if len(root)+len(value)+len(prepared)+1024 > metadataRootBytes {
		return merr.Wrapf(merr.ErrServiceResourceInsufficient, "split adoption publication exceeds storage budget")
	}
	key := buildDataViewVersionKey(view.GetCollectionId(), view.GetDataVersion().GetStreamingVersion(), view.GetDataVersion().GetCompactVersion())
	err = kc.MetaKv.MultiSaveAndRemove(ctx, map[string]string{adoptionKey(record.CollectionID, record.TaskID): root, key: value}, nil,
		predicates.ValueEqual(adoptionKey(record.CollectionID, record.TaskID), prepared))
	if err != nil {
		committed := kc.resolveSplitAdoptionCommit(ctx, record, key, value)
		if !committed {
			return err
		}
	}

	record.State = "Committed"
	return nil
}

// An uncertain transaction cannot release the caller's memory publication
// barrier until linearizable readback decides its outcome. Cancellation is not
// evidence of absence. A Prepared read must be fenced with a conditional state
// change before returning failure: the original transaction can still arrive
// after that read. This reconciliation can delay shutdown during a KV outage.
func (kc *Catalog) resolveSplitAdoptionCommit(ctx context.Context, record *model.SplitAdoption, key, value string) bool {
	resolveCtx := context.WithoutCancel(ctx)
	backoff := 100 * time.Millisecond
	for {
		attemptCtx, cancel := context.WithTimeout(resolveCtx, 10*time.Second)
		committed, err := kc.readSplitAdoptionOutcome(attemptCtx, record, key, value)
		cancel()
		if err == nil {
			return committed
		}
		mlog.RatedWarn(resolveCtx, 0.1, "waiting to resolve uncertain split adoption publication", mlog.FieldCollectionID(record.CollectionID), mlog.FieldTaskID(record.TaskID), mlog.Err(err))
		time.Sleep(backoff)
		backoff = min(backoff*2, 2*time.Second)
	}
}

func (kc *Catalog) readSplitAdoptionOutcome(ctx context.Context, record *model.SplitAdoption, key, value string) (bool, error) {
	durable, err := kc.LoadSplitAdoption(ctx, record.CollectionID, record.TaskID)
	if err != nil {
		return false, merr.Wrap(err, "resolve uncertain split adoption commit")
	}
	if durable == nil {
		return false, nil
	}
	if durable.AttemptID != record.AttemptID {
		if durable.State == "Preparing" || durable.State == "Prepared" {
			return false, nil
		}
		return false, merr.WrapErrDataIntegrityMsg("split adoption committed attempt identity mismatch")
	}
	if durable.State == "Preparing" {
		return false, nil
	}
	if durable.State == "Prepared" {
		prepared, err := encodeAdoption(durable)
		if err != nil {
			return false, err
		}
		return false, kc.fencePreparedAdoption(ctx, durable, prepared)
	}
	if durable.Digest != record.Digest {
		return false, merr.WrapErrDataIntegrityMsg("split adoption commit identity mismatch")
	}
	durableView, err := kc.MetaKv.Load(ctx, key)
	if err != nil {
		return false, merr.Wrap(err, "resolve uncertain split DataView publication")
	}
	if durableView != value {
		return false, merr.WrapErrDataIntegrityMsg("split adoption DataView publication identity mismatch")
	}
	return true, nil
}

// FencePreparedSplitAdoptions runs before the new leader loads either
// SegmentMeta or DataView. A prior leader's accepted commit can still be in
// flight even when a linearizable read observes Prepared. If it wins the CAS,
// startup retries the recovery phase and reads the committed owner and view.
func (kc *Catalog) FencePreparedSplitAdoptions(ctx context.Context) error {
	return kc.MetaKv.WalkWithPrefix(ctx, "datacoord-meta/split-adoption/", kc.paginationSize, func(key, value []byte) error {
		var record model.SplitAdoption
		if err := json.Unmarshal(value, &record); err != nil {
			return merr.WrapErrDataIntegrityMsg("invalid split adoption root")
		}
		if record.State != "Prepared" {
			return nil
		}
		if !adoptionWalkKeyMatches(string(key), adoptionKey(record.CollectionID, record.TaskID)) {
			return merr.WrapErrDataIntegrityMsg("split adoption key identity mismatch")
		}
		return kc.fencePreparedAdoption(ctx, &record, string(value))
	})
}

func (kc *Catalog) fencePreparedAdoption(ctx context.Context, record *model.SplitAdoption, expected string) error {
	aborted := *record
	aborted.State = "Preparing"
	aborted.AttemptID = uuid.NewString()
	fence, err := encodeAdoption(&aborted)
	if err != nil {
		return err
	}
	key := adoptionKey(record.CollectionID, record.TaskID)
	return kc.MetaKv.MultiSaveAndRemove(ctx, map[string]string{key: fence}, nil, predicates.ValueEqual(key, expected))
}

func (kc *Catalog) SaveSplitAdoptionState(ctx context.Context, record *model.SplitAdoption, state string) error {
	next := *record
	next.State = state
	if err := kc.saveAdoption(ctx, &next); err != nil {
		return err
	}
	record.State = state
	return nil
}

func (kc *Catalog) CanonicalizeSplitAdoptionSegment(ctx context.Context, segment *datapb.SegmentInfo) error {
	if proto.Size(segment) > metadataRootBytes-1024 {
		return merr.Wrapf(merr.ErrServiceResourceInsufficient, "segment %d exceeds canonical metadata value budget", segment.GetID())
	}
	return kc.Update(ctx, metastore.UpdateSegment(segment))
}

func applyAdoptionMove(segment *datapb.SegmentInfo, move model.SplitAdoptionMove) {
	segment.InsertChannel = move.Target
	segment.SplitTarget = ""
	segment.TransformStartAfterTimetick = move.TransformStart
	segment.StartPosition = &msgpb.MsgPosition{ChannelName: move.Target, Timestamp: move.TransformStart}
	segment.DmlPosition = &msgpb.MsgPosition{ChannelName: move.Target, Timestamp: move.TransformStart}
}

// Production walkers return backend keys including their configured MetaKV
// root. Memory backends return relative keys. Match the complete canonical
// collection/task suffix, including its path boundary, in either contract.
func adoptionWalkKeyMatches(actual, relative string) bool {
	return actual == relative || strings.HasSuffix(actual, "/"+relative)
}

func (kc *Catalog) applyCommittedAdoptions(ctx context.Context, collectionID int64, segments []*datapb.SegmentInfo) error {
	byID := make(map[int64]*datapb.SegmentInfo, len(segments))
	for _, segment := range segments {
		byID[segment.GetID()] = segment
	}
	return kc.MetaKv.WalkWithPrefix(ctx, adoptionPrefix(collectionID), kc.paginationSize, func(key, value []byte) error {
		record, err := kc.decodeAdoption(ctx, value)
		if err != nil {
			return err
		}
		if record.CollectionID != collectionID || !adoptionWalkKeyMatches(string(key), adoptionKey(collectionID, record.TaskID)) {
			return merr.WrapErrDataIntegrityMsg("split adoption collection mismatch")
		}
		if record.State != "Committed" && record.State != "Cleaning" {
			return nil
		}
		for _, move := range record.Moves {
			segment := byID[move.SegmentID]
			if segment == nil {
				continue
			} // subsequent drop/compaction is authoritative
			if segment.GetInsertChannel() == move.Target {
				continue
			} // canonical row can have a newer cursor/manifest
			if segment.GetInsertChannel() != record.Source || segment.GetPartitionID() != move.PartitionID || segment.GetSplitTarget() != move.Target {
				return merr.WrapErrDataIntegrityMsg("split adoption segment %d identity mismatch", move.SegmentID)
			}
			applyAdoptionMove(segment, move)
		}
		return nil
	})
}

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

package targets

import (
	"context"
	"errors"
	"math"
	"slices"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/datacoord/compaction/policy"
	"github.com/milvus-io/milvus/internal/datacoord/compaction/view"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func (r *CompactionTargetReconciler) selectableSchemaUpdates(ctx context.Context, auto bool, segments []*meta.SegmentInfo) []schemaUpdate {
	enabled := auto && paramtable.Get().DataCoordCfg.BumpSchemaVersionCompactionEnabled.GetAsBool()
	record, err := r.CompactionTargetMeta.SyncStanding(ctx, r.allocator, datapb.TargetIntent_INTENT_SCHEMA_VERSION, enabled, nil)
	if err != nil {
		mlog.RatedWarn(ctx, 1, "could not synchronize Standing Schema Version compaction target", mlog.Err(err))
		return nil
	}
	if !record.GetEnabled() {
		return nil
	}

	var updates []schemaUpdate
	var matches, awaitingV3 int
	collections := make(map[int64]collectionPlanningFacts)
	err = visitSchemaVersionMatches(ctx, r.handler, segments, func(segment *meta.SegmentInfo, collection *meta.CollectionInfo) error {
		matches++
		if segment.GetStorageVersion() != storage.StorageV3 || segment.GetManifestPath() == "" {
			awaitingV3++
			return nil
		}
		if segment.GetIsImporting() || segment.GetIsInvisible() || segment.GetNumOfRows() < 0 ||
			!policy.IsSharedCompactionSelectable(r.meta, segment) {
			return nil
		}
		facts, checked := collections[collection.ID]
		if !checked {
			facts = r.schemaPlanningFacts(ctx, collection)
			collections[collection.ID] = facts
		}
		if !facts.blocked {
			updates = append(updates, schemaUpdate{segment: segment, facts: facts})
		}
		return nil
	})
	if err != nil && ctx.Err() == nil {
		mlog.RatedWarn(ctx, 1, "could not fully observe Standing Schema Version compaction target",
			mlog.Int64("targetID", record.GetTargetID()), mlog.Err(err))
	}
	if len(updates) < matches {
		mlog.RatedInfo(ctx, 1, "Schema Version compaction target inputs awaiting selection",
			mlog.Int("matches", matches), mlog.Int("selectable", len(updates)), mlog.Int("awaitingV3Manifest", awaitingV3))
	}
	return updates
}

func (r *CompactionTargetReconciler) schemaPlanningFacts(ctx context.Context, collection *meta.CollectionInfo) collectionPlanningFacts {
	if r.meta.IsCollectionCompactionBlocked(collection.ID) {
		return collectionPlanningFacts{blocked: true}
	}
	ttl, err := common.GetCollectionTTLFromMap(collection.Properties)
	if err != nil {
		mlog.RatedWarn(ctx, 1, "could not determine collection TTL for Schema Version compaction target",
			mlog.FieldCollectionID(collection.ID), mlog.Err(err))
		return collectionPlanningFacts{blocked: true}
	}
	return collectionPlanningFacts{
		ttl: ttl, schema: proto.CloneOf(collection.Schema),
		expectedSize: policy.GetExpectedSegmentSize(r.IndexMeta, collection.ID, collection.Schema),
	}
}

type schemaUpdate struct {
	segment *meta.SegmentInfo
	facts   collectionPlanningFacts
}

func allocateSchemaViews(updates []schemaUpdate, allocated map[int64]struct{}, rewrites map[int64]*meta.SegmentInfo) []view.TargetView {
	slices.SortFunc(updates, func(a, b schemaUpdate) int { return compareSegment(a.segment, b.segment) })
	groups := make(map[view.CompactionGroupLabel]*view.BumpSchemaVersionView)
	var views []view.TargetView
	for _, update := range updates {
		id := update.segment.GetID()
		if _, assigned := allocated[id]; assigned || rewrites[id] != nil {
			continue
		}
		allocated[id] = struct{}{}
		input := view.GetTargetSegmentView(update.segment)
		label := *input.Label
		pool := groups[label]
		if pool == nil {
			pool = &view.BumpSchemaVersionView{RewriteInputs: view.RewriteInputs{
				Group: input.Label, Schema: update.facts.schema, TTL: update.facts.ttl, ExpectedSize: update.facts.expectedSize,
			}}
			groups[label] = pool
			views = append(views, pool)
		}
		pool.Segments = append(pool.Segments, input)
	}
	return views
}

// ObserveSchemaVersion reports the full Match, including inputs currently
// blocked from execution. An unknown effective schema makes the count unknown.
func ObserveSchemaVersion(ctx context.Context, mt *meta.Meta, handler meta.Handler) (int64, error) {
	var rows int64
	err := visitSchemaVersionMatches(ctx, handler, standingSegments(ctx, mt), func(segment *meta.SegmentInfo, _ *meta.CollectionInfo) error {
		count := segment.GetNumOfRows()
		if count < 0 || count > math.MaxInt64-rows {
			return merr.WrapErrDataIntegrityMsg("invalid Schema Version Match row count at segment %d", segment.GetID())
		}
		rows += count
		return nil
	})
	return rows, err
}

func visitSchemaVersionMatches(
	ctx context.Context,
	handler meta.Handler,
	segments []*meta.SegmentInfo,
	visit func(*meta.SegmentInfo, *meta.CollectionInfo) error,
) error {
	collections := make(map[int64]*meta.CollectionInfo)
	var firstErr error
	for _, segment := range segments {
		if err := ctx.Err(); err != nil {
			return err
		}
		id := segment.GetCollectionID()
		collection, checked := collections[id]
		if !checked {
			var err error
			collection, err = schemaVersionCollection(ctx, handler, id)
			if err != nil && firstErr == nil {
				firstErr = merr.Wrapf(err, "observe Schema Version for collection %d", id)
			}
			collections[id] = collection
		}
		if collection != nil && segment.GetSchemaVersion() < collection.Schema.GetVersion() {
			if err := visit(segment, collection); err != nil {
				return err
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return firstErr
}

func schemaVersionCollection(ctx context.Context, handler meta.Handler, id int64) (*meta.CollectionInfo, error) {
	published, err := handler.GetCollection(ctx, id)
	if errors.Is(err, merr.ErrCollectionNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if published == nil || published.IsExternal() {
		return nil, nil
	}
	pending, err := broadcast.GetPendingSchemaTarget(id)
	if err != nil {
		return nil, err
	}
	effective := *published
	if pending != nil && pending.Schema.GetVersion() >= published.Schema.GetVersion() {
		effective.Schema = pending.Schema
		effective.Properties = funcutil.KeyValuePair2Map(pending.Schema.GetProperties())
	}
	if effective.Schema == nil {
		return nil, merr.WrapErrDataIntegrityMsg("missing effective schema for collection %d", id)
	}
	if effective.IsExternal() {
		return nil, nil
	}
	return &effective, nil
}

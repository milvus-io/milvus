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
	"math"
	"slices"
	"strconv"
	"sync"

	"github.com/blang/semver/v4"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const (
	storageVersionConditionKey = "storage_version"
	storageFormatConditionKey  = "storage_format"
)

func IsStandingStorage(record *datapb.CompactionTarget) bool {
	return HasStandingCondition(record) &&
		(record.GetIntent() == datapb.TargetIntent_INTENT_STORAGE_VERSION ||
			record.GetIntent() == datapb.TargetIntent_INTENT_STORAGE_FORMAT)
}

func storageSegment(segment *meta.SegmentInfo) bool {
	return meta.IsSegmentHealthy(segment) && meta.IsFlushed(segment) && segment.GetLevel() != datapb.SegmentLevel_L0
}

type storageCondition struct {
	version int64
	format  string
}

func readStorageCondition(record *datapb.CompactionTarget) (storageCondition, error) {
	switch record.GetIntent() {
	case datapb.TargetIntent_INTENT_STORAGE_VERSION:
		version, err := strconv.ParseInt(record.GetProperties()[storageVersionConditionKey], 10, 64)
		if err == nil && (version == storage.StorageV2 || version == storage.StorageV3) {
			return storageCondition{version: version}, nil
		}
	case datapb.TargetIntent_INTENT_STORAGE_FORMAT:
		format := record.GetProperties()[storageFormatConditionKey]
		if validStorageFormat(format) {
			return storageCondition{format: format}, nil
		}
	}
	return storageCondition{}, merr.WrapErrDataIntegrityMsg("invalid saved storage condition for compaction target %d", record.GetTargetID())
}

func validStorageFormat(format string) bool {
	// These are the formats supported by dataNode.storage.format for insert
	// writers. Other formats accepted by the manifest reader can still differ.
	return format == "parquet" || format == "vortex"
}

type storageObservation struct {
	match []*meta.SegmentInfo
	rows  int64
}

func (observation *storageObservation) addMatch(segment *meta.SegmentInfo) error {
	rows := segment.GetNumOfRows()
	if rows < 0 || rows > math.MaxInt64-observation.rows {
		return merr.WrapErrDataIntegrityMsg("invalid storage observation row count at segment %d", segment.GetID())
	}
	observation.rows += rows
	observation.match = append(observation.match, segment)
	return nil
}

func observeStorage(ctx context.Context, mt *meta.Meta, record *datapb.CompactionTarget, segments []*meta.SegmentInfo) (storageObservation, error) {
	condition, err := readStorageCondition(record)
	if err != nil {
		return storageObservation{}, err
	}

	var observation storageObservation
	var firstErr error
	internal := make(map[int64]bool)
	if condition.format != "" {
		return observeStorageFormat(ctx, mt, condition.format, segments, internal)
	}
	for _, segment := range segments {
		if err := ctx.Err(); err != nil {
			return observation, err
		}
		if segment.GetStorageVersion() != condition.version && internalStorageSegment(mt, internal, segment) {
			if err := observation.addMatch(segment); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return observation, err
	}
	return observation, firstErr
}

func internalStorageSegment(mt *meta.Meta, internal map[int64]bool, segment *meta.SegmentInfo) bool {
	collectionID := segment.GetCollectionID()
	inScope, checked := internal[collectionID]
	if !checked {
		collection := mt.GetCollection(collectionID)
		inScope = collection.IsCreated() && !collection.IsExternal()
		internal[collectionID] = inScope
	}
	return inScope
}

func observeStorageFormat(ctx context.Context, mt *meta.Meta, format string, segments []*meta.SegmentInfo, internal map[int64]bool) (storageObservation, error) {
	config := meta.CreateStorageConfig()
	var observation storageObservation
	var firstErr error
	var mu sync.Mutex
	observe := func(segment *meta.SegmentInfo, formats []string, err error) {
		if err == nil && len(formats) == 0 && segment.GetNumOfRows() > 0 {
			err = merr.WrapErrDataIntegrityMsg("nonempty segment %d has no identifiable manifest insert groups", segment.GetID())
		}
		mismatch := slices.ContainsFunc(formats, func(actual string) bool { return actual != format })
		if err == nil && !mismatch {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		if err == nil {
			err = observation.addMatch(segment)
		}
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	var reads errgroup.Group
	reads.SetLimit(meta.SegmentIndexManifestReadConcurrency())
	for _, segment := range segments {
		if ctx.Err() != nil {
			break
		}
		if segment.GetStorageVersion() != storage.StorageV3 || !internalStorageSegment(mt, internal, segment) {
			continue
		}
		if formats, found := mt.CachedManifestInsertFormats(segment); found {
			observe(segment, formats, nil)
			continue
		}
		reads.Go(func() error {
			formats, err := mt.GetManifestInsertFormats(ctx, segment, config)
			observe(segment, formats, err)
			return nil
		})
	}
	_ = reads.Wait()
	if err := ctx.Err(); err != nil {
		return observation, err
	}
	return observation, firstErr
}

// ObserveStorage uses the saved condition and the published segment state.
// Execution readiness does not change the observed row count.
func ObserveStorage(ctx context.Context, mt *meta.Meta, record *datapb.CompactionTarget) (int64, error) {
	observation, err := observeStorage(ctx, mt, record, mt.SelectSegments(ctx, meta.SegmentFilterFunc(storageSegment)))
	if err != nil {
		return 0, err
	}
	return observation.rows, nil
}

func (r *CompactionTargetReconciler) selectableStorageRewrites(ctx context.Context, auto bool, segments []*meta.SegmentInfo) []*meta.SegmentInfo {
	version := storage.StorageV2
	if Params.CommonCfg.UseLoonFFI.GetAsBool() {
		version = storage.StorageV3
	}
	formatEnabled := auto && Params.DataCoordCfg.StorageFormatCompactionEnabled.GetAsBool()
	var formatProperties map[string]string
	if version == storage.StorageV3 {
		format := Params.DataNodeCfg.StorageFormat.GetValue()
		if validStorageFormat(format) {
			formatProperties = map[string]string{storageFormatConditionKey: format}
		} else if formatEnabled {
			mlog.RatedWarn(ctx, 1, "invalid storage compaction writer format", mlog.String("format", format))
		}
	}

	sources := [...]struct {
		intent     datapb.TargetIntent
		enabled    bool
		properties map[string]string
	}{
		{
			intent: datapb.TargetIntent_INTENT_STORAGE_VERSION, enabled: auto && Params.DataCoordCfg.StorageVersionCompactionEnabled.GetAsBool(),
			properties: map[string]string{storageVersionConditionKey: strconv.FormatInt(version, 10)},
		},
		{
			intent:     datapb.TargetIntent_INTENT_STORAGE_FORMAT,
			enabled:    auto && Params.DataCoordCfg.StorageFormatCompactionEnabled.GetAsBool() && formatProperties != nil,
			properties: formatProperties,
		},
	}
	records := make([]*datapb.CompactionTarget, 0, len(sources))
	for _, source := range sources {
		record, err := r.CompactionTargetMeta.SyncStanding(ctx, r.allocator, source.intent, source.enabled, source.properties)
		if err != nil {
			mlog.RatedWarn(ctx, 1, "could not synchronize Standing storage compaction target",
				mlog.Stringer("family", source.intent), mlog.Err(err))
			continue
		}
		if record.GetEnabled() {
			records = append(records, record)
		}
	}

	var matches []*meta.SegmentInfo
	for _, record := range records {
		observation, err := observeStorage(ctx, r.meta, record, segments)
		if err != nil && ctx.Err() == nil {
			mlog.RatedWarn(ctx, 1, "could not fully observe Standing storage compaction target",
				mlog.Stringer("family", record.GetIntent()), mlog.Err(err))
		}
		matches = append(matches, observation.match...)
	}
	if len(matches) == 0 || !r.storageCompactionReady(ctx) {
		return nil
	}

	textCollections := make(map[int64]bool)
	selectable := make([]*meta.SegmentInfo, 0, len(matches))
	for _, segment := range matches {
		if segment.GetIsImporting() {
			continue
		}
		if version == storage.StorageV2 {
			collectionID := segment.GetCollectionID()
			hasText, checked := textCollections[collectionID]
			if !checked {
				hasText = collectionHasText(r.meta.GetCollection(collectionID))
				textCollections[collectionID] = hasText
			}
			if hasText {
				continue
			}
		}
		selectable = append(selectable, segment)
	}
	return selectable
}

func (r *CompactionTargetReconciler) storageCompactionReady(ctx context.Context) bool {
	requirement := Params.DataCoordCfg.StorageVersionCompactionSessionVersionRequirement.GetValue()
	minimum, err := semver.Parse(requirement)
	if err != nil {
		mlog.RatedWarn(ctx, 1, "invalid storage compaction query node session version requirement", mlog.String("requirement", requirement), mlog.Err(err))
		return false
	}
	if r.versionManager == nil {
		return false
	}
	actual := r.versionManager.GetMinimalSessionVer()
	if actual.LT(minimum) {
		mlog.RatedInfo(ctx, 1, "storage compaction awaits query node session version",
			mlog.String("minimumSessionVersion", actual.String()), mlog.String("requirement", requirement))
		return false
	}
	return true
}

func collectionHasText(collection *meta.CollectionInfo) bool {
	if collection == nil {
		return false
	}
	for _, field := range collection.Schema.GetFields() {
		if field.GetDataType() == schemapb.DataType_Text {
			return true
		}
	}
	return false
}

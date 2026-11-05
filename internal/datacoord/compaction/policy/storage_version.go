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

package policy

import (
	"context"
	"time"

	"github.com/blang/semver/v4"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/compaction/view"
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type StorageVersionUpgradePolicy struct {
	meta           *meta.Meta
	allocator      allocator.Allocator
	handler        meta.Handler
	versionManager index.IndexEngineVersionManager

	// Rate limiting state, no need to be thread-safe since it is only used in one goroutine
	lastPeriod   time.Time
	currentCount int
}

func NewStorageVersionUpgradePolicy(mt *meta.Meta, allocator allocator.Allocator, handler meta.Handler, versionMgr index.IndexEngineVersionManager) *StorageVersionUpgradePolicy {
	return &StorageVersionUpgradePolicy{
		meta:           mt,
		allocator:      allocator,
		handler:        handler,
		versionManager: versionMgr,
	}
}

func (policy *StorageVersionUpgradePolicy) Name() string {
	return "storageVersionUpgrade"
}

func (policy *StorageVersionUpgradePolicy) Enable() bool {
	return paramtable.Get().DataCoordCfg.StorageVersionCompactionEnabled.GetAsBool() ||
		paramtable.Get().DataCoordCfg.StorageFormatCompactionEnabled.GetAsBool()
}

func (policy *StorageVersionUpgradePolicy) targetVersion() int64 {
	targetVersion := storage.StorageV2
	if paramtable.Get().CommonCfg.UseLoonFFI.GetAsBool() {
		targetVersion = storage.StorageV3
	}
	return targetVersion
}

func segmentColumnGroupFormatsAllEqual(segment *meta.SegmentInfo, targetFormat string) bool {
	if targetFormat == "" || segment.GetStorageVersion() != storage.StorageV3 {
		return true
	}

	binlogs := segment.GetBinlogs()
	if len(binlogs) == 0 {
		mlog.Warn(context.TODO(), "unexpected empty binlogs for V3 segment during storage format compaction",
			mlog.Int64("segmentID", segment.GetID()),
			mlog.Int64("collectionID", segment.GetCollectionID()),
			mlog.String("targetFormat", targetFormat))
		return false
	}

	for _, fieldBinlog := range binlogs {
		if fieldBinlog.GetFormat() != targetFormat {
			return false
		}
	}
	return true
}

func (policy *StorageVersionUpgradePolicy) Trigger(ctx context.Context) (map[CompactionTriggerType][]view.CompactionView, error) {
	versionReqStr := paramtable.Get().DataCoordCfg.StorageVersionCompactionSessionVersionRequirement.GetValue()
	versionRequirement, err := semver.Parse(versionReqStr)
	if err != nil {
		mlog.Warn(ctx, "failed to parse storage version upgrade version requirement", mlog.String("versionStr", versionReqStr), mlog.Err(err))
		return map[CompactionTriggerType][]view.CompactionView{}, err
	}

	minVersion := policy.versionManager.GetMinimalSessionVer()
	if minVersion.LT(versionRequirement) {
		mlog.Info(ctx, "storage version upgrade policy skipped due to minimal querynode version does not satisfy requirement", mlog.String("minVersion", minVersion.String()), mlog.String("requirement", versionRequirement.String()))
		return map[CompactionTriggerType][]view.CompactionView{}, nil
	}

	if time.Since(policy.lastPeriod) > paramtable.Get().DataCoordCfg.StorageVersionCompactionRateLimitInterval.GetAsDuration(time.Second) {
		policy.currentCount = 0
		policy.lastPeriod = time.Now()
	}

	maxCount := paramtable.Get().DataCoordCfg.StorageVersionCompactionRateLimitTokens.GetAsInt()
	if policy.currentCount >= maxCount {
		return map[CompactionTriggerType][]view.CompactionView{TriggerTypeStorageVersionUpgrade: nil}, nil
	}

	collections := policy.meta.GetCollections()
	views := make([]view.CompactionView, 0)
	for _, collection := range collections {
		if policy.currentCount >= maxCount {
			break
		}
		if policy.meta.IsCollectionCompactionBlocked(collection.ID) {
			mlog.Info(ctx, "skip storage version compaction for collection due to unloaded protected snapshot RefIndex",
				mlog.FieldCollectionID(collection.ID))
			continue
		}
		collectionViews, err := policy.TriggerOneCollection(ctx, collection.ID, maxCount)
		if err != nil {
			// not throw this error because no need to fail because of one collection
			mlog.Warn(ctx, "fail to trigger storage version compaction", mlog.FieldCollectionID(collection.ID), mlog.Err(err))
			continue
		}
		views = append(views, collectionViews...)
	}
	return map[CompactionTriggerType][]view.CompactionView{TriggerTypeStorageVersionUpgrade: views}, nil
}

func (policy *StorageVersionUpgradePolicy) TriggerOneCollection(ctx context.Context, collectionID int64, maxCount int) ([]view.CompactionView, error) {
	collection, err := policy.handler.GetCollection(ctx, collectionID)
	if err != nil {
		mlog.Warn(ctx, "fail to apply storageVersionUpgradePolicy, unable to get collection from handler",
			mlog.Err(err))
		return nil, err
	}
	if collection == nil {
		mlog.Warn(ctx, "fail to apply storageVersionUpgradePolicy, collection not exist")
		return nil, nil
	}
	if collection.IsExternal() {
		mlog.Info(ctx, "skip storage version compaction for external collection", mlog.FieldCollectionID(collectionID))
		return nil, nil
	}

	collectionTTL, err := common.GetCollectionTTLFromMap(collection.Properties)
	if err != nil {
		mlog.Warn(ctx, "failed to apply storageVersionUpgradePolicy, get collection ttl failed")
		return nil, err
	}

	targetVersion := policy.targetVersion()
	// TEXT fields require V3 manifest storage for LOB support and cannot be
	// downgraded. If the configured target version is lower than V3 for a
	// collection that has a TEXT field, skip this collection entirely instead
	// of silently bumping the target
	if targetVersion < storage.StorageV3 {
		for _, field := range collection.Schema.GetFields() {
			if field.GetDataType() == schemapb.DataType_Text {
				mlog.Warn(ctx, "storage version upgrade policy skipped: collection has TEXT field but configured target storage version is lower than V3, refusing to downgrade",
					mlog.Int64("targetVersion", targetVersion),
					mlog.Int64("requiredVersion", storage.StorageV3))
				return nil, nil
			}
		}
	}
	versionEnabled := paramtable.Get().DataCoordCfg.StorageVersionCompactionEnabled.GetAsBool()
	formatEnabled := paramtable.Get().DataCoordCfg.StorageFormatCompactionEnabled.GetAsBool()
	targetFormat := paramtable.Get().DataNodeCfg.StorageFormat.GetValue()

	segments := policy.meta.SelectSegments(ctx, meta.WithCollection(collectionID), meta.SegmentFilterFunc(func(segment *meta.SegmentInfo) bool {
		return meta.IsSegmentHealthy(segment) &&
			meta.IsFlushed(segment) &&
			!segment.IsCompacting &&
			!segment.GetIsImporting() &&
			segment.GetLevel() != datapb.SegmentLevel_L0 &&
			!policy.meta.IsSegmentCompactionProtected(segment.GetID()) &&
			((versionEnabled && segment.GetStorageVersion() != targetVersion) ||
				(formatEnabled &&
					targetVersion == storage.StorageV3 &&
					segment.GetStorageVersion() == storage.StorageV3 &&
					!segmentColumnGroupFormatsAllEqual(segment, targetFormat)))
	}))
	if len(segments) == 0 || policy.currentCount >= maxCount {
		return nil, nil
	}

	newTriggerID, err := policy.allocator.AllocID(ctx)
	if err != nil {
		mlog.Warn(ctx, "fail to apply storageVersionUpgradePolicy, unable to allocate triggerID", mlog.Err(err))
		return nil, err
	}

	views := make([]view.CompactionView, 0, len(segments))
	for _, segment := range segments {
		if policy.currentCount >= maxCount {
			break
		}
		segmentViews := view.GetViewsByInfo(segment)
		cv := &MixSegmentView{
			Label:         segmentViews[0].Label,
			Segments:      segmentViews,
			CollectionTTL: collectionTTL,
			TriggerID:     newTriggerID,
		}
		views = append(views, cv)
		policy.currentCount++
	}
	return views, nil
}

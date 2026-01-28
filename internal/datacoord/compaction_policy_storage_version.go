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
	"time"

	"go.uber.org/zap"

	"github.com/blang/semver/v4"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type storageVersionUpgradePolicy struct {
	meta           *meta
	allocator      allocator.Allocator
	handler        Handler
	versionManager IndexEngineVersionManager

	// Rate limiting state, no need to be thread-safe since it is only used in one goroutine
	lastPeriod   time.Time
	currentCount int
}

func newStorageVersionUpgradePolicy(meta *meta, allocator allocator.Allocator, handler Handler, versionMgr IndexEngineVersionManager) *storageVersionUpgradePolicy {
	return &storageVersionUpgradePolicy{
		meta:           meta,
		allocator:      allocator,
		handler:        handler,
		versionManager: versionMgr,
	}
}

func (policy *storageVersionUpgradePolicy) Enable() bool {
	return paramtable.Get().DataCoordCfg.StorageVersionCompactionEnabled.GetAsBool()
}

func (policy *storageVersionUpgradePolicy) targetVersion() int64 {
	targetVersion := storage.StorageV2
	// Uncomment after stv3 cp to 2.6
	// if paramtable.Get().CommonCfg.UseLoonFFI.GetAsBool() {
	// 	targetVersion = storage.StorageV3
	// }
	return targetVersion
}

func (policy *storageVersionUpgradePolicy) Trigger(ctx context.Context) (map[CompactionTriggerType][]CompactionView, error) {
	versionReqStr := paramtable.Get().DataCoordCfg.StorageVersionCompactionMinSessionVersion.GetValue()
	versionRequirement, err := semver.Parse(versionReqStr)
	if err != nil {
		log.Warn("failed to parse storage version upgrade version requirement", zap.String("versionStr", versionReqStr), zap.Error(err))
		return map[CompactionTriggerType][]CompactionView{}, err
	}

	minVersion := policy.versionManager.GetMinimalSessionVer()
	if minVersion.LT(versionRequirement) {
		log.Info("storage version upgrade policy skipped due to minimal querynode version does not satisfy requirement", zap.String("minVersion", minVersion.String()), zap.String("requirement", versionRequirement.String()))
		return map[CompactionTriggerType][]CompactionView{}, nil
	}

	collections := policy.meta.GetCollections()

	if time.Since(policy.lastPeriod) > paramtable.Get().DataCoordCfg.StorageVersionCompactionRateLimitInterval.GetAsDuration(time.Second) {
		policy.currentCount = 0
		policy.lastPeriod = time.Now()
	}

	maxCount := paramtable.Get().DataCoordCfg.StorageVersionCompactionRateLimitTokens.GetAsInt()

	views := make([]CompactionView, 0)
	for _, collection := range collections {
		if policy.currentCount >= maxCount {
			break
		}
		collectionViews, err := policy.triggerOneCollection(ctx, collection.ID, maxCount)
		if err != nil {
			// not throw this error because no need to fail because of one collection
			log.Warn("fail to trigger storage version compaction", zap.Int64("collectionID", collection.ID), zap.Error(err))
			continue
		}
		views = append(views, collectionViews...)
	}
	return map[CompactionTriggerType][]CompactionView{TriggerTypeStorageVersionUpgrade: views}, nil
}

func (policy *storageVersionUpgradePolicy) triggerOneCollection(ctx context.Context, collectionID int64, maxCount int) ([]CompactionView, error) {
	log := log.With(zap.Int64("collectionID", collectionID))
	collection, err := policy.handler.GetCollection(ctx, collectionID)
	if err != nil {
		log.Warn("fail to apply storageVersionUpgradePolicy, unable to get collection from handler",
			zap.Error(err))
		return nil, err
	}
	if collection == nil {
		log.Warn("fail to apply storageVersionUpgradePolicy, collection not exist")
		return nil, nil
	}

	collectionTTL, err := common.GetCollectionTTLFromMap(collection.Properties, paramtable.Get().CommonCfg.EntityExpirationTTL.GetAsDuration(time.Second))
	if err != nil {
		log.Warn("failed to apply storageVersionUpgradePolicy, get collection ttl failed")
		return nil, err
	}

	newTriggerID, err := policy.allocator.AllocID(ctx)
	if err != nil {
		log.Warn("fail to apply storageVersionUpgradePolicy, unable to allocate triggerID", zap.Error(err))
		return nil, err
	}

	targetVersion := policy.targetVersion()

	segments := policy.meta.SelectSegments(ctx, WithCollection(collectionID), SegmentFilterFunc(func(segment *SegmentInfo) bool {
		return isSegmentHealthy(segment) &&
			isFlushed(segment) &&
			!segment.isCompacting &&
			!segment.GetIsImporting() &&
			segment.GetLevel() != datapb.SegmentLevel_L0 &&
			segment.GetStorageVersion() != targetVersion
	}))

	views := make([]CompactionView, 0, len(segments))
	for _, segment := range segments {
		if policy.currentCount >= maxCount {
			break
		}
		segmentViews := GetViewsByInfo(segment)
		view := &MixSegmentView{
			label:         segmentViews[0].label,
			segments:      segmentViews,
			collectionTTL: collectionTTL,
			triggerID:     newTriggerID,
		}
		views = append(views, view)
		policy.currentCount++
	}
	return views, nil
}

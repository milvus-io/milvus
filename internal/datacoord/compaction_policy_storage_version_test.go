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
	"testing"
	"time"

	"github.com/blang/semver/v4"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestStorageVersionUpgradePolicySuite(t *testing.T) {
	suite.Run(t, new(StorageVersionUpgradePolicySuite))
}

type StorageVersionUpgradePolicySuite struct {
	suite.Suite

	mockAlloc  *allocator.MockAllocator
	testLabel  *CompactionGroupLabel
	handler    *NMockHandler
	versionMgr *MockVersionManager

	policy *storageVersionUpgradePolicy
}

func (s *StorageVersionUpgradePolicySuite) SetupTest() {
	s.testLabel = &CompactionGroupLabel{
		CollectionID: 1,
		PartitionID:  10,
		Channel:      "ch-1",
	}

	meta := &meta{
		segments:    NewSegmentsInfo(),
		collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
	}

	s.mockAlloc = allocator.NewMockAllocator(s.T())
	s.handler = NewNMockHandler(s.T())
	s.versionMgr = NewMockVersionManager(s.T())
	s.policy = newStorageVersionUpgradePolicy(meta, s.mockAlloc, s.handler, s.versionMgr)
}

func (s *StorageVersionUpgradePolicySuite) TestEnable() {
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.StorageVersionCompactionEnabled.Key)

	paramtable.Get().Save(paramtable.Get().DataCoordCfg.StorageVersionCompactionEnabled.Key, "false")
	s.False(s.policy.Enable())

	// Test with enabled
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.StorageVersionCompactionEnabled.Key, "true")
	s.True(s.policy.Enable())
}

func (s *StorageVersionUpgradePolicySuite) TestTargetVersion() {
	s.T().Skip("storage v3 not supported yet")
	// Default: UseLoonFFI is false, target should be StorageV2
	// paramtable.Get().Save(paramtable.Get().CommonCfg.UseLoonFFI.Key, "false")
	// defer paramtable.Get().Reset(paramtable.Get().CommonCfg.UseLoonFFI.Key)
	// s.Equal(storage.StorageV2, s.policy.targetVersion())

	// // When UseLoonFFI is true, target should be StorageV3
	// paramtable.Get().Save(paramtable.Get().CommonCfg.UseLoonFFI.Key, "true")
	// s.Equal(storage.StorageV3, s.policy.targetVersion())
}

func (s *StorageVersionUpgradePolicySuite) TestTriggerNoCollections() {
	// Mock version manager to return a version that satisfies requirement
	s.versionMgr.EXPECT().GetMinimalSessionVer().Return(semver.MustParse("2.7.0"))

	// Test with no collections
	events, err := s.policy.Trigger(context.Background())
	s.NoError(err)
	s.NotNil(events)
	gotViews, ok := events[TriggerTypeStorageVersionUpgrade]
	s.True(ok)
	s.Empty(gotViews)
}

func (s *StorageVersionUpgradePolicySuite) TestTriggerWithSegments() {
	ctx := context.Background()
	collID := int64(100)

	// Setup params
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.StorageVersionCompactionEnabled.Key, "true")
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.StorageVersionCompactionRateLimitInterval.Key, "1")
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.StorageVersionCompactionRateLimitTokens.Key, "10")
	// paramtable.Get().Save(paramtable.Get().CommonCfg.UseLoonFFI.Key, "true") // target is StorageV3
	defer func() {
		paramtable.Get().Reset(paramtable.Get().DataCoordCfg.StorageVersionCompactionEnabled.Key)
		paramtable.Get().Reset(paramtable.Get().DataCoordCfg.StorageVersionCompactionRateLimitInterval.Key)
		paramtable.Get().Reset(paramtable.Get().DataCoordCfg.StorageVersionCompactionRateLimitTokens.Key)
		// paramtable.Get().Reset(paramtable.Get().CommonCfg.UseLoonFFI.Key)
	}()

	coll := &collectionInfo{
		ID:     collID,
		Schema: newTestSchema(),
	}
	s.handler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(coll, nil)
	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(int64(1000), nil)

	// Create segments with different storage versions
	segments := make(map[UniqueID]*SegmentInfo)
	// Segment with old storage version (should be upgraded)
	segments[101] = &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             101,
			CollectionID:   collID,
			PartitionID:    10,
			InsertChannel:  "ch-1",
			Level:          datapb.SegmentLevel_L1,
			State:          commonpb.SegmentState_Flushed,
			NumOfRows:      10000,
			StorageVersion: storage.StorageV1, // Old version
		},
	}
	// Segment already at target version (should NOT be upgraded)
	segments[102] = &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             102,
			CollectionID:   collID,
			PartitionID:    10,
			InsertChannel:  "ch-1",
			Level:          datapb.SegmentLevel_L1,
			State:          commonpb.SegmentState_Flushed,
			NumOfRows:      10000,
			StorageVersion: storage.StorageV2, // Already at target
		},
	}
	// L0 segment (should NOT be upgraded)
	segments[103] = &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             103,
			CollectionID:   collID,
			PartitionID:    10,
			InsertChannel:  "ch-1",
			Level:          datapb.SegmentLevel_L0,
			State:          commonpb.SegmentState_Flushed,
			NumOfRows:      1000,
			StorageVersion: storage.StorageV2,
		},
	}

	segmentsInfo := &SegmentsInfo{
		segments: segments,
		secondaryIndexes: segmentInfoIndexes{
			coll2Segments: map[UniqueID]map[UniqueID]*SegmentInfo{
				collID: segments,
			},
		},
	}

	collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
	collections.Insert(collID, coll)

	s.policy.meta = &meta{
		segments:    segmentsInfo,
		collections: collections,
	}

	views, err := s.policy.triggerOneCollection(ctx, collID, 10)
	s.NoError(err)
	// Only segment 101 should be triggered (old version, not L0)
	s.Equal(1, len(views))
}

func (s *StorageVersionUpgradePolicySuite) TestTriggerWithCompactingSegment() {
	ctx := context.Background()
	collID := int64(100)

	// Setup params
	// paramtable.Get().Save(paramtable.Get().CommonCfg.UseLoonFFI.Key, "true")
	// defer paramtable.Get().Reset(paramtable.Get().CommonCfg.UseLoonFFI.Key)

	coll := &collectionInfo{
		ID:     collID,
		Schema: newTestSchema(),
	}
	s.handler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(coll, nil)
	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(int64(1000), nil)

	// Create a compacting segment (should NOT be upgraded)
	segments := make(map[UniqueID]*SegmentInfo)
	segments[101] = &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             101,
			CollectionID:   collID,
			PartitionID:    10,
			InsertChannel:  "ch-1",
			Level:          datapb.SegmentLevel_L1,
			State:          commonpb.SegmentState_Flushed,
			NumOfRows:      10000,
			StorageVersion: storage.StorageV1,
		},
		isCompacting: true, // Already compacting
	}

	segmentsInfo := &SegmentsInfo{
		segments: segments,
		secondaryIndexes: segmentInfoIndexes{
			coll2Segments: map[UniqueID]map[UniqueID]*SegmentInfo{
				collID: segments,
			},
		},
	}

	collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
	collections.Insert(collID, coll)

	s.policy.meta = &meta{
		segments:    segmentsInfo,
		collections: collections,
	}

	views, err := s.policy.triggerOneCollection(ctx, collID, 10)
	s.NoError(err)
	s.Equal(0, len(views)) // No segments should be triggered
}

func (s *StorageVersionUpgradePolicySuite) TestTriggerWithImportingSegment() {
	ctx := context.Background()
	collID := int64(100)

	// Setup params
	// paramtable.Get().Save(paramtable.Get().CommonCfg.UseLoonFFI.Key, "true")
	// defer paramtable.Get().Reset(paramtable.Get().CommonCfg.UseLoonFFI.Key)

	coll := &collectionInfo{
		ID:     collID,
		Schema: newTestSchema(),
	}
	s.handler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(coll, nil)
	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(int64(1000), nil)

	// Create an importing segment (should NOT be upgraded)
	segments := make(map[UniqueID]*SegmentInfo)
	segments[101] = &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             101,
			CollectionID:   collID,
			PartitionID:    10,
			InsertChannel:  "ch-1",
			Level:          datapb.SegmentLevel_L1,
			State:          commonpb.SegmentState_Flushed,
			NumOfRows:      10000,
			StorageVersion: storage.StorageV1,
			IsImporting:    true, // Importing
		},
	}

	segmentsInfo := &SegmentsInfo{
		segments: segments,
		secondaryIndexes: segmentInfoIndexes{
			coll2Segments: map[UniqueID]map[UniqueID]*SegmentInfo{
				collID: segments,
			},
		},
	}

	collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
	collections.Insert(collID, coll)

	s.policy.meta = &meta{
		segments:    segmentsInfo,
		collections: collections,
	}

	views, err := s.policy.triggerOneCollection(ctx, collID, 10)
	s.NoError(err)
	s.Equal(0, len(views)) // No segments should be triggered
}

func (s *StorageVersionUpgradePolicySuite) TestTriggerRateLimiting() {
	ctx := context.Background()
	collID := int64(100)

	// Setup params
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.StorageVersionCompactionEnabled.Key, "true")
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.StorageVersionCompactionRateLimitInterval.Key, "3600") // 1 hour
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.StorageVersionCompactionRateLimitTokens.Key, "2")      // Max 2 per interval
	// paramtable.Get().Save(paramtable.Get().CommonCfg.UseLoonFFI.Key, "true")
	defer func() {
		paramtable.Get().Reset(paramtable.Get().DataCoordCfg.StorageVersionCompactionEnabled.Key)
		paramtable.Get().Reset(paramtable.Get().DataCoordCfg.StorageVersionCompactionRateLimitInterval.Key)
		paramtable.Get().Reset(paramtable.Get().DataCoordCfg.StorageVersionCompactionRateLimitTokens.Key)
		// paramtable.Get().Reset(paramtable.Get().CommonCfg.UseLoonFFI.Key)
	}()

	coll := &collectionInfo{
		ID:     collID,
		Schema: newTestSchema(),
	}
	s.handler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(coll, nil)
	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(int64(1000), nil)

	// Create 5 segments that need upgrade
	segments := make(map[UniqueID]*SegmentInfo)
	for i := int64(101); i <= 105; i++ {
		segments[i] = &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:             i,
				CollectionID:   collID,
				PartitionID:    10,
				InsertChannel:  "ch-1",
				Level:          datapb.SegmentLevel_L1,
				State:          commonpb.SegmentState_Flushed,
				NumOfRows:      10000,
				StorageVersion: storage.StorageV1,
			},
		}
	}

	segmentsInfo := &SegmentsInfo{
		segments: segments,
		secondaryIndexes: segmentInfoIndexes{
			coll2Segments: map[UniqueID]map[UniqueID]*SegmentInfo{
				collID: segments,
			},
		},
	}

	collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
	collections.Insert(collID, coll)

	s.policy.meta = &meta{
		segments:    segmentsInfo,
		collections: collections,
	}

	// Should only trigger 2 segments due to rate limiting
	views, err := s.policy.triggerOneCollection(ctx, collID, 2)
	s.NoError(err)
	s.Equal(2, len(views))
}

func (s *StorageVersionUpgradePolicySuite) TestTriggerIntervalReset() {
	// Setup params
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.StorageVersionCompactionEnabled.Key, "true")
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.StorageVersionCompactionRateLimitInterval.Key, "1") // 1 second interval
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.StorageVersionCompactionRateLimitTokens.Key, "5")
	defer func() {
		paramtable.Get().Reset(paramtable.Get().DataCoordCfg.StorageVersionCompactionEnabled.Key)
		paramtable.Get().Reset(paramtable.Get().DataCoordCfg.StorageVersionCompactionRateLimitInterval.Key)
		paramtable.Get().Reset(paramtable.Get().DataCoordCfg.StorageVersionCompactionRateLimitTokens.Key)
	}()

	// Mock version manager to return a version that satisfies requirement
	s.versionMgr.EXPECT().GetMinimalSessionVer().Return(semver.MustParse("2.7.0"))

	// Set last period to a time before the interval
	s.policy.lastPeriod = time.Now().Add(-2 * time.Second)
	s.policy.currentCount = 100 // Some high count

	// Trigger should reset the counter because interval has passed
	_, err := s.policy.Trigger(context.Background())
	s.NoError(err)
	s.Equal(0, s.policy.currentCount)
}

func (s *StorageVersionUpgradePolicySuite) TestTriggerCollectionNotFound() {
	ctx := context.Background()
	collID := int64(100)

	coll := &collectionInfo{
		ID:     collID,
		Schema: newTestSchema(),
	}

	// Handler returns nil collection
	s.handler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(nil, nil)

	collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
	collections.Insert(collID, coll)

	s.policy.meta = &meta{
		segments:    NewSegmentsInfo(),
		collections: collections,
	}

	views, err := s.policy.triggerOneCollection(ctx, collID, 10)
	s.NoError(err)
	s.Nil(views)
}

func (s *StorageVersionUpgradePolicySuite) TestTriggerGetCollectionError() {
	ctx := context.Background()
	collID := int64(100)

	coll := &collectionInfo{
		ID:     collID,
		Schema: newTestSchema(),
	}

	// Handler returns error
	s.handler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(nil, context.DeadlineExceeded)

	collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
	collections.Insert(collID, coll)

	s.policy.meta = &meta{
		segments:    NewSegmentsInfo(),
		collections: collections,
	}

	views, err := s.policy.triggerOneCollection(ctx, collID, 10)
	s.Error(err)
	s.Nil(views)
}

func (s *StorageVersionUpgradePolicySuite) TestTriggerAllocIDError() {
	ctx := context.Background()
	collID := int64(100)

	coll := &collectionInfo{
		ID:     collID,
		Schema: newTestSchema(),
	}
	s.handler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(coll, nil)
	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(int64(0), context.DeadlineExceeded)

	collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
	collections.Insert(collID, coll)

	s.policy.meta = &meta{
		segments:    NewSegmentsInfo(),
		collections: collections,
	}

	views, err := s.policy.triggerOneCollection(ctx, collID, 10)
	s.Error(err)
	s.Nil(views)
}

func (s *StorageVersionUpgradePolicySuite) TestTriggerMultipleCollections() {
	ctx := context.Background()

	// Setup params
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.StorageVersionCompactionEnabled.Key, "true")
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.StorageVersionCompactionRateLimitInterval.Key, "1")
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.StorageVersionCompactionRateLimitTokens.Key, "10")
	// paramtable.Get().Save(paramtable.Get().CommonCfg.UseLoonFFI.Key, "true")
	defer func() {
		paramtable.Get().Reset(paramtable.Get().DataCoordCfg.StorageVersionCompactionEnabled.Key)
		paramtable.Get().Reset(paramtable.Get().DataCoordCfg.StorageVersionCompactionRateLimitInterval.Key)
		paramtable.Get().Reset(paramtable.Get().DataCoordCfg.StorageVersionCompactionRateLimitTokens.Key)
		// paramtable.Get().Reset(paramtable.Get().CommonCfg.UseLoonFFI.Key)
	}()

	// Mock version manager to return a version that satisfies requirement
	s.versionMgr.EXPECT().GetMinimalSessionVer().Return(semver.MustParse("2.7.0"))

	coll1 := &collectionInfo{
		ID:     100,
		Schema: newTestSchema(),
	}
	coll2 := &collectionInfo{
		ID:     200,
		Schema: newTestSchema(),
	}
	s.handler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(coll1, nil).Times(1)
	s.handler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(coll2, nil).Times(1)
	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(int64(1000), nil).Times(2)

	// Create segments for collection 100
	segments := make(map[UniqueID]*SegmentInfo)
	segments[101] = &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             101,
			CollectionID:   100,
			PartitionID:    10,
			InsertChannel:  "ch-1",
			Level:          datapb.SegmentLevel_L1,
			State:          commonpb.SegmentState_Flushed,
			NumOfRows:      10000,
			StorageVersion: storage.StorageV1,
		},
	}
	// Create segment for collection 200
	segments[201] = &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             201,
			CollectionID:   200,
			PartitionID:    20,
			InsertChannel:  "ch-2",
			Level:          datapb.SegmentLevel_L1,
			State:          commonpb.SegmentState_Flushed,
			NumOfRows:      10000,
			StorageVersion: storage.StorageV1,
		},
	}

	segmentsInfo := &SegmentsInfo{
		segments: segments,
		secondaryIndexes: segmentInfoIndexes{
			coll2Segments: map[UniqueID]map[UniqueID]*SegmentInfo{
				100: {101: segments[101]},
				200: {201: segments[201]},
			},
		},
	}

	collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
	collections.Insert(100, coll1)
	collections.Insert(200, coll2)

	s.policy.meta = &meta{
		segments:    segmentsInfo,
		collections: collections,
	}

	events, err := s.policy.Trigger(ctx)
	s.NoError(err)
	s.NotNil(events)
	gotViews, ok := events[TriggerTypeStorageVersionUpgrade]
	s.True(ok)
	s.Equal(2, len(gotViews)) // One from each collection
}

func (s *StorageVersionUpgradePolicySuite) TestViewContent() {
	ctx := context.Background()
	collID := int64(100)

	coll := &collectionInfo{
		ID:     collID,
		Schema: newTestSchema(),
	}
	s.handler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(coll, nil)
	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(int64(1000), nil)

	segments := make(map[UniqueID]*SegmentInfo)
	segments[101] = &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             101,
			CollectionID:   collID,
			PartitionID:    10,
			InsertChannel:  "ch-1",
			Level:          datapb.SegmentLevel_L1,
			State:          commonpb.SegmentState_Flushed,
			NumOfRows:      10000,
			StorageVersion: storage.StorageV1,
		},
	}

	segmentsInfo := &SegmentsInfo{
		segments: segments,
		secondaryIndexes: segmentInfoIndexes{
			coll2Segments: map[UniqueID]map[UniqueID]*SegmentInfo{
				collID: segments,
			},
		},
	}

	collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
	collections.Insert(collID, coll)

	s.policy.meta = &meta{
		segments:    segmentsInfo,
		collections: collections,
	}

	views, err := s.policy.triggerOneCollection(ctx, collID, 10)
	s.NoError(err)
	s.Equal(1, len(views))

	// Verify view content
	mixView, ok := views[0].(*MixSegmentView)
	s.True(ok)
	s.NotNil(mixView.GetGroupLabel())
	s.Equal(collID, mixView.GetGroupLabel().CollectionID)
	s.Equal(int64(10), mixView.GetGroupLabel().PartitionID)
	s.Equal("ch-1", mixView.GetGroupLabel().Channel)
	s.Equal(1, len(mixView.GetSegmentsView()))
	s.Equal(int64(101), mixView.GetSegmentsView()[0].ID)
}

func (s *StorageVersionUpgradePolicySuite) TestDroppedSegmentFiltered() {
	ctx := context.Background()
	collID := int64(100)

	// Setup params
	// paramtable.Get().Save(paramtable.Get().CommonCfg.UseLoonFFI.Key, "true")
	// defer paramtable.Get().Reset(paramtable.Get().CommonCfg.UseLoonFFI.Key)

	coll := &collectionInfo{
		ID:     collID,
		Schema: newTestSchema(),
	}
	s.handler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(coll, nil)
	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(int64(1000), nil)

	// Create a dropped segment (should NOT be upgraded)
	segments := make(map[UniqueID]*SegmentInfo)
	segments[101] = &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             101,
			CollectionID:   collID,
			PartitionID:    10,
			InsertChannel:  "ch-1",
			Level:          datapb.SegmentLevel_L1,
			State:          commonpb.SegmentState_Dropped,
			NumOfRows:      10000,
			StorageVersion: storage.StorageV1,
		},
	}

	segmentsInfo := &SegmentsInfo{
		segments: segments,
		secondaryIndexes: segmentInfoIndexes{
			coll2Segments: map[UniqueID]map[UniqueID]*SegmentInfo{
				collID: segments,
			},
		},
	}

	collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
	collections.Insert(collID, coll)

	s.policy.meta = &meta{
		segments:    segmentsInfo,
		collections: collections,
	}

	views, err := s.policy.triggerOneCollection(ctx, collID, 10)
	s.NoError(err)
	s.Equal(0, len(views)) // Dropped segment should not be triggered
}

func (s *StorageVersionUpgradePolicySuite) TestGrowingSegmentFiltered() {
	ctx := context.Background()
	collID := int64(100)

	// Setup params
	// paramtable.Get().Save(paramtable.Get().CommonCfg.UseLoonFFI.Key, "true")
	// defer paramtable.Get().Reset(paramtable.Get().CommonCfg.UseLoonFFI.Key)

	coll := &collectionInfo{
		ID:     collID,
		Schema: newTestSchema(),
	}
	s.handler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(coll, nil)
	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(int64(1000), nil)

	// Create a growing segment (should NOT be upgraded - not flushed)
	segments := make(map[UniqueID]*SegmentInfo)
	segments[101] = &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             101,
			CollectionID:   collID,
			PartitionID:    10,
			InsertChannel:  "ch-1",
			Level:          datapb.SegmentLevel_L1,
			State:          commonpb.SegmentState_Growing,
			NumOfRows:      10000,
			StorageVersion: storage.StorageV1,
		},
	}

	segmentsInfo := &SegmentsInfo{
		segments: segments,
		secondaryIndexes: segmentInfoIndexes{
			coll2Segments: map[UniqueID]map[UniqueID]*SegmentInfo{
				collID: segments,
			},
		},
	}

	collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
	collections.Insert(collID, coll)

	s.policy.meta = &meta{
		segments:    segmentsInfo,
		collections: collections,
	}

	views, err := s.policy.triggerOneCollection(ctx, collID, 10)
	s.NoError(err)
	s.Equal(0, len(views)) // Growing segment should not be triggered
}

func (s *StorageVersionUpgradePolicySuite) TestTriggerSkippedDueToVersionRequirement() {
	ctx := context.Background()
	collID := int64(100)

	// Setup params with a higher version requirement
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.StorageVersionCompactionEnabled.Key, "true")
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.StorageVersionCompactionMinSessionVersion.Key, "2.7.0")
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.StorageVersionCompactionRateLimitInterval.Key, "1")
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.StorageVersionCompactionRateLimitTokens.Key, "10")
	paramtable.Get().Save(paramtable.Get().CommonCfg.UseLoonFFI.Key, "true")
	defer func() {
		paramtable.Get().Reset(paramtable.Get().DataCoordCfg.StorageVersionCompactionEnabled.Key)
		paramtable.Get().Reset(paramtable.Get().DataCoordCfg.StorageVersionCompactionMinSessionVersion.Key)
		paramtable.Get().Reset(paramtable.Get().DataCoordCfg.StorageVersionCompactionRateLimitInterval.Key)
		paramtable.Get().Reset(paramtable.Get().DataCoordCfg.StorageVersionCompactionRateLimitTokens.Key)
		paramtable.Get().Reset(paramtable.Get().CommonCfg.UseLoonFFI.Key)
	}()

	// Mock version manager to return a lower version than requirement
	s.versionMgr.EXPECT().GetMinimalSessionVer().Return(semver.MustParse("2.6.0"))

	coll := &collectionInfo{
		ID:     collID,
		Schema: newTestSchema(),
	}

	// Create a segment that would normally be upgraded
	segments := make(map[UniqueID]*SegmentInfo)
	segments[101] = &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             101,
			CollectionID:   collID,
			PartitionID:    10,
			InsertChannel:  "ch-1",
			Level:          datapb.SegmentLevel_L1,
			State:          commonpb.SegmentState_Flushed,
			NumOfRows:      10000,
			StorageVersion: storage.StorageV2,
		},
	}

	segmentsInfo := &SegmentsInfo{
		segments: segments,
		secondaryIndexes: segmentInfoIndexes{
			coll2Segments: map[UniqueID]map[UniqueID]*SegmentInfo{
				collID: segments,
			},
		},
	}

	collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
	collections.Insert(collID, coll)

	s.policy.meta = &meta{
		segments:    segmentsInfo,
		collections: collections,
	}

	// Should return empty views because version requirement is not met
	events, err := s.policy.Trigger(ctx)
	s.NoError(err)
	s.NotNil(events)
	_, ok := events[TriggerTypeStorageVersionUpgrade]
	s.False(ok)
}

func (s *StorageVersionUpgradePolicySuite) TestTriggerVersionRequirementSatisfied() {
	ctx := context.Background()
	collID := int64(100)

	// Setup params
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.StorageVersionCompactionEnabled.Key, "true")
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.StorageVersionCompactionMinSessionVersion.Key, "2.6.0")
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.StorageVersionCompactionRateLimitInterval.Key, "1")
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.StorageVersionCompactionRateLimitTokens.Key, "10")
	paramtable.Get().Save(paramtable.Get().CommonCfg.UseLoonFFI.Key, "true")
	defer func() {
		paramtable.Get().Reset(paramtable.Get().DataCoordCfg.StorageVersionCompactionEnabled.Key)
		paramtable.Get().Reset(paramtable.Get().DataCoordCfg.StorageVersionCompactionMinSessionVersion.Key)
		paramtable.Get().Reset(paramtable.Get().DataCoordCfg.StorageVersionCompactionRateLimitInterval.Key)
		paramtable.Get().Reset(paramtable.Get().DataCoordCfg.StorageVersionCompactionRateLimitTokens.Key)
		paramtable.Get().Reset(paramtable.Get().CommonCfg.UseLoonFFI.Key)
	}()

	// Mock version manager to return a version that satisfies requirement
	s.versionMgr.EXPECT().GetMinimalSessionVer().Return(semver.MustParse("2.7.0"))

	coll := &collectionInfo{
		ID:     collID,
		Schema: newTestSchema(),
	}
	s.handler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(coll, nil)
	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(int64(1000), nil)

	// Create a segment that should be upgraded
	segments := make(map[UniqueID]*SegmentInfo)
	segments[101] = &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             101,
			CollectionID:   collID,
			PartitionID:    10,
			InsertChannel:  "ch-1",
			Level:          datapb.SegmentLevel_L1,
			State:          commonpb.SegmentState_Flushed,
			NumOfRows:      10000,
			StorageVersion: storage.StorageV2,
		},
	}

	segmentsInfo := &SegmentsInfo{
		segments: segments,
		secondaryIndexes: segmentInfoIndexes{
			coll2Segments: map[UniqueID]map[UniqueID]*SegmentInfo{
				collID: segments,
			},
		},
	}

	collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
	collections.Insert(collID, coll)

	s.policy.meta = &meta{
		segments:    segmentsInfo,
		collections: collections,
	}

	// Should trigger because version requirement is met
	events, err := s.policy.Trigger(ctx)
	s.NoError(err)
	s.NotNil(events)
	gotViews, ok := events[TriggerTypeStorageVersionUpgrade]
	s.True(ok)
	s.Equal(1, len(gotViews)) // Segment should be triggered
}

func (s *StorageVersionUpgradePolicySuite) TestTriggerInvalidVersionRequirement() {
	// Setup params with an invalid version string
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.StorageVersionCompactionMinSessionVersion.Key, "invalid-version")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.StorageVersionCompactionMinSessionVersion.Key)

	// Should return error because version requirement is invalid
	events, err := s.policy.Trigger(context.Background())
	s.Error(err)
	s.Empty(events)
}

func (s *StorageVersionUpgradePolicySuite) TestTriggerVersionExactlyEqual() {
	ctx := context.Background()
	collID := int64(100)

	// Setup params - minVersion equals requirement exactly
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.StorageVersionCompactionEnabled.Key, "true")
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.StorageVersionCompactionMinSessionVersion.Key, "2.6.10")
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.StorageVersionCompactionRateLimitInterval.Key, "1")
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.StorageVersionCompactionRateLimitTokens.Key, "10")
	paramtable.Get().Save(paramtable.Get().CommonCfg.UseLoonFFI.Key, "true")
	defer func() {
		paramtable.Get().Reset(paramtable.Get().DataCoordCfg.StorageVersionCompactionEnabled.Key)
		paramtable.Get().Reset(paramtable.Get().DataCoordCfg.StorageVersionCompactionMinSessionVersion.Key)
		paramtable.Get().Reset(paramtable.Get().DataCoordCfg.StorageVersionCompactionRateLimitInterval.Key)
		paramtable.Get().Reset(paramtable.Get().DataCoordCfg.StorageVersionCompactionRateLimitTokens.Key)
		paramtable.Get().Reset(paramtable.Get().CommonCfg.UseLoonFFI.Key)
	}()

	// Mock version manager to return exact same version as requirement
	s.versionMgr.EXPECT().GetMinimalSessionVer().Return(semver.MustParse("2.6.10"))

	coll := &collectionInfo{
		ID:     collID,
		Schema: newTestSchema(),
	}
	s.handler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(coll, nil)
	s.mockAlloc.EXPECT().AllocID(mock.Anything).Return(int64(1000), nil)

	// Create a segment that should be upgraded
	segments := make(map[UniqueID]*SegmentInfo)
	segments[101] = &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             101,
			CollectionID:   collID,
			PartitionID:    10,
			InsertChannel:  "ch-1",
			Level:          datapb.SegmentLevel_L1,
			State:          commonpb.SegmentState_Flushed,
			NumOfRows:      10000,
			StorageVersion: storage.StorageV2,
		},
	}

	segmentsInfo := &SegmentsInfo{
		segments: segments,
		secondaryIndexes: segmentInfoIndexes{
			coll2Segments: map[UniqueID]map[UniqueID]*SegmentInfo{
				collID: segments,
			},
		},
	}

	collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
	collections.Insert(collID, coll)

	s.policy.meta = &meta{
		segments:    segmentsInfo,
		collections: collections,
	}

	// Should trigger because minVersion equals requirement (not less than)
	events, err := s.policy.Trigger(ctx)
	s.NoError(err)
	s.NotNil(events)
	gotViews, ok := events[TriggerTypeStorageVersionUpgrade]
	s.True(ok)
	s.Equal(1, len(gotViews)) // Segment should be triggered
}

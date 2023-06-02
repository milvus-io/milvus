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
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/commonpbutil"
	"github.com/milvus-io/milvus/internal/util/retry"
	"github.com/milvus-io/milvus/internal/util/trace"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

var (
	// allocPool pool of Allocation, to reduce allocation of Allocation
	allocPool = sync.Pool{
		New: func() interface{} {
			return &Allocation{}
		},
	}
)

// getAllocation unifies way to retrieve allocation struct
func getAllocation(numOfRows int64) *Allocation {
	v := allocPool.Get()
	a, ok := v.(*Allocation)
	if !ok {
		a = &Allocation{}
	}
	if a == nil {
		return &Allocation{
			NumOfRows: numOfRows,
		}
	}
	a.NumOfRows = numOfRows
	a.ExpireTime = 0
	a.SegmentID = 0
	return a
}

// putAllocation puts an allocation for recycling
func putAllocation(a *Allocation) {
	allocPool.Put(a)
}

// Manager manages segment related operations.
type Manager interface {
	// AllocSegment allocates rows and record the allocation.
	AllocSegment(ctx context.Context, collectionID, partitionID UniqueID, channelName string, requestRows int64) ([]*Allocation, error)
	// allocSegmentForImport allocates one segment allocation for bulk insert.
	// TODO: Remove this method and AllocSegment() above instead.
	allocSegmentForImport(ctx context.Context, collectionID, partitionID UniqueID, channelName string, requestRows int64, taskID int64) (*Allocation, error)
	// DropSegment drops the segment from manager.
	DropSegment(ctx context.Context, segmentID UniqueID)
	// SealAllSegments seals all segments of collection with collectionID and return sealed segments.
	// If segIDs is not empty, also seals segments in segIDs.
	SealAllSegments(ctx context.Context, collectionID UniqueID, segIDs []UniqueID, isImport bool) ([]UniqueID, error)
	// GetFlushableSegments returns flushable segment ids
	GetFlushableSegments(ctx context.Context, channel string, ts Timestamp) ([]UniqueID, error)
	// ExpireAllocations notifies segment status to expire old allocations
	ExpireAllocations(channel string, ts Timestamp) error
	// DropSegmentsOfChannel drops all segments in a channel
	DropSegmentsOfChannel(ctx context.Context, channel string)
	Start(ctx context.Context)
	Stop()
}

// Allocation records the allocation info
type Allocation struct {
	SegmentID  UniqueID
	NumOfRows  int64
	ExpireTime Timestamp
}

func (alloc Allocation) String() string {
	t, _ := tsoutil.ParseTS(alloc.ExpireTime)
	return fmt.Sprintf("SegmentID: %d, NumOfRows: %d, ExpireTime: %v", alloc.SegmentID, alloc.NumOfRows, t)
}

// make sure SegmentManager implements Manager
var _ Manager = (*SegmentManager)(nil)

// SegmentManager handles segment related logic
type SegmentManager struct {
	meta                *meta
	mu                  sync.RWMutex
	allocator           allocator
	helper              allocHelper
	segments            []UniqueID
	estimatePolicy      calUpperLimitPolicy
	allocPolicy         AllocatePolicy
	segmentSealPolicies []segmentSealPolicy
	channelSealPolicies []channelSealPolicy
	flushPolicy         flushPolicy
	rcc                 types.RootCoord
	//for global segment last expire
	expireLock                 sync.RWMutex
	globalMaxSegmentLastExpire uint64
	finishedRestartExpireCheck bool
	stopAllocateSegments       *atomic.Bool
	stopper                    context.CancelFunc
	stopWaiter                 sync.WaitGroup
}

type allocHelper struct {
	afterCreateSegment func(segment *datapb.SegmentInfo) error
}

// allocOption allocation option applies to `SegmentManager`
type allocOption interface {
	apply(manager *SegmentManager)
}

// allocFunc function shortcut for allocOption
type allocFunc func(manager *SegmentManager)

// implement allocOption
func (f allocFunc) apply(manager *SegmentManager) {
	f(manager)
}

// get allocOption with allocHelper setting
func withAllocHelper(helper allocHelper) allocOption {
	return allocFunc(func(manager *SegmentManager) { manager.helper = helper })
}

// get default allocHelper, which does nothing
func defaultAllocHelper() allocHelper {
	return allocHelper{
		afterCreateSegment: func(segment *datapb.SegmentInfo) error { return nil },
	}
}

// get allocOption with estimatePolicy
func withCalUpperLimitPolicy(policy calUpperLimitPolicy) allocOption {
	return allocFunc(func(manager *SegmentManager) { manager.estimatePolicy = policy })
}

// get allocOption with allocPolicy
func withAllocPolicy(policy AllocatePolicy) allocOption {
	return allocFunc(func(manager *SegmentManager) { manager.allocPolicy = policy })
}

// get allocOption with segmentSealPolicies
func withSegmentSealPolices(policies ...segmentSealPolicy) allocOption {
	return allocFunc(func(manager *SegmentManager) {
		// do override instead of append, to override default options
		manager.segmentSealPolicies = policies
	})
}

// get allocOption with channelSealPolicies
func withChannelSealPolices(policies ...channelSealPolicy) allocOption {
	return allocFunc(func(manager *SegmentManager) {
		// do override instead of append, to override default options
		manager.channelSealPolicies = policies
	})
}

// get allocOption with flushPolicy
func withFlushPolicy(policy flushPolicy) allocOption {
	return allocFunc(func(manager *SegmentManager) { manager.flushPolicy = policy })
}

func defaultCalUpperLimitPolicy() calUpperLimitPolicy {
	return calBySchemaPolicy
}

func defaultAllocatePolicy() AllocatePolicy {
	return AllocatePolicyV1
}

func defaultSegmentSealPolicy() []segmentSealPolicy {
	return []segmentSealPolicy{
		sealByMaxBinlogFileNumberPolicy(Params.DataCoordCfg.SegmentMaxBinlogFileNumber),
		sealByLifetimePolicy(Params.DataCoordCfg.SegmentMaxLifetime),
		getSegmentCapacityPolicy(Params.DataCoordCfg.SegmentSealProportion),
		sealLongTimeIdlePolicy(Params.DataCoordCfg.SegmentMaxIdleTime, Params.DataCoordCfg.SegmentMinSizeFromIdleToSealed, Params.DataCoordCfg.SegmentMaxSize),
	}
}

func defaultFlushPolicy() flushPolicy {
	return flushPolicyV1
}

// newSegmentManager should be the only way to retrieve SegmentManager.
func newSegmentManager(meta *meta, allocator allocator, rcc types.RootCoord, opts ...allocOption) (*SegmentManager, error) {
	manager := &SegmentManager{
		meta:                       meta,
		allocator:                  allocator,
		helper:                     defaultAllocHelper(),
		segments:                   make([]UniqueID, 0),
		estimatePolicy:             defaultCalUpperLimitPolicy(),
		allocPolicy:                defaultAllocatePolicy(),
		segmentSealPolicies:        defaultSegmentSealPolicy(), // default only segment size policy
		channelSealPolicies:        []channelSealPolicy{},      // no default channel seal policy
		flushPolicy:                defaultFlushPolicy(),
		rcc:                        rcc,
		finishedRestartExpireCheck: false,
		stopAllocateSegments:       atomic.NewBool(true),
	}
	for _, opt := range opts {
		opt.apply(manager)
	}
	if err := manager.loadGlobalSegmentExpire(); err != nil {
		return nil, err
	}
	manager.loadSegmentsFromMeta()
	return manager, nil
}

func (manager *SegmentManager) Start(ctx context.Context) {
	managerCtx, cancelManager := context.WithCancel(ctx)
	manager.stopper = cancelManager
	manager.stopWaiter.Add(1)
	go manager.saveGlobalSegmentExpireLoop(managerCtx)
}

func (manager *SegmentManager) Stop() {
	if manager.stopper != nil {
		manager.stopper()
		manager.stopWaiter.Wait()
	}
}

func (manager *SegmentManager) saveGlobalSegmentExpireLoop(ctx context.Context) {
	ticker := time.NewTicker(Params.DataCoordCfg.GlobalSegmentLastExpireCheckInterval)
	defer ticker.Stop()
	defer manager.stopWaiter.Done()
	var lastSyncGlobalExpirationTime = time.Now().Add(-1 * Params.DataCoordCfg.GlobalSegmentLastExpireSyncInterval)
	for {
		select {
		case <-ctx.Done():
			log.Info("stop saving global segment expire loop")
			manager.maybeSyncGlobalMaxSegmentExpire(&lastSyncGlobalExpirationTime)
			return
		case <-ticker.C:
			if !manager.isFinishedExpireCheck() {
				continue
			}
			if err := manager.maybeSyncGlobalMaxSegmentExpire(&lastSyncGlobalExpirationTime); err != nil {
				log.Error("failed to sync meta, stop allocating segments", zap.Error(err))
				manager.setStoppedAllocation(true)
			} else {
				manager.setStoppedAllocation(false)
			}
		}
	}
}

func (manager *SegmentManager) maybeSyncGlobalMaxSegmentExpire(lastSyncGlobalExpirationTime *time.Time) error {
	sinceLastSyncDuration := time.Since(*lastSyncGlobalExpirationTime)
	currentGlobalMaxLastExpire := manager.getGlobalSegmentExpire()
	if sinceLastSyncDuration >= (Params.DataCoordCfg.GlobalSegmentLastExpireSyncInterval*3)/2 {
		err := errors.New("dangerous! globalSegmentExpire has not been synced to meta store for mor than 2 intervals," +
			"we have to panic dataCoord to avoid data loss")
		log.Fatal(err.Error(),
			zap.Uint64("globalMaxSegmentLastExpire", currentGlobalMaxLastExpire))
		panic(err)
	}
	if sinceLastSyncDuration > Params.DataCoordCfg.GlobalSegmentLastExpireSyncInterval {
		//write global expire kv to etcd, no retry
		currentTsPhyTime := tsoutil.PhysicalTime(currentGlobalMaxLastExpire)
		log.Info("try to sync global last expire to meta store",
			zap.Time("currentGlobalLastExpire", currentTsPhyTime))
		err := manager.meta.SaveGlobalMaxSegmentExpireTs(currentGlobalMaxLastExpire)
		if err != nil {
			log.Error("failed to sync globalSegmentExpire meta store", zap.Error(err))
			return errors.New("dangerous! globalSegmentExpire failed be synced to meta store, " +
				"there is a danger of data loss")
		}
		*lastSyncGlobalExpirationTime = time.Now()
		log.Info("sync global segment last expire ts success",
			zap.Time("currentGlobalLastExpire", currentTsPhyTime))
	}
	return nil
}

func (manager *SegmentManager) saveGlobalSegmentExpire(expireTs uint64) {
	manager.expireLock.Lock()
	defer manager.expireLock.Unlock()
	if expireTs > manager.globalMaxSegmentLastExpire {
		manager.globalMaxSegmentLastExpire = expireTs
	}
}

func (manager *SegmentManager) getGlobalSegmentExpire() uint64 {
	manager.expireLock.RLock()
	defer manager.expireLock.RUnlock()
	return manager.globalMaxSegmentLastExpire
}

func (manager *SegmentManager) isFinishedExpireCheck() bool {
	manager.expireLock.RLock()
	defer manager.expireLock.RUnlock()
	return manager.finishedRestartExpireCheck
}

func (manager *SegmentManager) setFinishedExpireCheck(finished bool) {
	manager.expireLock.Lock()
	defer manager.expireLock.Unlock()
	manager.finishedRestartExpireCheck = finished
}

func (manager *SegmentManager) isStoppedAllocation() bool {
	manager.expireLock.RLock()
	defer manager.expireLock.RUnlock()
	return manager.stopAllocateSegments.Load()
}

func (manager *SegmentManager) setStoppedAllocation(stopped bool) {
	manager.expireLock.Lock()
	defer manager.expireLock.Unlock()
	manager.stopAllocateSegments.Store(stopped)
}

func (manager *SegmentManager) loadGlobalSegmentExpire() error {
	if lastGlobalSegmentExpire, err := manager.meta.GetGlobalMaxSegmentExpire(); err != nil {
		log.Warn("cannot get last global segment expire time, use current ts instead")
		var latestTs uint64 = 0
		allocateErr := retry.Do(context.Background(), func() error {
			tsResponse, tryErr := manager.rcc.AllocTimestamp(context.Background(), &rootcoordpb.AllocTimestampRequest{
				Base: commonpbutil.NewMsgBase(
					commonpbutil.WithMsgType(commonpb.MsgType_RequestTSO),
					commonpbutil.WithMsgID(0),
					commonpbutil.WithTimeStamp(0),
					commonpbutil.WithSourceID(Params.DataCoordCfg.GetNodeID()),
				),
				Count: 1,
			})
			log.Warn("failed to get ts from rootCoord for globalLastExpire", zap.Error(tryErr))
			if tryErr != nil {
				return tryErr
			}
			latestTs = tsResponse.Timestamp
			return nil
		}, retry.Attempts(120), retry.Sleep(1*time.Second))

		if allocateErr != nil {
			log.Warn("cannot allocate latest ts from rootCoord", zap.Error(allocateErr))
			return errors.New("global max expire ts is unavailable for segment manager")
		}
		log.Info("segmentManager use the latest global last expire from rootCoord", zap.Uint64("globalLastExpire", latestTs))
		manager.saveGlobalSegmentExpire(latestTs)
	} else {
		log.Info("segmentManager use global last expire from meta store", zap.Uint64("globalLastExpire", lastGlobalSegmentExpire))
		manager.saveGlobalSegmentExpire(lastGlobalSegmentExpire)
	}
	return nil
}

// loadSegmentsFromMeta generate corresponding segment status for each segment from meta
func (manager *SegmentManager) loadSegmentsFromMeta() {
	segments := manager.meta.GetUnFlushedSegments()
	segmentsID := make([]UniqueID, 0, len(segments))
	for _, segment := range segments {
		segmentsID = append(segmentsID, segment.GetID())
	}
	manager.segments = segmentsID
}

// AllocSegment allocate segment per request collcation, partication, channel and rows
func (manager *SegmentManager) AllocSegment(ctx context.Context, collectionID UniqueID,
	partitionID UniqueID, channelName string, requestRows int64) ([]*Allocation, error) {
	if manager.isStoppedAllocation() {
		return nil, errors.New("segment manager is in " +
			"stoppedAllocate status, cannot allocate segment for the moment")
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()

	// filter segments
	segments := make([]*SegmentInfo, 0)
	for _, segmentID := range manager.segments {
		segment := manager.meta.GetHealthySegment(segmentID)
		if segment == nil {
			log.Warn("Failed to get seginfo from meta", zap.Int64("id", segmentID))
			continue
		}
		if !satisfy(segment, collectionID, partitionID, channelName) || !isGrowing(segment) {
			continue
		}
		segments = append(segments, segment)
	}

	// Apply allocation policy.
	maxCountPerSegment, err := manager.estimateMaxNumOfRows(collectionID)
	if err != nil {
		return nil, err
	}
	newSegmentAllocations, existedSegmentAllocations := manager.allocPolicy(segments,
		requestRows, int64(maxCountPerSegment))

	// create new segments and add allocations
	expireTs, err := manager.genExpireTs(ctx, false)
	if err != nil {
		return nil, err
	}
	for _, allocation := range newSegmentAllocations {
		segment, err := manager.openNewSegment(ctx, collectionID, partitionID, channelName, commonpb.SegmentState_Growing)
		if err != nil {
			return nil, err
		}
		allocation.ExpireTime = expireTs
		allocation.SegmentID = segment.GetID()
		if err := manager.meta.AddAllocation(segment.GetID(), allocation); err != nil {
			return nil, err
		}
	}

	for _, allocation := range existedSegmentAllocations {
		allocation.ExpireTime = expireTs
		if err := manager.meta.AddAllocation(allocation.SegmentID, allocation); err != nil {
			return nil, err
		}
	}
	allocations := append(newSegmentAllocations, existedSegmentAllocations...)
	manager.saveGlobalSegmentExpire(expireTs)
	return allocations, nil
}

// allocSegmentForImport allocates one segment allocation for bulk insert.
func (manager *SegmentManager) allocSegmentForImport(ctx context.Context, collectionID UniqueID,
	partitionID UniqueID, channelName string, requestRows int64, importTaskID int64) (*Allocation, error) {
	sp, _ := trace.StartSpanFromContext(ctx)
	defer sp.Finish()
	manager.mu.Lock()
	defer manager.mu.Unlock()

	// Init allocation.
	allocation := getAllocation(requestRows)
	// Create new segments and add allocations to meta.
	// To avoid mixing up with growing segments, the segment state is "Importing"
	expireTs, err := manager.genExpireTs(ctx, true)
	if err != nil {
		return nil, err
	}

	segment, err := manager.openNewSegment(ctx, collectionID, partitionID, channelName, commonpb.SegmentState_Importing)
	if err != nil {
		return nil, err
	}
	// ReportImport with the new segment so RootCoord can add segment ref lock onto it.
	// TODO: This is a hack and will be removed once the whole ImportManager is migrated from RootCoord to DataCoord.
	if manager.rcc == nil {
		log.Error("RootCoord client not set")
		return nil, errors.New("RootCoord client not set")
	}

	allocation.ExpireTime = expireTs
	allocation.SegmentID = segment.GetID()
	if err := manager.meta.AddAllocation(segment.GetID(), allocation); err != nil {
		return nil, err
	}
	return allocation, nil
}

func satisfy(segment *SegmentInfo, collectionID, partitionID UniqueID, channel string) bool {
	return segment.GetCollectionID() == collectionID && segment.GetPartitionID() == partitionID &&
		segment.GetInsertChannel() == channel
}

func isGrowing(segment *SegmentInfo) bool {
	return segment.GetState() == commonpb.SegmentState_Growing
}

func (manager *SegmentManager) genExpireTs(ctx context.Context, isImport bool) (Timestamp, error) {
	ts, err := manager.allocator.allocTimestamp(ctx)
	if err != nil {
		return 0, err
	}
	physicalTs, logicalTs := tsoutil.ParseTS(ts)
	expirePhysicalTs := physicalTs.Add(time.Duration(Params.DataCoordCfg.SegAssignmentExpiration) * time.Millisecond)
	if isImport {
		// for import, wait until import task expired
		expirePhysicalTs = physicalTs.Add(time.Duration(Params.RootCoordCfg.ImportTaskExpiration) * time.Second)
	}
	expireTs := tsoutil.ComposeTS(expirePhysicalTs.UnixNano()/int64(time.Millisecond), int64(logicalTs))
	return expireTs, nil
}

func (manager *SegmentManager) openNewSegment(ctx context.Context, collectionID UniqueID, partitionID UniqueID,
	channelName string, segmentState commonpb.SegmentState) (*SegmentInfo, error) {
	sp, _ := trace.StartSpanFromContext(ctx)
	defer sp.Finish()
	id, err := manager.allocator.allocID(ctx)
	if err != nil {
		log.Error("failed to open new segment while allocID", zap.Error(err))
		return nil, err
	}
	maxNumOfRows, err := manager.estimateMaxNumOfRows(collectionID)
	if err != nil {
		log.Error("failed to open new segment while estimateMaxNumOfRows", zap.Error(err))
		return nil, err
	}

	segmentInfo := &datapb.SegmentInfo{
		ID:             id,
		CollectionID:   collectionID,
		PartitionID:    partitionID,
		InsertChannel:  channelName,
		NumOfRows:      0,
		State:          segmentState,
		MaxRowNum:      int64(maxNumOfRows),
		LastExpireTime: 0,
	}
	if segmentState == commonpb.SegmentState_Importing {
		segmentInfo.IsImporting = true
	}
	segment := NewSegmentInfo(segmentInfo)
	if err := manager.meta.AddSegment(segment); err != nil {
		log.Error("failed to add segment to DataCoord", zap.Error(err))
		return nil, err
	}
	manager.segments = append(manager.segments, id)
	log.Info("dataCoord: open new segment estimateTotalRows: ",
		zap.Int64("CollectionID", segmentInfo.CollectionID),
		zap.Int64("SegmentID", segmentInfo.ID),
		zap.Int("Rows", maxNumOfRows),
		zap.String("Channel", segmentInfo.InsertChannel))

	return segment, manager.helper.afterCreateSegment(segmentInfo)
}

func (manager *SegmentManager) estimateMaxNumOfRows(collectionID UniqueID) (int, error) {
	// it's ok to use meta.GetCollection here, since collection meta is set before using segmentManager
	collMeta := manager.meta.GetCollection(collectionID)
	if collMeta == nil {
		return -1, fmt.Errorf("failed to get collection %d", collectionID)
	}
	return manager.estimatePolicy(collMeta.Schema)
}

// DropSegment drop the segment from manager.
func (manager *SegmentManager) DropSegment(ctx context.Context, segmentID UniqueID) {
	sp, _ := trace.StartSpanFromContext(ctx)
	defer sp.Finish()
	manager.mu.Lock()
	defer manager.mu.Unlock()
	for i, id := range manager.segments {
		if id == segmentID {
			manager.segments = append(manager.segments[:i], manager.segments[i+1:]...)
			break
		}
	}
	segment := manager.meta.GetHealthySegment(segmentID)
	if segment == nil {
		log.Warn("Failed to get segment", zap.Int64("id", segmentID))
		return
	}
	manager.meta.SetAllocations(segmentID, []*Allocation{})
	for _, allocation := range segment.allocations {
		putAllocation(allocation)
	}
}

// SealAllSegments seals all segments of collection with collectionID and return sealed segments
func (manager *SegmentManager) SealAllSegments(ctx context.Context, collectionID UniqueID, segIDs []UniqueID, isImport bool) ([]UniqueID, error) {
	sp, _ := trace.StartSpanFromContext(ctx)
	defer sp.Finish()
	manager.mu.Lock()
	defer manager.mu.Unlock()
	var ret []UniqueID
	segCandidates := manager.segments
	if len(segIDs) != 0 {
		segCandidates = segIDs
	}
	for _, id := range segCandidates {
		info := manager.meta.GetHealthySegment(id)
		if info == nil {
			log.Warn("failed to get seg info from meta", zap.Int64("segment ID", id))
			continue
		}
		if info.CollectionID != collectionID {
			continue
		}
		if info.State == commonpb.SegmentState_Sealed {
			ret = append(ret, id)
			continue
		}
		if (!isImport && info.State != commonpb.SegmentState_Growing) || (isImport && info.State != commonpb.SegmentState_Importing) {
			continue
		}
		if err := manager.meta.SetState(id, commonpb.SegmentState_Sealed); err != nil {
			return nil, err
		}
		ret = append(ret, id)
	}
	return ret, nil
}

// this function is expected to be called sequentially
func (manager *SegmentManager) segmentServiceAvailable(t Timestamp) bool {
	if manager.isFinishedExpireCheck() {
		return true
	}
	currentGlobalMaxSegmentLastExpire := manager.getGlobalSegmentExpire()
	guardTs := tsoutil.AddPhysicalDurationOnTs(currentGlobalMaxSegmentLastExpire,
		2*Params.DataCoordCfg.GlobalSegmentLastExpireSyncInterval)
	if t > guardTs {
		manager.setFinishedExpireCheck(true)
		manager.setStoppedAllocation(false)
		return true
	}

	return false
}

// GetFlushableSegments get segment ids with Sealed State and flushable (meets flushPolicy)
func (manager *SegmentManager) GetFlushableSegments(ctx context.Context, channel string, t Timestamp) ([]UniqueID, error) {
	if !manager.segmentServiceAvailable(t) {
		return nil, nil
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	sp, _ := trace.StartSpanFromContext(ctx)
	defer sp.Finish()
	// TODO:move tryToSealSegment and dropEmptySealedSegment outside
	if err := manager.tryToSealSegment(t, channel); err != nil {
		return nil, err
	}

	manager.cleanupSealedSegment(t, channel)

	ret := make([]UniqueID, 0, len(manager.segments))
	for _, id := range manager.segments {
		info := manager.meta.GetHealthySegment(id)
		if info == nil || info.InsertChannel != channel {
			continue
		}
		if manager.flushPolicy(info, t) {
			ret = append(ret, id)
		}
	}

	return ret, nil
}

// ExpireAllocations notify segment status to expire old allocations
func (manager *SegmentManager) ExpireAllocations(channel string, ts Timestamp) error {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	for _, id := range manager.segments {
		segment := manager.meta.GetHealthySegment(id)
		if segment == nil || segment.InsertChannel != channel {
			continue
		}
		allocations := make([]*Allocation, 0, len(segment.allocations))
		for i := 0; i < len(segment.allocations); i++ {
			if segment.allocations[i].ExpireTime <= ts {
				a := segment.allocations[i]
				putAllocation(a)
			} else {
				allocations = append(allocations, segment.allocations[i])
			}
		}
		manager.meta.SetAllocations(segment.GetID(), allocations)
	}
	return nil
}

func (manager *SegmentManager) cleanupSealedSegment(ts Timestamp, channel string) {
	valids := make([]int64, 0, len(manager.segments))
	for _, id := range manager.segments {
		segment := manager.meta.GetHealthySegment(id)
		if segment == nil || segment.InsertChannel != channel {
			valids = append(valids, id)
			continue
		}

		if isEmptySealedSegment(segment, ts) {
			log.Info("remove empty sealed segment", zap.Int64("collection", segment.CollectionID), zap.Any("segment", id))
			manager.meta.SetState(id, commonpb.SegmentState_Dropped)
			continue
		}

		if segment.GetState() == commonpb.SegmentState_Importing && segment.GetLastExpireTime() < ts {
			log.Info("cleanup staled importing segment", zap.Int64("collection", segment.CollectionID), zap.Int64("segment", id))
			manager.meta.SetState(id, commonpb.SegmentState_Dropped)
			continue
		}

		valids = append(valids, id)
	}
	manager.segments = valids
}

func isEmptySealedSegment(segment *SegmentInfo, ts Timestamp) bool {
	return segment.GetState() == commonpb.SegmentState_Sealed && segment.GetLastExpireTime() <= ts && segment.currRows == 0
}

// tryToSealSegment applies segment & channel seal policies
func (manager *SegmentManager) tryToSealSegment(ts Timestamp, channel string) error {
	channelInfo := make(map[string][]*SegmentInfo)
	for _, id := range manager.segments {
		info := manager.meta.GetHealthySegment(id)
		if info == nil || info.InsertChannel != channel {
			continue
		}
		channelInfo[info.InsertChannel] = append(channelInfo[info.InsertChannel], info)
		if info.State != commonpb.SegmentState_Growing {
			continue
		}
		// change shouldSeal to segment seal policy logic
		for _, policy := range manager.segmentSealPolicies {
			if policy(info, ts) {
				if err := manager.meta.SetState(id, commonpb.SegmentState_Sealed); err != nil {
					return err
				}
				break
			}
		}
	}
	for channel, segmentInfos := range channelInfo {
		for _, policy := range manager.channelSealPolicies {
			vs := policy(channel, segmentInfos, ts)
			for _, info := range vs {
				if info.State == commonpb.SegmentState_Sealed ||
					info.State == commonpb.SegmentState_Flushing ||
					info.State == commonpb.SegmentState_Flushed {
					continue
				}
				if err := manager.meta.SetState(info.GetID(), commonpb.SegmentState_Sealed); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// DropSegmentsOfChannel drops all segments in a channel
func (manager *SegmentManager) DropSegmentsOfChannel(ctx context.Context, channel string) {
	manager.mu.Lock()
	defer manager.mu.Unlock()

	validSegments := make([]int64, 0, len(manager.segments))
	for _, sid := range manager.segments {
		segment := manager.meta.GetHealthySegment(sid)
		if segment == nil {
			continue
		}
		if segment.GetInsertChannel() != channel {
			validSegments = append(validSegments, sid)
			continue
		}
		manager.meta.SetAllocations(sid, nil)
		for _, allocation := range segment.allocations {
			putAllocation(allocation)
		}
	}

	manager.segments = validSegments
}

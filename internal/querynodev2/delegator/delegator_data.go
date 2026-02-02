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

package delegator

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/cluster"
	"github.com/milvus-io/milvus/internal/querynodev2/delegator/deletebuffer"
	"github.com/milvus-io/milvus/internal/querynodev2/pkoracle"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/conc"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/retry"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// delegator data related part

// InsertData
type InsertData struct {
	RowIDs       []int64
	PrimaryKeys  []storage.PrimaryKey
	Timestamps   []uint64
	InsertRecord *segcorepb.InsertRecord
	BM25Stats    map[int64]*storage.BM25Stats

	StartPosition *msgpb.MsgPosition
	PartitionID   int64
}

type DeleteData struct {
	PartitionID int64
	PrimaryKeys []storage.PrimaryKey
	Timestamps  []uint64
	RowCount    int64
}

// Append appends another delete data into this one.
func (d *DeleteData) Append(ad DeleteData) {
	d.PrimaryKeys = append(d.PrimaryKeys, ad.PrimaryKeys...)
	d.Timestamps = append(d.Timestamps, ad.Timestamps...)
	d.RowCount += ad.RowCount
}

// ProcessInsert handles insert data in delegator.
func (sd *shardDelegator) ProcessInsert(insertRecords map[int64]*InsertData) {
	method := "ProcessInsert"
	tr := timerecord.NewTimeRecorder(method)
	log := sd.getLogger(context.Background())
	for segmentID, insertData := range insertRecords {
		growing := sd.segmentManager.GetGrowing(segmentID)
		newGrowingSegment := false
		if growing == nil {
			var err error
			// TODO: It's a wired implementation that growing segment have load info.
			// we should separate the growing segment and sealed segment by type system.
			growing, err = segments.NewSegment(
				context.Background(),
				sd.collection,
				sd.segmentManager,
				segments.SegmentTypeGrowing,
				0,
				&querypb.SegmentLoadInfo{
					SegmentID:     segmentID,
					PartitionID:   insertData.PartitionID,
					CollectionID:  sd.collectionID,
					InsertChannel: sd.vchannelName,
					StartPosition: insertData.StartPosition,
					DeltaPosition: insertData.StartPosition,
					Level:         datapb.SegmentLevel_L1,
				},
			)
			if err != nil {
				log.Error("failed to create new segment",
					zap.Int64("segmentID", segmentID),
					zap.Error(err))
				panic(err)
			}
			newGrowingSegment = true
		}

		err := growing.Insert(context.Background(), insertData.RowIDs, insertData.Timestamps, insertData.InsertRecord)
		if err != nil {
			log.Error("failed to insert data into growing segment",
				zap.Int64("segmentID", segmentID),
				zap.Error(err),
			)
			if errors.IsAny(err, merr.ErrSegmentNotLoaded, merr.ErrSegmentNotFound) {
				log.Warn("try to insert data into released segment, skip it", zap.Error(err))
				continue
			}
			// panic here, insert failure
			panic(err)
		}
		growing.UpdateBloomFilter(insertData.PrimaryKeys)

		if newGrowingSegment {
			sd.growingSegmentLock.Lock()
			// Forbid create growing segment in excluded segment
			// 	(Now excluded ts may not worked for growing segment with multiple partition.
			//	Because use checkpoint ts as excluded ts when add excluded, but it may less than last message ts.
			//	And cause some invalid message not filtered out and create growing again.
			//	So we forbid all segment in excluded segment create here.)
			// TODO:
			//	Use right ts when add excluded segment. And Verify with insert ts here.
			if ok := sd.VerifyExcludedSegments(segmentID, 0); !ok {
				log.Warn("try to insert data into released segment, skip it", zap.Int64("segmentID", segmentID))
				sd.growingSegmentLock.Unlock()
				growing.Release(context.Background())
				continue
			}

			if !sd.distribution.GrowingSegmentExists(segmentID) {
				// register created growing segment after insert, avoid to add empty growing to delegator
				if sd.idfOracle != nil {
					sd.idfOracle.RegisterGrowing(segmentID, insertData.BM25Stats)
				}
				sd.segmentManager.Put(context.Background(), segments.SegmentTypeGrowing, growing)
				sd.addGrowing(SegmentEntry{
					NodeID:        paramtable.GetNodeID(),
					SegmentID:     segmentID,
					PartitionID:   insertData.PartitionID,
					Version:       0,
					TargetVersion: initialTargetVersion,
					Candidate:     growing, // growing segment itself is the Candidate
				})
			}

			sd.growingSegmentLock.Unlock()
		} else if sd.idfOracle != nil {
			sd.idfOracle.UpdateGrowing(growing.ID(), insertData.BM25Stats)
		}
		log.Info("insert into growing segment",
			zap.Int64("collectionID", growing.Collection()),
			zap.Int64("segmentID", segmentID),
			zap.Int("rowCount", len(insertData.RowIDs)),
			zap.Uint64("maxTimestamp", insertData.Timestamps[len(insertData.Timestamps)-1]),
		)
	}
	metrics.QueryNodeProcessCost.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.InsertLabel).
		Observe(float64(tr.ElapseSpan().Milliseconds()))
}

// ProcessDelete handles delete data in delegator.
// delegator puts deleteData into buffer first,
// then dispatch data to segments according to the result of bloom filter check.
func (sd *shardDelegator) ProcessDelete(deleteData []*DeleteData, ts uint64) {
	// Early return if delegator is stopped - ProcessDelete becomes a no-op
	// This prevents unnecessary processing and side effects during shutdown
	if sd.Stopped() {
		return
	}

	method := "ProcessDelete"
	tr := timerecord.NewTimeRecorder(method)
	// block load segment handle delete buffer
	sd.deleteMut.Lock()
	defer sd.deleteMut.Unlock()

	log := sd.getLogger(context.Background())

	log.Debug("start to process delete", zap.Uint64("ts", ts))
	// add deleteData into buffer.
	cacheItems := make([]deletebuffer.BufferItem, 0, len(deleteData))
	for _, entry := range deleteData {
		cacheItems = append(cacheItems, deletebuffer.BufferItem{
			PartitionID: entry.PartitionID,
			DeleteData: storage.DeleteData{
				Pks:      entry.PrimaryKeys,
				Tss:      entry.Timestamps,
				RowCount: entry.RowCount,
			},
		})
	}

	sd.deleteBuffer.Put(&deletebuffer.Item{
		Ts:   ts,
		Data: cacheItems,
	})

	sd.forwardStreamingDeletion(context.Background(), deleteData)

	metrics.QueryNodeProcessCost.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), metrics.DeleteLabel).
		Observe(float64(tr.ElapseSpan().Milliseconds()))
}

type BatchApplyRet = struct {
	DeleteDataIdx int
	StartIdx      int
	Segment2Hits  map[int64][]bool
}

// applyBFInParallel applies bloom filter check in parallel using distribution's BatchGet.
// IMPORTANT: Caller must ensure segments are protected (e.g., via Pin) during this call,
// otherwise candidates may be released concurrently leading to use-after-free.
func (sd *shardDelegator) applyBFInParallel(deleteDatas []*DeleteData, pool *conc.Pool[any]) *typeutil.ConcurrentMap[int, *BatchApplyRet] {
	retIdx := 0
	retMap := typeutil.NewConcurrentMap[int, *BatchApplyRet]()
	batchSize := paramtable.Get().CommonCfg.BloomFilterApplyBatchSize.GetAsInt()

	var futures []*conc.Future[any]
	for didx, data := range deleteDatas {
		pks := data.PrimaryKeys
		for idx := 0; idx < len(pks); idx += batchSize {
			startIdx := idx
			endIdx := startIdx + batchSize
			if endIdx > len(pks) {
				endIdx = len(pks)
			}

			retIdx += 1
			tmpRetIndex := retIdx
			deleteDataId := didx
			partitionID := data.PartitionID
			future := pool.Submit(func() (any, error) {
				ret := sd.distribution.BatchGet(pks[startIdx:endIdx], partitionID)
				retMap.Insert(tmpRetIndex, &BatchApplyRet{
					DeleteDataIdx: deleteDataId,
					StartIdx:      startIdx,
					Segment2Hits:  ret,
				})
				return nil, nil
			})
			futures = append(futures, future)
		}
	}
	conc.AwaitAll(futures...)

	return retMap
}

// applyDelete handles delete record and apply them to corresponding workers.
func (sd *shardDelegator) applyDelete(ctx context.Context,
	nodeID int64,
	worker cluster.Worker,
	delRecords func(segmentID int64) (DeleteData, bool),
	entries []SegmentEntry,
	scope querypb.DataScope,
) []int64 {
	offlineSegments := typeutil.NewConcurrentSet[int64]()
	log := sd.getLogger(ctx)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	pool := conc.NewPool[struct{}](runtime.GOMAXPROCS(0) * 4)
	defer pool.Release()

	var futures []*conc.Future[struct{}]
	for _, segmentEntry := range entries {
		segmentEntry := segmentEntry
		delRecord, ok := delRecords(segmentEntry.SegmentID)
		log := log.With(
			zap.Int64("segmentID", segmentEntry.SegmentID),
			zap.Int64("workerID", nodeID),
			zap.Int("forwardRowCount", len(delRecord.PrimaryKeys)),
		)
		if ok {
			future := pool.Submit(func() (struct{}, error) {
				log.Debug("delegator plan to applyDelete via worker")
				err := retry.Handle(ctx, func() (bool, error) {
					if sd.Stopped() {
						return false, merr.WrapErrChannelNotAvailable(sd.vchannelName, "channel is unsubscribing")
					}

					err := worker.Delete(ctx, &querypb.DeleteRequest{
						Base:         commonpbutil.NewMsgBase(commonpbutil.WithTargetID(nodeID)),
						CollectionId: sd.collectionID,
						PartitionId:  segmentEntry.PartitionID,
						VchannelName: sd.vchannelName,
						SegmentId:    segmentEntry.SegmentID,
						PrimaryKeys:  storage.ParsePrimaryKeys2IDs(delRecord.PrimaryKeys),
						Timestamps:   delRecord.Timestamps,
						Scope:        scope,
					})
					if errors.Is(err, merr.ErrNodeNotFound) {
						log.Warn("try to delete data on non-exist node")
						// cancel other request
						cancel()
						return false, err
					} else if errors.IsAny(err, merr.ErrSegmentNotFound, merr.ErrSegmentNotLoaded) {
						log.Warn("try to delete data of released segment")
						return false, nil
					} else if err != nil {
						log.Warn("worker failed to delete on segment", zap.Error(err))
						return true, err
					}
					return false, nil
				}, retry.Attempts(10))
				if err != nil {
					log.Warn("apply delete for segment failed, marking it offline")
					offlineSegments.Insert(segmentEntry.SegmentID)
				}
				return struct{}{}, err
			})
			futures = append(futures, future)
		}
	}
	conc.AwaitAll(futures...)
	return offlineSegments.Collect()
}

// markSegmentOffline makes segment go offline and waits for QueryCoord to fix.
func (sd *shardDelegator) markSegmentOffline(segmentIDs ...int64) {
	sd.distribution.MarkOfflineSegments(segmentIDs...)
}

// addGrowing add growing segment record for delegator.
func (sd *shardDelegator) addGrowing(entries ...SegmentEntry) {
	log := sd.getLogger(context.Background())
	log.Info("add growing segments to delegator", zap.Int64s("segmentIDs", lo.Map(entries, func(entry SegmentEntry, _ int) int64 {
		return entry.SegmentID
	})))
	sd.distribution.AddGrowing(entries...)
}

// LoadGrowing load growing segments locally.
func (sd *shardDelegator) LoadGrowing(ctx context.Context, infos []*querypb.SegmentLoadInfo, version int64) error {
	log := sd.getLogger(ctx)

	segmentIDs := lo.Map(infos, func(info *querypb.SegmentLoadInfo, _ int) int64 { return info.GetSegmentID() })
	log.Info("loading growing segments...", zap.Int64s("segmentIDs", segmentIDs))
	loaded, err := sd.loader.Load(ctx, sd.collectionID, segments.SegmentTypeGrowing, version, infos...)
	if err != nil {
		log.Warn("failed to load growing segment", zap.Error(err))
		return err
	}

	for _, segment := range loaded {
		err = sd.addL0ForGrowing(ctx, segment)
		if err != nil {
			log.Warn("failed to forward L0 deletions to growing segment",
				zap.Error(err),
			)

			// clear loaded growing segments
			for _, segment := range loaded {
				segment.Release(ctx)
			}
			return err
		}
	}

	segmentIDs = lo.Map(loaded, func(segment segments.Segment, _ int) int64 { return segment.ID() })
	log.Info("load growing segments done", zap.Int64s("segmentIDs", segmentIDs))

	for _, segment := range loaded {
		if sd.idfOracle != nil {
			sd.idfOracle.RegisterGrowing(segment.ID(), segment.GetBM25Stats())
		}
	}
	sd.addGrowing(lo.Map(loaded, func(segment segments.Segment, _ int) SegmentEntry {
		return SegmentEntry{
			NodeID:        paramtable.GetNodeID(),
			SegmentID:     segment.ID(),
			PartitionID:   segment.Partition(),
			Version:       version,
			TargetVersion: sd.distribution.getTargetVersion(),
			Candidate:     segment, // growing segment itself is the Candidate
		}
	})...)
	return nil
}

// load bm25 stats for sealed segments
func (sd *shardDelegator) loadBM25Stats(ctx context.Context, infos []*querypb.SegmentLoadInfo, req *querypb.LoadSegmentsRequest) error {
	if sd.idfOracle == nil {
		return nil
	}

	pool := segments.GetBM25LoadPool()

	future := pool.Submit(func() (any, error) {
		bm25Stats, err := sd.loader.LoadBM25Stats(ctx, req.GetCollectionID(), infos...)
		if err != nil {
			log.Warn("failed to load bm25 stats for segment", zap.Int64("collectionID", req.GetCollectionID()), zap.Error(err))
			return nil, err
		}

		if bm25Stats != nil {
			bm25Stats.Range(func(segmentID int64, stats map[int64]*storage.BM25Stats) bool {
				log.Info("register sealed segment bm25 stats into idforacle",
					zap.Int64("segmentID", segmentID),
				)
				err = sd.idfOracle.RegisterSealed(segmentID, stats)
				if err != nil {
					log.Warn("failed to register sealed segment bm25 stats into idforacle", zap.Error(err))
					return false
				}
				return true
			})

			if err != nil {
				log.Warn("failed to register sealed segment bm25 stats into idforacle", zap.Error(err))
				return nil, err
			}
		}

		return nil, nil
	})

	err := conc.BlockOnAll(future)
	if err != nil {
		log.Warn("failed to load bm25 stats", zap.Error(err))
		return err
	}
	return nil
}

// LoadSegments load segments local or remotely depends on the target node.
func (sd *shardDelegator) LoadSegments(ctx context.Context, req *querypb.LoadSegmentsRequest) error {
	if len(req.GetInfos()) == 0 {
		return nil
	}

	log := sd.getLogger(ctx)

	targetNodeID := req.GetDstNodeID()
	// add common log fields
	log = log.With(
		zap.Int64("workID", targetNodeID),
		zap.Int64s("segments", lo.Map(req.GetInfos(), func(info *querypb.SegmentLoadInfo, _ int) int64 { return info.GetSegmentID() })),
	)

	if req.GetInfos()[0].GetLevel() == datapb.SegmentLevel_L0 {
		return merr.WrapErrServiceInternal("load L0 segment is not supported, l0 segment should only be loaded by watchChannel")
	}

	// pin all segments to prevent delete buffer has been cleaned up during worker load segments
	// Note: if delete records is pinned, it will skip cleanup during SyncTargetVersion
	// which means after segment is loaded, then delete buffer will be cleaned up by next SyncTargetVersion call
	for _, info := range req.GetInfos() {
		sd.deleteBuffer.Pin(info.GetStartPosition().GetTimestamp(), info.GetSegmentID())
	}
	defer func() {
		for _, info := range req.GetInfos() {
			sd.deleteBuffer.Unpin(info.GetStartPosition().GetTimestamp(), info.GetSegmentID())
		}
	}()

	worker, err := sd.workerManager.GetWorker(ctx, targetNodeID)
	if err != nil {
		log.Warn("delegator failed to find worker", zap.Error(err))
		return err
	}

	req.Base.TargetID = targetNodeID
	log.Debug("worker loads segments...")

	sLoad := func(ctx context.Context, req *querypb.LoadSegmentsRequest) error {
		segmentID := req.GetInfos()[0].GetSegmentID()
		nodeID := req.GetDstNodeID()
		_, err, _ := sd.sf.Do(fmt.Sprintf("%d-%d", nodeID, segmentID), func() (struct{}, error) {
			err := worker.LoadSegments(ctx, req)
			return struct{}{}, err
		})
		return err
	}

	// separate infos into different load task
	if len(req.GetInfos()) > 1 {
		var reqs []*querypb.LoadSegmentsRequest
		for _, info := range req.GetInfos() {
			newReq := typeutil.Clone(req)
			newReq.Infos = []*querypb.SegmentLoadInfo{info}
			reqs = append(reqs, newReq)
		}

		group, ctx := errgroup.WithContext(ctx)
		for _, req := range reqs {
			req := req
			group.Go(func() error {
				return sLoad(ctx, req)
			})
		}
		err = group.Wait()
	} else {
		err = sLoad(ctx, req)
	}

	if err != nil {
		log.Warn("worker failed to load segments", zap.Error(err))
		return err
	}
	log.Debug("work loads segments done")

	// load index segment need no stream delete and distribution change
	if req.GetLoadScope() == querypb.LoadScope_Index {
		return nil
	}

	infos := lo.Filter(req.GetInfos(), func(info *querypb.SegmentLoadInfo, _ int) bool {
		return !sd.distribution.SealedSegmentExistsOnNode(info.GetSegmentID(), targetNodeID)
	})

	candidates, err := sd.loader.LoadBloomFilterSet(ctx, req.GetCollectionID(), infos...)
	if err != nil {
		log.Warn("failed to load bloom filter set for segment", zap.Error(err))
		return err
	}

	log.Debug("load delete...")
	err = sd.loadStreamDelete(ctx, candidates, infos, req, targetNodeID, worker)
	if err != nil {
		log.Warn("load stream delete failed", zap.Error(err))
		return err
	}

	err = sd.loadBM25Stats(ctx, infos, req)
	if err != nil {
		log.Warn("failed to load BM25 stats", zap.Error(err))
		return err
	}

	// Build a map from segmentID to BloomFilterSet
	bfMap := make(map[int64]pkoracle.Candidate)
	for _, candidate := range candidates {
		log.Info("loaded bloom filter set for sealed segment",
			zap.Int64("segmentID", candidate.ID()),
		)
		bfMap[candidate.ID()] = candidate
	}

	// Build entries with Candidate - use filtered infos instead of req.GetInfos()
	// This ensures we only add entries for segments that were actually loaded (not skipped as duplicates)
	entries := make([]SegmentEntry, 0, len(infos))
	for _, info := range infos {
		entry := SegmentEntry{
			SegmentID:   info.GetSegmentID(),
			PartitionID: info.GetPartitionID(),
			NodeID:      req.GetDstNodeID(),
			Version:     req.GetVersion(),
			Level:       info.GetLevel(),
			Candidate:   bfMap[info.GetSegmentID()],
		}
		entries = append(entries, entry)
	}

	return sd.addDistributionIfVersionOK(req.GetLoadMeta().GetSchemaVersion(), entries...)
}

func (sd *shardDelegator) addDistributionIfVersionOK(version uint64, entries ...SegmentEntry) error {
	sd.schemaChangeMutex.Lock()
	defer sd.schemaChangeMutex.Unlock()
	if version < sd.schemaVersion {
		return merr.WrapErrServiceInternal("schema version changed")
	}

	// alter distribution
	sd.distribution.AddDistributions(entries...)
	return nil
}

// LoadGrowing load growing segments locally.
func (sd *shardDelegator) LoadL0(ctx context.Context, infos []*querypb.SegmentLoadInfo, version int64) error {
	log := sd.getLogger(ctx)

	segmentIDs := lo.Map(infos, func(info *querypb.SegmentLoadInfo, _ int) int64 { return info.GetSegmentID() })
	log.Info("loading l0 segments...", zap.Int64s("segmentIDs", segmentIDs))

	loaded := make([]segments.Segment, 0)
	if sd.l0ForwardPolicy == L0ForwardPolicyRemoteLoad {
		for _, info := range infos {
			l0Seg, err := segments.NewL0Segment(sd.collection, segments.SegmentTypeSealed, version, info)
			if err != nil {
				return err
			}
			loaded = append(loaded, l0Seg)
		}
	} else {
		var err error
		loaded, err = sd.loader.Load(ctx, sd.collectionID, segments.SegmentTypeSealed, version, infos...)
		if err != nil {
			log.Warn("failed to load l0 segment", zap.Error(err))
			return err
		}
	}

	segmentIDs = lo.Map(loaded, func(segment segments.Segment, _ int) int64 { return segment.ID() })
	log.Info("load l0 segments done", zap.Int64s("segmentIDs", segmentIDs))

	sd.deleteBuffer.RegisterL0(loaded...)
	// register l0 segment
	sd.RefreshLevel0DeletionStats()
	return nil
}

func (sd *shardDelegator) rangeHitL0Deletions(partitionID int64, candidate pkoracle.Candidate, fn func(pk storage.PrimaryKey, ts uint64) error) error {
	level0Segments := sd.deleteBuffer.ListL0()

	if len(level0Segments) == 0 {
		return nil
	}

	log := sd.getLogger(context.Background())
	start := time.Now()
	totalL0Rows := 0
	totalBfHitRows := int64(0)
	processedL0Count := 0

	for _, segment := range level0Segments {
		segment := segment.(*segments.L0Segment)
		if segment.Partition() == partitionID || segment.Partition() == common.AllPartitionsID {
			segmentPks, segmentTss := segment.DeleteRecords()
			totalL0Rows += len(segmentPks)
			processedL0Count++
			batchSize := paramtable.Get().CommonCfg.BloomFilterApplyBatchSize.GetAsInt()

			for idx := 0; idx < len(segmentPks); idx += batchSize {
				endIdx := idx + batchSize
				if endIdx > len(segmentPks) {
					endIdx = len(segmentPks)
				}

				lc := storage.NewBatchLocationsCache(segmentPks[idx:endIdx])
				hits := candidate.BatchPkExist(lc)
				for i, hit := range hits {
					if hit {
						totalBfHitRows += 1
						if err := fn(segmentPks[idx+i], segmentTss[idx+i]); err != nil {
							return err
						}
					}
				}
			}
		}
	}

	log.Info("forward delete from L0 segments to worker",
		zap.Int64("targetSegmentID", candidate.ID()),
		zap.String("channel", sd.vchannelName),
		zap.Int("l0SegmentCount", processedL0Count),
		zap.Int("totalDeleteRowsInL0", totalL0Rows),
		zap.Int64("totalBfHitRows", totalBfHitRows),
		zap.Int64("totalCost", time.Since(start).Milliseconds()),
	)

	return nil
}

func (sd *shardDelegator) GetLevel0Deletions(partitionID int64, candidate pkoracle.Candidate) (storage.PrimaryKeys, []storage.Timestamp) {
	deltaData := storage.NewDeltaData(0)

	sd.rangeHitL0Deletions(partitionID, candidate, func(pk storage.PrimaryKey, ts uint64) error {
		deltaData.Append(pk, ts)
		return nil
	})

	return deltaData.DeletePks(), deltaData.DeleteTimestamps()
}

func (sd *shardDelegator) StreamForwardLevel0Deletions(bufferedForwarder *BufferForwarder, partitionID int64, candidate pkoracle.Candidate) error {
	err := sd.rangeHitL0Deletions(partitionID, candidate, func(pk storage.PrimaryKey, ts uint64) error {
		return bufferedForwarder.Buffer(pk, ts)
	})
	if err != nil {
		return err
	}
	return bufferedForwarder.Flush()
}

func (sd *shardDelegator) RefreshLevel0DeletionStats() {
	level0Segments := sd.deleteBuffer.ListL0()
	totalSize := int64(0)
	for _, segment := range level0Segments {
		segment := segment.(*segments.L0Segment)
		pks, tss := segment.DeleteRecords()
		totalSize += lo.SumBy(pks, func(pk storage.PrimaryKey) int64 { return pk.Size() }) + int64(len(tss)*8)
	}

	metrics.QueryNodeNumSegments.WithLabelValues(
		fmt.Sprint(paramtable.GetNodeID()),
		fmt.Sprint(sd.Collection()),
		commonpb.SegmentState_Sealed.String(),
		datapb.SegmentLevel_L0.String(),
	).Set(float64(len(level0Segments)))

	metrics.QueryNodeLevelZeroSize.WithLabelValues(
		fmt.Sprint(paramtable.GetNodeID()),
		fmt.Sprint(sd.collectionID),
		sd.vchannelName,
	).Set(float64(totalSize))
}

func (sd *shardDelegator) loadStreamDelete(ctx context.Context,
	candidates []*pkoracle.BloomFilterSet,
	infos []*querypb.SegmentLoadInfo,
	req *querypb.LoadSegmentsRequest,
	targetNodeID int64,
	worker cluster.Worker,
) error {
	log := sd.getLogger(ctx)

	idCandidates := lo.SliceToMap(candidates, func(candidate *pkoracle.BloomFilterSet) (int64, *pkoracle.BloomFilterSet) {
		return candidate.ID(), candidate
	})
	for _, info := range infos {
		candidate := idCandidates[info.GetSegmentID()]
		// forward l0 deletion
		err := sd.forwardL0Deletion(ctx, info, req, candidate, targetNodeID, worker)
		if err != nil {
			return err
		}
	}

	sd.deleteMut.RLock()
	defer sd.deleteMut.RUnlock()
	// apply buffered delete for new segments
	// no goroutines here since qnv2 has no load merging logic
	for _, info := range infos {
		candidate := idCandidates[info.GetSegmentID()]
		// after L0 segment feature
		// growing segemnts should have load stream delete as well
		deleteScope := querypb.DataScope_All
		switch candidate.Type() {
		case commonpb.SegmentState_Sealed:
			deleteScope = querypb.DataScope_Historical
		case commonpb.SegmentState_Growing:
			deleteScope = querypb.DataScope_Streaming
		}

		bufferedForwarder := NewBufferedForwarder(paramtable.Get().QueryNodeCfg.ForwardBatchSize.GetAsInt64(),
			deleteViaWorker(ctx, worker, targetNodeID, info, deleteScope))

		// list buffered delete
		deleteRecords := sd.deleteBuffer.ListAfter(info.GetStartPosition().GetTimestamp())
		tsHitDeleteRows := int64(0)
		bfHitDeleteRows := int64(0)
		start := time.Now()
		for _, entry := range deleteRecords {
			for _, record := range entry.Data {
				tsHitDeleteRows += int64(len(record.DeleteData.Pks))
				if record.PartitionID != common.AllPartitionsID && candidate.Partition() != record.PartitionID {
					continue
				}
				pks := record.DeleteData.Pks
				batchSize := paramtable.Get().CommonCfg.BloomFilterApplyBatchSize.GetAsInt()
				for idx := 0; idx < len(pks); idx += batchSize {
					endIdx := idx + batchSize
					if endIdx > len(pks) {
						endIdx = len(pks)
					}

					lc := storage.NewBatchLocationsCache(pks[idx:endIdx])
					hits := candidate.BatchPkExist(lc)
					for i, hit := range hits {
						if hit {
							bfHitDeleteRows += 1
							err := bufferedForwarder.Buffer(pks[idx+i], record.DeleteData.Tss[idx+i])
							if err != nil {
								return err
							}
						}
					}
				}
			}
		}
		log.Info("forward delete to worker...",
			zap.String("channel", info.InsertChannel),
			zap.Int64("segmentID", info.GetSegmentID()),
			zap.Time("startPosition", tsoutil.PhysicalTime(info.GetStartPosition().GetTimestamp())),
			zap.Int64("tsHitDeleteRowNum", tsHitDeleteRows),
			zap.Int64("bfHitDeleteRowNum", bfHitDeleteRows),
			zap.Int64("bfCost", time.Since(start).Milliseconds()),
		)
		err := bufferedForwarder.Flush()
		if err != nil {
			return err
		}
	}
	log.Info("load delete done")
	return nil
}

// ReleaseSegments releases segments local or remotely depending on the target node.
func (sd *shardDelegator) ReleaseSegments(ctx context.Context, req *querypb.ReleaseSegmentsRequest, force bool) error {
	log := sd.getLogger(ctx)

	targetNodeID := req.GetNodeID()
	level0Segments := typeutil.NewSet(lo.Map(sd.deleteBuffer.ListL0(), func(segment segments.Segment, _ int) int64 {
		return segment.ID()
	})...)
	hasLevel0 := false
	for _, segmentID := range req.GetSegmentIDs() {
		hasLevel0 = level0Segments.Contain(segmentID)
		if hasLevel0 {
			return merr.WrapErrServiceInternal("release L0 segment is not supported, l0 segment should only be released by unSubChannel/SyncDataDistribution")
		}
	}

	// add common log fields
	log = log.With(
		zap.Int64s("segmentIDs", req.GetSegmentIDs()),
		zap.Int64("nodeID", req.GetNodeID()),
		zap.String("scope", req.GetScope().String()),
		zap.Bool("force", force))

	log.Info("delegator start to release segments")
	// alter distribution first
	var sealed, growing []SegmentEntry
	convertSealed := func(segmentID int64, _ int) SegmentEntry {
		return SegmentEntry{
			SegmentID: segmentID,
			NodeID:    targetNodeID,
		}
	}
	convertGrowing := func(segmentID int64, _ int) SegmentEntry {
		return SegmentEntry{
			SegmentID: segmentID,
		}
	}
	switch req.GetScope() {
	case querypb.DataScope_All:
		sealed = lo.Map(req.GetSegmentIDs(), convertSealed)
		growing = lo.Map(req.GetSegmentIDs(), convertGrowing)
	case querypb.DataScope_Streaming:
		growing = lo.Map(req.GetSegmentIDs(), convertGrowing)
	case querypb.DataScope_Historical:
		sealed = lo.Map(req.GetSegmentIDs(), convertSealed)
	}
	signal := sd.distribution.RemoveDistributions(sealed, growing)
	// wait cleared signal
	<-signal

	if len(growing) > 0 {
		sd.growingSegmentLock.Lock()
	}
	// when we try to release a segment, add it to pipeline's exclude list first
	// in case of consumed it's growing segment again
	droppedInfos := lo.SliceToMap(req.GetSegmentIDs(), func(id int64) (int64, uint64) {
		if req.GetCheckpoint() == nil {
			return id, typeutil.MaxTimestamp
		}

		return id, req.GetCheckpoint().GetTimestamp()
	})
	sd.AddExcludedSegments(droppedInfos)

	// Note: Candidate cleanup is handled by RemoveDistributions above
	// - Sealed segment candidates (BloomFilterSet) are refunded in RemoveDistributions
	// - Growing segment candidates (LocalSegment) are managed by segmentManager.Release()

	var releaseErr error
	if !force {
		worker, err := sd.workerManager.GetWorker(ctx, targetNodeID)
		if err != nil {
			log.Warn("delegator failed to find worker", zap.Error(err))
			releaseErr = err
		}
		req.Base.TargetID = targetNodeID
		err = worker.ReleaseSegments(ctx, req)
		if err != nil {
			log.Warn("worker failed to release segments", zap.Error(err))
			releaseErr = err
		}
	}
	if len(growing) > 0 {
		sd.growingSegmentLock.Unlock()
	}

	if releaseErr != nil {
		return releaseErr
	}
	return nil
}

func (sd *shardDelegator) SyncTargetVersion(action *querypb.SyncAction, partitions []int64) {
	sd.distribution.SyncTargetVersion(action, partitions)
	// clean delete buffer after distribution becomes serviceable
	if sd.distribution.queryView.Serviceable() {
		checkpoint := action.GetCheckpoint()
		deleteSeekPos := action.GetDeleteCP()
		if deleteSeekPos == nil {
			// for compatible with 2.4, we use checkpoint as deleteCP when deleteCP is nil
			deleteSeekPos = checkpoint
			log.Info("use checkpoint as deleteCP",
				zap.String("channelName", sd.vchannelName),
				zap.Time("deleteSeekPos", tsoutil.PhysicalTime(action.GetCheckpoint().GetTimestamp())))
		}

		start := time.Now()
		sizeBeforeClean, _ := sd.deleteBuffer.Size()
		l0NumBeforeClean := len(sd.deleteBuffer.ListL0())
		sd.deleteBuffer.UnRegister(deleteSeekPos.GetTimestamp())
		sizeAfterClean, _ := sd.deleteBuffer.Size()
		l0NumAfterClean := len(sd.deleteBuffer.ListL0())

		if sizeAfterClean < sizeBeforeClean || l0NumAfterClean < l0NumBeforeClean {
			log.Info("clean delete buffer",
				zap.String("channel", sd.vchannelName),
				zap.Time("deleteSeekPos", tsoutil.PhysicalTime(deleteSeekPos.GetTimestamp())),
				zap.Time("channelCP", tsoutil.PhysicalTime(checkpoint.GetTimestamp())),
				zap.Int64("sizeBeforeClean", sizeBeforeClean),
				zap.Int64("sizeAfterClean", sizeAfterClean),
				zap.Int("l0NumBeforeClean", l0NumBeforeClean),
				zap.Int("l0NumAfterClean", l0NumAfterClean),
				zap.Duration("cost", time.Since(start)),
			)
		}
		sd.RefreshLevel0DeletionStats()
	}
}

func (sd *shardDelegator) GetChannelQueryView() *channelQueryView {
	return sd.distribution.GetQueryView()
}

func (sd *shardDelegator) AddExcludedSegments(excludeInfo map[int64]uint64) {
	sd.excludedSegments.Insert(excludeInfo)
}

func (sd *shardDelegator) VerifyExcludedSegments(segmentID int64, ts uint64) bool {
	return sd.excludedSegments.Verify(segmentID, ts)
}

func (sd *shardDelegator) TryCleanExcludedSegments(ts uint64) {
	if sd.excludedSegments.ShouldClean() {
		sd.excludedSegments.CleanInvalid(ts)
	}
}

func (sd *shardDelegator) buildBM25IDF(req *internalpb.SearchRequest) (float64, error) {
	pb := &commonpb.PlaceholderGroup{}
	proto.Unmarshal(req.GetPlaceholderGroup(), pb)

	if len(pb.Placeholders) != 1 || len(pb.Placeholders[0].Values) == 0 {
		return 0, merr.WrapErrParameterInvalidMsg("please provide varchar/text for BM25 Function based search")
	}

	holder := pb.Placeholders[0]
	if holder.Type != commonpb.PlaceholderType_VarChar {
		return 0, merr.WrapErrParameterInvalidMsg(fmt.Sprintf("please provide varchar/text for BM25 Function based search, got %s", holder.Type.String()))
	}

	texts := funcutil.GetVarCharFromPlaceholder(holder)
	datas := []any{texts}
	functionRunner, ok := sd.functionRunners[req.GetFieldId()]
	if !ok {
		return 0, fmt.Errorf("functionRunner not found for field: %d", req.GetFieldId())
	}

	if len(functionRunner.GetInputFields()) == 2 {
		analyzerName := "default"
		if name := req.GetAnalyzerName(); name != "" {
			// use user provided analyzer name
			analyzerName = name
		}

		analyzers := make([]string, len(texts))
		for i := range texts {
			analyzers[i] = analyzerName
		}
		datas = append(datas, analyzers)
	}

	// get search text term frequency
	output, err := functionRunner.BatchRun(datas...)
	if err != nil {
		return 0, err
	}

	tfArray, ok := output[0].(*schemapb.SparseFloatArray)
	if !ok {
		return 0, errors.New("functionRunner return unknown data")
	}

	idfSparseVector, avgdl, err := sd.idfOracle.BuildIDF(req.GetFieldId(), tfArray)
	if err != nil {
		return 0, err
	}

	if avgdl <= 0 {
		return 0, nil
	}

	for _, idf := range idfSparseVector {
		metrics.QueryNodeSearchFTSNumTokens.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), fmt.Sprint(sd.collectionID), fmt.Sprint(req.GetFieldId())).Observe(float64(typeutil.SparseFloatRowElementCount(idf)))
	}

	err = SetBM25Params(req, avgdl)
	if err != nil {
		return 0, err
	}

	req.PlaceholderGroup = funcutil.SparseVectorDataToPlaceholderGroupBytes(idfSparseVector)
	return avgdl, nil
}

func (sd *shardDelegator) DropIndex(ctx context.Context, req *querypb.DropIndexRequest) error {
	workers := sd.workerManager.GetAllWorkers()
	for _, worker := range workers {
		if err := worker.DropIndex(ctx, req); err != nil {
			return err
		}
	}
	return nil
}

func (sd *shardDelegator) GetHighlight(ctx context.Context, req *querypb.GetHighlightRequest) ([]*querypb.HighlightResult, error) {
	result := []*querypb.HighlightResult{}
	for _, task := range req.GetTasks() {
		if len(task.GetTexts()) != int(task.GetSearchTextNum()+task.GetCorpusTextNum())+len(task.GetQueries()) {
			return nil, errors.Errorf("package highlight texts error, num of texts not equal the expected num %d:%d", len(task.GetTexts()), int(task.GetSearchTextNum()+task.GetCorpusTextNum())+len(task.GetQueries()))
		}
		analyzer, ok := sd.analyzerRunners[task.GetFieldId()]
		if !ok {
			return nil, merr.WrapErrParameterInvalidMsg("get highlight failed, the highlight field not found, %s:%d", task.GetFieldName(), task.GetFieldId())
		}
		topks := req.GetTopks()
		var results [][]*milvuspb.AnalyzerToken
		var err error

		if len(analyzer.GetInputFields()) == 1 {
			results, err = analyzer.BatchAnalyze(true, false, task.GetTexts())
			if err != nil {
				return nil, err
			}
		} else if len(analyzer.GetInputFields()) == 2 {
			// use analyzer names if analyzer need two input field
			results, err = analyzer.BatchAnalyze(true, false, task.GetTexts(), task.GetAnalyzerNames())
			if err != nil {
				return nil, err
			}
		}

		// analyze result of search text
		searchResults := results[0:task.SearchTextNum]
		// analyze result of query text
		queryResults := results[task.SearchTextNum : task.SearchTextNum+int64(len(task.Queries))]
		// analyze result of corpus text
		corpusStartOffset := int(task.SearchTextNum) + len(task.Queries)
		corpusResults := results[corpusStartOffset:]

		// query for all corpus texts
		// only support text match now
		// build match set for all analyze result of query text
		// TODO: support more query types
		queryTokenSet := typeutil.NewSet[string]()
		for _, tokens := range queryResults {
			for _, token := range tokens {
				queryTokenSet.Insert(token.GetToken())
			}
		}

		corpusIdx := 0
		for i := range len(topks) {
			tokenSet := typeutil.NewSet[string]()
			if len(searchResults) > i {
				for _, token := range searchResults[i] {
					tokenSet.Insert(token.GetToken())
				}
			}

			for j := 0; j < int(topks[i]); j++ {
				spans := SpanList{}
				for _, token := range corpusResults[corpusIdx] {
					if tokenSet.Contain(token.GetToken()) || queryTokenSet.Contain(token.GetToken()) {
						spans = append(spans, Span{token.GetStartOffset(), token.GetEndOffset()})
					}
				}
				spans = mergeOffsets(spans)

				// Convert byte offsets from analyzer to rune (character) offsets
				corpusText := task.Texts[corpusStartOffset+corpusIdx]
				err := bytesOffsetToRuneOffset(corpusText, spans)
				if err != nil {
					return nil, err
				}

				frags := fetchFragmentsFromOffsets(corpusText, spans,
					task.GetOptions().GetFragmentOffset(),
					task.GetOptions().GetFragmentSize(),
					task.GetOptions().GetNumOfFragments())
				result = append(result, &querypb.HighlightResult{Fragments: frags})
				corpusIdx++
			}
		}
	}

	return result, nil
}

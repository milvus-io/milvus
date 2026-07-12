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
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/cluster"
	"github.com/milvus-io/milvus/internal/querynodev2/delegator/deletebuffer"
	"github.com/milvus-io/milvus/internal/querynodev2/pkoracle"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/internal/util/grpcclient"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// delegator data related part

const defaultAnalyzerName = "default"

func normalizeAnalyzerNames(analyzerNames []string, textNum int) ([]string, error) {
	if textNum == 0 {
		return []string{}, nil
	}

	switch len(analyzerNames) {
	case 0:
		names := make([]string, textNum)
		for i := range names {
			names[i] = defaultAnalyzerName
		}
		return names, nil
	case 1:
		name := analyzerNames[0]
		if name == "" {
			name = defaultAnalyzerName
		}
		names := make([]string, textNum)
		for i := range names {
			names[i] = name
		}
		return names, nil
	case textNum:
		names := append([]string(nil), analyzerNames...)
		for i, name := range names {
			if name == "" {
				names[i] = defaultAnalyzerName
			}
		}
		return names, nil
	default:
		return nil, merr.WrapErrParameterInvalidMsg("analyzer names size must be 0, 1, or equal to text size, got analyzer names size [%d], text size [%d]", len(analyzerNames), textNum)
	}
}

func normalizeHighlightAnalyzerNames(analyzerNames []string, textNum int) ([]string, error) {
	if len(analyzerNames) != textNum {
		return nil, merr.WrapErrServiceInternalMsg("highlight analyzer names size must equal text size, got analyzer names size [%d], text size [%d]", len(analyzerNames), textNum)
	}

	names := append([]string(nil), analyzerNames...)
	for i, name := range names {
		if name == "" {
			names[i] = defaultAnalyzerName
		}
	}
	return names, nil
}

// segmentEffectiveTs returns the timestamp for delete-buffer pin/ListAfter.
// For import segments with commit_timestamp, only deletes from T_commit onwards
// are applied via the buffer (pre-commit deletes are in the delta log or L0 path).
func segmentEffectiveTs(info *querypb.SegmentLoadInfo) uint64 {
	if ts := info.GetCommitTimestamp(); ts != 0 {
		return ts
	}
	return info.GetStartPosition().GetTimestamp()
}

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

type DeleteBatch struct {
	Ts   uint64
	Data []*DeleteData
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
				log.Error(context.TODO(), "failed to create new segment",
					mlog.FieldSegmentID(segmentID),
					mlog.Err(err))
				panic(err)
			}
			newGrowingSegment = true
		}

		err := growing.Insert(context.Background(), insertData.RowIDs, insertData.Timestamps, insertData.InsertRecord)
		if err != nil {
			log.Error(context.TODO(), "failed to insert data into growing segment",
				mlog.FieldSegmentID(segmentID),
				mlog.Err(err),
			)
			if errors.IsAny(err, merr.ErrSegmentNotLoaded, merr.ErrSegmentNotFound) {
				log.Warn(context.TODO(), "try to insert data into released segment, skip it", mlog.Err(err))
				continue
			}
			// panic here, insert failure
			panic(err)
		}
		growing.UpdatePkCandidate(insertData.PrimaryKeys)

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
				log.Warn(context.TODO(), "try to insert data into released segment, skip it", mlog.FieldSegmentID(segmentID))
				sd.growingSegmentLock.Unlock()
				growing.Release(context.Background())
				continue
			}

			if !sd.distribution.GrowingSegmentExists(segmentID) {
				// register created growing segment after insert, avoid to add empty growing to delegator
				if idfOracle := sd.getIDFOracle(); idfOracle != nil {
					idfOracle.RegisterGrowing(segmentID, insertData.BM25Stats)
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
		} else if idfOracle := sd.getIDFOracle(); idfOracle != nil {
			idfOracle.UpdateGrowing(growing.ID(), insertData.BM25Stats)
		}
		log.Info(context.TODO(), "insert into growing segment",
			mlog.FieldCollectionID(growing.Collection()),
			mlog.FieldSegmentID(segmentID),
			mlog.Int("rowCount", len(insertData.RowIDs)),
			mlog.Uint64("maxTimestamp", insertData.Timestamps[len(insertData.Timestamps)-1]),
		)
	}
	metrics.QueryNodeProcessCost.WithLabelValues(paramtable.GetStringNodeID(), metrics.InsertLabel).
		Observe(float64(tr.ElapseSpan().Milliseconds()))
}

// ProcessDelete handles delete data in delegator.
// delegator puts deleteData into buffer first,
// then dispatch data to segments according to the result of bloom filter check.
func (sd *shardDelegator) ProcessDelete(deleteData []*DeleteData, ts uint64) {
	sd.ProcessDeleteBatches([]DeleteBatch{{Ts: ts, Data: deleteData}})
}

func (sd *shardDelegator) ProcessDeleteBatches(batches []DeleteBatch) {
	// Early return if delegator is stopped - ProcessDelete becomes a no-op
	// This prevents unnecessary processing and side effects during shutdown
	if sd.Stopped() {
		return
	}
	if len(batches) == 0 {
		return
	}

	method := "ProcessDelete"
	tr := timerecord.NewTimeRecorder(method)
	// block load segment handle delete buffer
	sd.deleteMut.Lock()
	defer sd.deleteMut.Unlock()

	log := sd.getLogger(context.Background())

	log.Debug(context.TODO(), "start to process delete batches", mlog.Int("batchNum", len(batches)))
	allDeleteData := make([]*DeleteData, 0, len(batches))
	for _, batch := range batches {
		if len(batch.Data) == 0 {
			continue
		}

		cacheItems := make([]deletebuffer.BufferItem, 0, len(batch.Data))
		for _, entry := range batch.Data {
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
			Ts:   batch.Ts,
			Data: cacheItems,
		})
		allDeleteData = append(allDeleteData, batch.Data...)
	}

	if len(allDeleteData) > 0 {
		sd.forwardStreamingDeletion(context.Background(), allDeleteData)
	}

	metrics.QueryNodeProcessCost.WithLabelValues(paramtable.GetStringNodeID(), metrics.DeleteLabel).
		Observe(float64(tr.ElapseSpan().Milliseconds()))
}

type BatchApplyRet = struct {
	DeleteDataIdx int
	StartIdx      int
	Segment2Hits  map[int64][]bool
}

// applyBFInParallel applies bloom filter check in parallel on the provided pinned segments.
// Using pinned segments ensures consistency between BF check and delete application,
// preventing race conditions where new segments could be added between PinOnlineSegments
// and this call.
func (sd *shardDelegator) applyBFInParallel(deleteDatas []*DeleteData, pool *conc.Pool[any], sealed []SnapshotItem, growing []SegmentEntry) *typeutil.ConcurrentMap[int, *BatchApplyRet] {
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
				ret := BatchGetFromSegments(pks[startIdx:endIdx], partitionID, sealed, growing)
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
			mlog.FieldSegmentID(segmentEntry.SegmentID),
			mlog.Int64("workerID", nodeID),
			mlog.Int("forwardRowCount", len(delRecord.PrimaryKeys)),
		)
		if ok {
			future := pool.Submit(func() (struct{}, error) {
				log.Debug(ctx, "delegator plan to applyDelete via worker")
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
						log.Warn(ctx, "try to delete data on non-exist node")
						// cancel other request
						cancel()
						return false, err
					} else if grpcclient.IsServerIDMismatchErr(err) {
						log.Warn(ctx, "try to delete data on mismatched node, node has been replaced", mlog.Err(err))
						cancel()
						return false, err
					} else if errors.IsAny(err, merr.ErrSegmentNotFound, merr.ErrSegmentNotLoaded) {
						log.Warn(ctx, "try to delete data of released segment")
						return false, nil
					} else if err != nil {
						log.Warn(ctx, "worker failed to delete on segment", mlog.Err(err))
						return true, err
					}
					return false, nil
				}, retry.Attempts(10))
				if err != nil {
					log.Warn(ctx, "apply delete for segment failed, marking it offline")
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
	log.Info(context.TODO(), "add growing segments to delegator", mlog.Int64s("segmentIDs", lo.Map(entries, func(entry SegmentEntry, _ int) int64 {
		return entry.SegmentID
	})))
	sd.distribution.AddGrowing(entries...)
}

// LoadGrowing load growing segments locally.
func (sd *shardDelegator) LoadGrowing(ctx context.Context, infos []*querypb.SegmentLoadInfo, version int64) error {
	log := sd.getLogger(ctx)

	segmentIDs := lo.Map(infos, func(info *querypb.SegmentLoadInfo, _ int) int64 { return info.GetSegmentID() })
	log.Info(ctx, "loading growing segments...", mlog.Int64s("segmentIDs", segmentIDs))
	loaded, err := sd.loader.Load(ctx, sd.collectionID, segments.SegmentTypeGrowing, version, infos...)
	if err != nil {
		log.Warn(ctx, "failed to load growing segment", mlog.Err(err))
		return err
	}

	for _, segment := range loaded {
		err = sd.addL0ForGrowing(ctx, segment)
		if err != nil {
			log.Warn(ctx, "failed to forward L0 deletions to growing segment",
				mlog.Err(err),
			)

			// clear loaded growing segments
			for _, segment := range loaded {
				segment.Release(ctx)
			}
			return err
		}
	}

	segmentIDs = lo.Map(loaded, func(segment segments.Segment, _ int) int64 { return segment.ID() })
	log.Info(ctx, "load growing segments done", mlog.Int64s("segmentIDs", segmentIDs))

	if idfOracle := sd.getIDFOracle(); idfOracle != nil {
		for _, segment := range loaded {
			idfOracle.RegisterGrowing(segment.ID(), segment.GetBM25Stats())
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

// load bm25 stats for sealed segments.
// idf oracle owns the full lifecycle: download, disk write, register, cleanup.
func (sd *shardDelegator) loadBM25Stats(ctx context.Context, infos []*querypb.SegmentLoadInfo, req *querypb.LoadSegmentsRequest) error {
	idfOracle := sd.getIDFOracle()
	if idfOracle == nil {
		// A nil oracle is only legitimate when the schema has no BM25 function.
		// Judge by the SAME schema source as the pendingBM25Loads marking (the
		// request's schema, falling back to the live view): a load can carry a
		// BM25-bearing schema while the live view has not advanced yet (e.g.
		// the request had no index meta, so syncCollectionIndexMeta did not
		// advance it) — returning success there would silently skip this
		// segment's stats forever (progressive visibility never retries an
		// already-loaded segment). Error out instead so querycoord retries the
		// load until the oracle exists; mirrors loadBM25StatsForReopen.
		bm25Schema := req.GetSchema()
		if bm25Schema == nil {
			bm25Schema = sd.collection.Schema()
		}
		if len(newBM25FunctionSet(bm25Schema)) > 0 {
			return merr.WrapErrServiceInternal("segment load carries BM25 stats before delegator BM25 oracle is initialized")
		}
		return nil
	}

	pool := segments.GetLoadPool()

	cm := sd.loader.GetChunkManager()
	futures := make([]*conc.Future[any], 0, len(infos))
	for _, info := range infos {
		info := info
		futures = append(futures, pool.Submit(func() (any, error) {
			if err := idfOracle.LoadSealed(ctx, info.GetSegmentID(), info, cm); err != nil {
				mlog.Warn(ctx, "failed to load bm25 stats for segment",
					mlog.FieldCollectionID(req.GetCollectionID()),
					mlog.FieldSegmentID(info.GetSegmentID()),
					mlog.Err(err))
				return nil, err
			}
			return nil, nil
		}))
	}

	err := conc.BlockOnAll(futures...)
	if err != nil {
		mlog.Warn(ctx, "failed to load bm25 stats", mlog.Err(err))
		return err
	}
	return nil
}

// reopenInfosWithBM25Stats returns the reopen request's segments that carry
// BM25 stats to ingest.
func (sd *shardDelegator) reopenInfosWithBM25Stats(ctx context.Context, req *querypb.LoadSegmentsRequest) ([]*querypb.SegmentLoadInfo, error) {
	log := sd.getLogger(ctx)
	infosWithBM25Stats := make([]*querypb.SegmentLoadInfo, 0, len(req.GetInfos()))
	for _, info := range req.GetInfos() {
		bm25Paths, err := packed.NewStatsResolverFromLoadInfo(info).BM25StatsPaths()
		if err != nil {
			log.Warn(ctx, "resolve reopened bm25 stats failed", mlog.FieldSegmentID(info.GetSegmentID()), mlog.Err(err))
			return nil, err
		}
		if len(bm25Paths) > 0 {
			infosWithBM25Stats = append(infosWithBM25Stats, info)
		}
	}
	return infosWithBM25Stats, nil
}

// preloadReopenBM25Stats downloads and registers (without activating) the BM25
// stats of every reopened segment BEFORE the worker swaps the new data shape
// in. LoadSealedForReopen is idempotent: the post-load activation pass finds
// everything downloaded and only flips the activation.
func (sd *shardDelegator) preloadReopenBM25Stats(ctx context.Context, req *querypb.LoadSegmentsRequest) error {
	infos, err := sd.reopenInfosWithBM25Stats(ctx, req)
	if err != nil {
		return err
	}
	if len(infos) == 0 {
		return nil
	}
	idfOracle := sd.getIDFOracle()
	if idfOracle == nil {
		return merr.WrapErrServiceInternal("reopen contains BM25 stats before delegator BM25 oracle is initialized")
	}
	cm := sd.loader.GetChunkManager()
	for _, info := range infos {
		if err := idfOracle.LoadSealedForReopen(ctx, info.GetSegmentID(), info, cm, false); err != nil {
			return err
		}
	}
	return nil
}

func (sd *shardDelegator) handleReopenPostLoad(ctx context.Context, req *querypb.LoadSegmentsRequest) error {
	infosWithBM25Stats, err := sd.reopenInfosWithBM25Stats(ctx, req)
	if err != nil {
		return err
	}

	if len(infosWithBM25Stats) > 0 {
		if err := sd.loadBM25StatsForReopen(ctx, infosWithBM25Stats, req); err != nil {
			return err
		}
	}
	// The ready publish attempt happens in LoadSegments' deferred
	// tryPublishReadySchema, after this load's pending BM25 marks are cleared.
	return nil
}

func (sd *shardDelegator) loadBM25StatsForReopen(ctx context.Context, infos []*querypb.SegmentLoadInfo, req *querypb.LoadSegmentsRequest) error {
	idfOracle := sd.getIDFOracle()
	if idfOracle == nil {
		return merr.WrapErrServiceInternal("reopen contains BM25 stats before delegator BM25 oracle is initialized")
	}

	pool := segments.GetLoadPool()
	cm := sd.loader.GetChunkManager()
	futures := make([]*conc.Future[any], 0, len(infos))
	for _, info := range infos {
		info := info
		futures = append(futures, pool.Submit(func() (any, error) {
			activateIfReadable := sd.distribution.IsReadableSealedSegment(info.GetSegmentID())
			if err := idfOracle.LoadSealedForReopen(ctx, info.GetSegmentID(), info, cm, activateIfReadable); err != nil {
				mlog.Warn(ctx, "failed to load reopened bm25 stats for segment",
					mlog.FieldCollectionID(req.GetCollectionID()),
					mlog.FieldSegmentID(info.GetSegmentID()),
					mlog.Bool("activateIfReadable", activateIfReadable),
					mlog.Err(err))
				return nil, err
			}
			return nil, nil
		}))
	}

	if err := conc.BlockOnAll(futures...); err != nil {
		mlog.Warn(ctx, "failed to load reopened bm25 stats", mlog.Err(err))
		return err
	}
	return nil
}

// syncCollectionIndexMeta refreshes the delegator node's CCollection IndexMeta after a
// forwarded worker load. Worker LoadSegments already updates IndexMeta on the target
// worker, but the delegator (which executes growing search locally) must stay in sync.
func (sd *shardDelegator) syncCollectionIndexMeta(ctx context.Context, req *querypb.LoadSegmentsRequest) error {
	if len(req.GetIndexInfoList()) == 0 {
		return nil
	}

	schema := req.GetSchema()
	if schema == nil {
		schema = sd.collection.Schema()
	}

	loadMeta := req.GetLoadMeta()
	if loadMeta == nil {
		loadMeta = &querypb.LoadMetaInfo{
			CollectionID: req.GetCollectionID(),
		}
	}

	beforeVersion := sd.collection.Schema().GetVersion()

	meta := segments.ComposeIndexMeta(ctx, req.GetIndexInfoList(), schema)
	if err := sd.collectionManager.PutOrRef(req.GetCollectionID(), schema, meta, loadMeta); err != nil {
		return err
	}
	sd.collectionManager.Unref(req.GetCollectionID(), 1)

	// Load-wins race: PutOrRef may have advanced the collection schema ahead of
	// the UpdateSchema event. When it did, register the new runtime deps and
	// attempt the ready publish so the read path can serve the new version.
	// schemaChangeMutex is required: ensureIDFOracle is a non-atomic
	// check-then-act, and this must not interleave with UpdateSchema or another
	// concurrent load (otherwise two oracles get created — split-brain). Taking
	// the lock here is safe: LoadSegments does not hold it, so no re-entrancy.
	// updateFunctionRunners/ensureIDFOracle are idempotent and tryPublish
	// re-derives readiness from state (this load's own BM25 stats are still in
	// the pending set here, so a not-yet-ready version cannot slip out), so a
	// redundant run after a concurrent UpdateSchema is harmless.
	sd.schemaChangeMutex.Lock()
	defer sd.schemaChangeMutex.Unlock()
	if advanced, _, advancedBarrier := sd.collection.SchemaSnapshot(); advanced.GetVersion() > beforeVersion {
		// Load-wins: this load advanced the local schema ahead of the WAL
		// UpdateSchema event, but only this load's DstNode worker knows the new
		// schema. Fan it out to every worker before the publish attempt — a
		// published version admits reads compiled against it, and those reads
		// fan out to workers whose segcore must know the schema. An error fails
		// the load and querycoord retries it.
		if err := sd.fanoutWorkerSchema(ctx, advanced, advancedBarrier); err != nil {
			return err
		}
		if err := sd.syncSchemaRuntimeAndPublish(ctx, advanced); err != nil {
			return err
		}
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
		mlog.Int64("workID", targetNodeID),
		mlog.Int64s("segments", lo.Map(req.GetInfos(), func(info *querypb.SegmentLoadInfo, _ int) int64 { return info.GetSegmentID() })),
	)

	if req.GetInfos()[0].GetLevel() == datapb.SegmentLevel_L0 {
		return merr.WrapErrServiceInternal("load L0 segment is not supported, l0 segment should only be loaded by watchChannel")
	}

	// pin all segments to prevent delete buffer has been cleaned up during worker load segments
	// Note: if delete records is pinned, it will skip cleanup during SyncTargetVersion
	// which means after segment is loaded, then delete buffer will be cleaned up by next SyncTargetVersion call
	for _, info := range req.GetInfos() {
		sd.deleteBuffer.Pin(segmentEffectiveTs(info), info.GetSegmentID())
	}
	defer func() {
		for _, info := range req.GetInfos() {
			sd.deleteBuffer.Unpin(segmentEffectiveTs(info), info.GetSegmentID())
		}
	}()

	// Mark segments whose BM25 stats this load may download as pending BEFORE
	// any step that can advance the collection schema (syncCollectionIndexMeta's
	// PutOrRef): while pending, tryPublishReadySchema refuses to publish, so a
	// concurrent schema-version advance (UpdateSchema no-op branch, another
	// load) cannot expose a BM25 search to stats that are still downloading.
	// The BM25 schema check uses the request's schema (the load-wins case
	// carries a schema newer than the collection's) falling back to the live
	// one; the segment filter over-approximates on purpose (v2 manifest paths
	// are only resolved to concrete stats paths later, remotely) — a spurious
	// mark merely delays a concurrent version publish until this load returns.
	// Always cleared on return — a failed load is retried by querycoord and
	// meanwhile degrades to progressive visibility instead of blocking
	// publishes; the deferred tryPublish then makes the attempt that a gated
	// concurrent path may have skipped while this load was pending.
	bm25Schema := req.GetSchema()
	if bm25Schema == nil {
		bm25Schema = sd.collection.Schema()
	}
	if len(newBM25FunctionSet(bm25Schema)) > 0 {
		pendingBM25Segments := lo.FilterMap(req.GetInfos(), func(info *querypb.SegmentLoadInfo, _ int) (int64, bool) {
			return info.GetSegmentID(), len(info.GetBm25Logs()) > 0 || info.GetManifestPath() != ""
		})
		if len(pendingBM25Segments) > 0 {
			sd.addPendingBM25Loads(pendingBM25Segments)
			defer func() {
				sd.removePendingBM25Loads(pendingBM25Segments)
				if err := sd.tryPublishReadySchema(ctx); err != nil {
					log.Warn(ctx, "failed to publish ready schema after BM25 stats load", mlog.Err(err))
				}
			}()
		}
	}

	// Reopen carries a new data shape (e.g. a backfilled BM25 column) for
	// segments that are ALREADY searchable. We DOWNLOAD their BM25 stats BEFORE
	// the worker swaps the new column in, which shrinks the inconsistency window
	// from "the whole remote download" (seconds) to just the local activation
	// step: a download failure aborts the reopen before the swap, so no long
	// window opens. NOTE: preload only downloads/registers the stats; they are
	// merged into the IDF oracle's live corpus only at ACTIVATION, which happens
	// after the worker swap (in handleReopenPostLoad). So a residual sub-millisecond
	// window remains — between swap and activation the column is searchable while
	// its stats are not yet in the oracle (empty results when avgdl is still 0,
	// slightly skewed IDF otherwise) — but it self-heals within this same
	// LoadSegments call. Fully closing it would require activating before the swap,
	// which risks the reverse inconsistency (stats present, column not yet live),
	// so this ordering is a deliberate trade-off.
	if req.GetLoadScope() == querypb.LoadScope_Reopen {
		if err := sd.preloadReopenBM25Stats(ctx, req); err != nil {
			log.Warn(ctx, "failed to preload reopened BM25 stats before worker swap", mlog.Err(err))
			return err
		}
	}

	worker, err := sd.workerManager.GetWorker(ctx, targetNodeID)
	if err != nil {
		log.Warn(ctx, "delegator failed to find worker", mlog.Err(err))
		return err
	}

	req.Base.TargetID = targetNodeID
	log.Debug(ctx, "worker loads segments...")

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
		log.Warn(ctx, "worker failed to load segments", mlog.Err(err))
		return err
	}
	log.Debug(ctx, "work loads segments done")

	if err := sd.syncCollectionIndexMeta(ctx, req); err != nil {
		log.Warn(ctx, "failed to sync collection index meta on delegator", mlog.Err(err))
		return err
	}

	// load index segment need no stream delete and distribution change
	if req.GetLoadScope() == querypb.LoadScope_Index {
		return nil
	}
	if req.GetLoadScope() == querypb.LoadScope_Reopen {
		return sd.handleReopenPostLoad(ctx, req)
	}

	return sd.withPostLoadLimit(ctx, func() error {
		infos := lo.Filter(req.GetInfos(), func(info *querypb.SegmentLoadInfo, _ int) bool {
			return !sd.distribution.SealedSegmentExistsOnNode(info.GetSegmentID(), targetNodeID)
		})

		candidates, err := sd.loader.LoadBloomFilterSet(ctx, req.GetCollectionID(), infos...)
		if err != nil {
			log.Warn(ctx, "failed to load bloom filter set for segment", mlog.Err(err))
			return err
		}

		// Load BM25 stats BEFORE loadStreamDelete so stats are ready before segment becomes visible
		err = sd.loadBM25Stats(ctx, infos, req)
		if err != nil {
			log.Warn(ctx, "failed to load BM25 stats", mlog.Err(err))
			return err
		}

		// Build a map from segmentID to BloomFilterSet
		bfMap := make(map[int64]pkoracle.Candidate)
		for _, candidate := range candidates {
			log.Info(ctx, "loaded bloom filter set for sealed segment",
				mlog.FieldSegmentID(candidate.ID()),
			)
			bfMap[candidate.ID()] = candidate
		}

		// Build entries with Candidate before loadStreamDelete, which will atomically add them to distribution
		entries := make([]SegmentEntry, 0, len(infos))
		for _, info := range infos {
			entries = append(entries, SegmentEntry{
				SegmentID:   info.GetSegmentID(),
				PartitionID: info.GetPartitionID(),
				NodeID:      req.GetDstNodeID(),
				Version:     req.GetVersion(),
				Level:       info.GetLevel(),
				Candidate:   bfMap[info.GetSegmentID()],
			})
		}

		log.Debug(ctx, "load delete...")
		// loadStreamDelete now handles distribution add atomically in Phase 3
		err = sd.loadStreamDelete(ctx, candidates, infos, req, targetNodeID, worker,
			entries, req.GetLoadMeta().GetSchemaBarrierTs())
		if err != nil {
			log.Warn(ctx, "load stream delete failed", mlog.Err(err))
			// BM25 stats already loaded into idf oracle will be cleaned up
			// automatically by SyncDistribution when the segment is not in target.
			return err
		}
		log.Debug(ctx, "load stream delete done")

		return nil
	})
}

func (sd *shardDelegator) withPostLoadLimit(ctx context.Context, fn func() error) error {
	if sd.postLoadSem == nil {
		return fn()
	}

	start := time.Now()
	if err := sd.postLoadSem.Acquire(ctx); err != nil {
		return err
	}
	defer sd.postLoadSem.Release()
	mlog.Debug(ctx, "delegator acquired post-load slot",
		mlog.Duration("wait", time.Since(start)),
		mlog.Int("capacity", sd.postLoadSem.Cap()),
		mlog.Int("current", sd.postLoadSem.Current()))

	return fn()
}

func (sd *shardDelegator) addDistributionIfSchemaBarrierOK(schemaBarrierTs uint64, entries ...SegmentEntry) error {
	sd.schemaChangeMutex.RLock()
	defer sd.schemaChangeMutex.RUnlock()
	if schemaBarrierTs < sd.schemaBarrierTs {
		return merr.WrapErrServiceInternal("schema barrier changed")
	}

	// alter distribution
	sd.distribution.AddDistributions(entries...)
	return nil
}

// LoadGrowing load growing segments locally.
func (sd *shardDelegator) LoadL0(ctx context.Context, infos []*querypb.SegmentLoadInfo, version int64) error {
	log := sd.getLogger(ctx)

	segmentIDs := lo.Map(infos, func(info *querypb.SegmentLoadInfo, _ int) int64 { return info.GetSegmentID() })
	log.Info(ctx, "loading l0 segments...", mlog.Int64s("segmentIDs", segmentIDs))

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
			log.Warn(ctx, "failed to load l0 segment", mlog.Err(err))
			return err
		}
	}

	segmentIDs = lo.Map(loaded, func(segment segments.Segment, _ int) int64 { return segment.ID() })
	log.Info(ctx, "load l0 segments done", mlog.Int64s("segmentIDs", segmentIDs))

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
	totalForwardRows := int64(0)
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

				if !candidate.PkCandidateExist() {
					for i := idx; i < endIdx; i++ {
						totalForwardRows += 1
						if err := fn(segmentPks[i], segmentTss[i]); err != nil {
							return err
						}
					}
					continue
				}

				lc := storage.NewBatchLocationsCache(segmentPks[idx:endIdx])
				hits := candidate.BatchPkExist(lc)
				for i, hit := range hits {
					if !hit {
						continue
					}
					totalForwardRows += 1
					if err := fn(segmentPks[idx+i], segmentTss[idx+i]); err != nil {
						return err
					}
				}
			}
		}
	}

	log.Info(context.TODO(), "forward delete from L0 segments to worker",
		mlog.Int64("targetSegmentID", candidate.ID()),
		mlog.String("channel", sd.vchannelName),
		mlog.Bool("broadcast", !candidate.PkCandidateExist()),
		mlog.Int("l0SegmentCount", processedL0Count),
		mlog.Int("totalDeleteRowsInL0", totalL0Rows),
		mlog.Int64("totalForwardRows", totalForwardRows),
		mlog.Int64("totalCost", time.Since(start).Milliseconds()),
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
		paramtable.GetStringNodeID(),
		fmt.Sprint(sd.Collection()),
		commonpb.SegmentState_Sealed.String(),
		datapb.SegmentLevel_L0.String(),
	).Set(float64(len(level0Segments)))

	metrics.QueryNodeLevelZeroSize.WithLabelValues(
		paramtable.GetStringNodeID(),
		fmt.Sprint(sd.collectionID),
		sd.vchannelName,
	).Set(float64(totalSize))
}

// processDeleteRecords performs BF checks on delete buffer records and forwards matching deletes
// via the buffered forwarder. Does NOT require any lock to be held.
// When candidate has no stats (PkCandidateExist() == false), all deletes are forwarded (broadcast mode).
// Returns the number of timestamp-hit and bloom-filter-hit rows.
func (sd *shardDelegator) processDeleteRecords(
	candidate *pkoracle.BloomFilterSet,
	records []*deletebuffer.Item,
	forwarder *BufferForwarder,
) (tsHit, bfHit int64, err error) {
	for _, entry := range records {
		for _, record := range entry.Data {
			tsHit += int64(len(record.DeleteData.Pks))
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

				// When BF not initialized (bloom filter disabled), forward all deletes
				if !candidate.PkCandidateExist() {
					for i := idx; i < endIdx; i++ {
						bfHit++
						if err = forwarder.Buffer(pks[i], record.DeleteData.Tss[i]); err != nil {
							return tsHit, bfHit, err
						}
					}
					continue
				}

				lc := storage.NewBatchLocationsCache(pks[idx:endIdx])
				hits := candidate.BatchPkExist(lc)
				for i, hit := range hits {
					if hit {
						bfHit++
						if err = forwarder.Buffer(pks[idx+i], record.DeleteData.Tss[idx+i]); err != nil {
							return tsHit, bfHit, err
						}
					}
				}
			}
		}
	}
	return tsHit, bfHit, nil
}

// segDeleteSnapshot holds the snapshotted delete buffer entries for a segment,
// captured under RLock in Phase 1 of loadStreamDelete.
type segDeleteSnapshot struct {
	records       []*deletebuffer.Item // copied slice of delete buffer entries
	snapshotMaxTs uint64               // max Item.Ts in snapshot, used for timestamp-based catch-up
}

func (sd *shardDelegator) loadStreamDelete(ctx context.Context,
	candidates []*pkoracle.BloomFilterSet,
	infos []*querypb.SegmentLoadInfo,
	req *querypb.LoadSegmentsRequest,
	targetNodeID int64,
	worker cluster.Worker,
	entries []SegmentEntry,
	schemaBarrierTs uint64,
) error {
	log := sd.getLogger(ctx)

	idCandidates := lo.SliceToMap(candidates, func(candidate *pkoracle.BloomFilterSet) (int64, *pkoracle.BloomFilterSet) {
		return candidate.ID(), candidate
	})

	// Phase 0: Forward L0 deletions (no lock needed, unchanged)
	for _, info := range infos {
		candidate := idCandidates[info.GetSegmentID()]
		err := sd.forwardL0Deletion(ctx, info, req, candidate, targetNodeID, worker)
		if err != nil {
			return err
		}
	}

	// === Phase 1: Snapshot delete buffer entries under RLock (fast — microseconds) ===
	sd.deleteMut.RLock()
	snapshots := make([]segDeleteSnapshot, len(infos))
	for i, info := range infos {
		records := sd.deleteBuffer.ListAfter(segmentEffectiveTs(info))
		// Copy the slice to safely use outside lock scope.
		// ListAfter returns a new slice from doubleCacheBuffer, but we copy to
		// ensure no dependency on internal buffer state that may change after unlock.
		copied := make([]*deletebuffer.Item, len(records))
		copy(copied, records)
		var maxTs uint64
		if len(records) > 0 {
			maxTs = records[len(records)-1].Ts
		}
		snapshots[i] = segDeleteSnapshot{
			records:       copied,
			snapshotMaxTs: maxTs,
		}
	}
	sd.deleteMut.RUnlock()
	// RLock released — WAL pipeline (ProcessDelete) is now unblocked

	// Create one forwarder per segment, shared across Phase 2 and Phase 3, flushed once at the end.
	forwarders := make([]*BufferForwarder, len(infos))
	for i, info := range infos {
		candidate := idCandidates[info.GetSegmentID()]
		deleteScope := querypb.DataScope_All
		switch candidate.Type() {
		case commonpb.SegmentState_Sealed:
			deleteScope = querypb.DataScope_Historical
		case commonpb.SegmentState_Growing:
			deleteScope = querypb.DataScope_Streaming
		}
		forwarders[i] = NewBufferedForwarder(paramtable.Get().QueryNodeCfg.ForwardBatchSize.GetAsInt64(),
			deleteViaWorker(ctx, worker, targetNodeID, info, deleteScope))
	}

	// === Phase 2: Process snapshot WITHOUT lock (expensive — seconds) ===
	for i, info := range infos {
		candidate := idCandidates[info.GetSegmentID()]
		start := time.Now()
		tsHit, bfHit, err := sd.processDeleteRecords(candidate, snapshots[i].records, forwarders[i])
		if err != nil {
			return err
		}
		log.Info(ctx, "forward delete to worker (phase 2: snapshot)...",
			mlog.String("channel", info.InsertChannel),
			mlog.FieldSegmentID(info.GetSegmentID()),
			mlog.Time("startPosition", tsoutil.PhysicalTime(info.GetStartPosition().GetTimestamp())),
			mlog.Int64("tsHitDeleteRowNum", tsHit),
			mlog.Int64("bfHitDeleteRowNum", bfHit),
			mlog.Int64("bfCost", time.Since(start).Milliseconds()),
		)
	}

	// === Phase 3: Catch-up new entries + flush + add distribution under RLock (fast — milliseconds) ===
	sd.deleteMut.RLock()
	defer sd.deleteMut.RUnlock()

	for i, info := range infos {
		candidate := idCandidates[info.GetSegmentID()]

		// Use timestamp-based catch-up: fetch records added after the snapshot's max timestamp.
		// This is robust against delete buffer eviction (Put → evict discards old tail during Phase 2).
		// Index-based approach (allRecords[snapshotLen:]) would panic or miss data if eviction occurs.
		// Item.Ts comes from WAL TSO, monotonically increasing and unique per ProcessDelete call,
		// so ListAfter(snapshotMaxTs + 1) precisely captures only new records.
		catchUpTs := segmentEffectiveTs(info)
		if snapshots[i].snapshotMaxTs > 0 {
			catchUpTs = snapshots[i].snapshotMaxTs + 1
		}
		newRecords := sd.deleteBuffer.ListAfter(catchUpTs)
		if len(newRecords) > 0 {
			start := time.Now()
			tsHit, bfHit, err := sd.processDeleteRecords(candidate, newRecords, forwarders[i])
			if err != nil {
				return err
			}
			log.Info(ctx, "forward delete to worker (phase 3: catch-up)...",
				mlog.String("channel", info.InsertChannel),
				mlog.FieldSegmentID(info.GetSegmentID()),
				mlog.Int64("tsHitDeleteRowNum", tsHit),
				mlog.Int64("bfHitDeleteRowNum", bfHit),
				mlog.Int64("bfCost", time.Since(start).Milliseconds()),
			)
		}

		// Flush once per segment after both phases are done
		if err := forwarders[i].Flush(); err != nil {
			return err
		}
	}

	// Atomically add to distribution while still holding RLock.
	// This guarantees no ProcessDelete can run between catch-up and distribution update,
	// so there is no gap between "deletes applied" and "segment visible".
	if err := sd.addDistributionIfSchemaBarrierOK(schemaBarrierTs, entries...); err != nil {
		return err
	}
	log.Info(ctx, "load stream delete done")
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
		mlog.Int64s("segmentIDs", req.GetSegmentIDs()),
		mlog.FieldNodeID(req.GetNodeID()),
		mlog.String("scope", req.GetScope().String()),
		mlog.Bool("force", force))

	log.Info(ctx, "delegator start to release segments")
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

	var releaseErr error
	if !force {
		worker, err := sd.workerManager.GetWorker(ctx, targetNodeID)
		if err != nil {
			log.Warn(ctx, "delegator failed to find worker", mlog.Err(err))
			releaseErr = err
		}
		req.Base.TargetID = targetNodeID
		err = worker.ReleaseSegments(ctx, req)
		if err != nil {
			log.Warn(ctx, "worker failed to release segments", mlog.Err(err))
			releaseErr = err
		}
	}
	if len(growing) > 0 {
		sd.growingSegmentLock.Unlock()
	}

	if releaseErr != nil {
		return releaseErr
	}
	if len(growing) > 0 && sd.growingSourceProvider != nil {
		for _, entry := range growing {
			sd.growingSourceProvider.ClearReleasePrepared(entry.SegmentID)
		}
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
			mlog.Info(context.TODO(), "use checkpoint as deleteCP",
				mlog.String("channelName", sd.vchannelName),
				mlog.Time("deleteSeekPos", tsoutil.PhysicalTime(action.GetCheckpoint().GetTimestamp())))
		}

		start := time.Now()
		sizeBeforeClean, _ := sd.deleteBuffer.Size()
		l0NumBeforeClean := len(sd.deleteBuffer.ListL0())
		sd.deleteBuffer.UnRegister(deleteSeekPos.GetTimestamp())
		sizeAfterClean, _ := sd.deleteBuffer.Size()
		l0NumAfterClean := len(sd.deleteBuffer.ListL0())

		if sizeAfterClean < sizeBeforeClean || l0NumAfterClean < l0NumBeforeClean {
			mlog.Info(context.TODO(), "clean delete buffer",
				mlog.String("channel", sd.vchannelName),
				mlog.Time("deleteSeekPos", tsoutil.PhysicalTime(deleteSeekPos.GetTimestamp())),
				mlog.Time("channelCP", tsoutil.PhysicalTime(checkpoint.GetTimestamp())),
				mlog.Int64("sizeBeforeClean", sizeBeforeClean),
				mlog.Int64("sizeAfterClean", sizeAfterClean),
				mlog.Int("l0NumBeforeClean", l0NumBeforeClean),
				mlog.Int("l0NumAfterClean", l0NumAfterClean),
				mlog.Duration("cost", time.Since(start)),
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

func (sd *shardDelegator) buildBM25IDF(ctx context.Context, req *internalpb.SearchRequest) (float64, error) {
	idfOracle := sd.getIDFOracle()
	if idfOracle == nil {
		return 0, merr.WrapErrServiceInternal("bm25 oracle is not initialized")
	}

	pb := &commonpb.PlaceholderGroup{}
	if err := proto.Unmarshal(req.GetPlaceholderGroup(), pb); err != nil {
		return 0, merr.WrapErrParameterInvalidMsg("failed to unmarshal BM25 IDF placeholder group: %v", err)
	}

	if len(pb.Placeholders) != 1 || len(pb.Placeholders[0].Values) == 0 {
		return 0, merr.WrapErrParameterInvalidMsg("please provide varchar/text for BM25 Function based search")
	}

	holder := pb.Placeholders[0]
	if holder.Type != commonpb.PlaceholderType_VarChar {
		return 0, merr.WrapErrParameterInvalidMsg("please provide varchar/text for BM25 Function based search, got %s", holder.Type.String())
	}

	texts := funcutil.GetVarCharFromPlaceholder(holder)
	var tfArray *schemapb.SparseFloatArray
	// Resolve the runner at the latest registered version, NOT the ready
	// snapshot's version: runner registration follows the live schema
	// (updateFunctionRunners GCs versions the delegator key no longer holds), so
	// during a reopen's deferred-publish window the published snapshot's version
	// has no runners while the advanced live version does. Runners are keyed by
	// function signature and shared across versions, and an incompatible BM25
	// function change on a loaded collection is rejected by UpdateSchema, so the
	// latest runner is always compatible with the snapshot's function set.
	schemaVersion := function.LatestFunctionRunnerVersion
	ok, err := function.RunWithRunner(ctx, sd.collectionID, schemaVersion, req.GetFieldId(), func(functionType schemapb.FunctionType, functionRunner function.FunctionRunner) error {
		if functionType != schemapb.FunctionType_BM25 {
			return merr.WrapErrServiceInternalMsg("functionRunner not found for field: %d", req.GetFieldId())
		}

		datas := []any{texts}
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
			return err
		}
		if len(output) == 0 {
			return merr.WrapErrFunctionFailedMsg("BM25 embedding failed: runner returned empty output")
		}

		var ok bool
		tfArray, ok = output[0].(*schemapb.SparseFloatArray)
		if !ok {
			return merr.WrapErrFunctionFailedMsg("functionRunner return unknown data")
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	if !ok {
		// The latest runner version has no runner for this field. The published
		// snapshot's function state said it IS a BM25 output, so the function
		// was dropped in an applied-but-not-yet-published schema (runner GC'd by
		// updateFunctionRunners). Transitional by construction: once the newer
		// version publishes, this request's version stops mapping the field to
		// BM25 entirely. Retriable so the proxy retries instead of failing a
		// read that legitimately passed the version gate.
		return 0, errors.Wrapf(merr.ErrCollectionSchemaVersionNotReady,
			"function runner for field %d is not available; the schema is transitioning past the published snapshot", req.GetFieldId())
	}

	idfSparseVector, avgdl, err := idfOracle.BuildIDF(req.GetFieldId(), tfArray)
	if err != nil {
		return 0, err
	}

	if avgdl <= 0 {
		return 0, nil
	}

	for _, idf := range idfSparseVector {
		metrics.QueryNodeSearchFTSNumTokens.WithLabelValues(paramtable.GetStringNodeID(), fmt.Sprint(sd.collectionID), fmt.Sprint(req.GetFieldId())).Observe(float64(typeutil.SparseFloatRowElementCount(idf)))
	}

	err = SetBM25Params(req, avgdl)
	if err != nil {
		return 0, err
	}

	req.PlaceholderGroup = funcutil.SparseVectorDataToPlaceholderGroupBytes(idfSparseVector)
	return avgdl, nil
}

func (sd *shardDelegator) parseMinHash(ctx context.Context, req *internalpb.SearchRequest) error {
	pb := &commonpb.PlaceholderGroup{}
	if err := proto.Unmarshal(req.GetPlaceholderGroup(), pb); err != nil {
		return merr.WrapErrParameterInvalidMsg("failed to unmarshal MinHash placeholder group: %v", err)
	}

	if len(pb.Placeholders) != 1 || len(pb.Placeholders[0].Values) == 0 {
		return merr.WrapErrParameterInvalidMsg("please provide varchar/text for MinHash Function based search")
	}

	holder := pb.Placeholders[0]
	if holder.Type != commonpb.PlaceholderType_VarChar {
		return merr.WrapErrParameterInvalidMsg("please provide varchar/text for MinHash Function based search, got %s", holder.Type.String())
	}

	texts := funcutil.GetVarCharFromPlaceholder(holder)
	var fieldData *schemapb.FieldData
	// Latest registered runner version, not the snapshot's — see buildBM25IDF.
	schemaVersion := function.LatestFunctionRunnerVersion
	ok, err := function.RunWithRunner(ctx, sd.collectionID, schemaVersion, req.GetFieldId(), func(functionType schemapb.FunctionType, functionRunner function.FunctionRunner) error {
		if functionType != schemapb.FunctionType_MinHash {
			return merr.WrapErrServiceInternalMsg("functionRunner not found for field: %d", req.GetFieldId())
		}

		output, err := functionRunner.BatchRun(texts)
		if err != nil {
			return err
		}
		if len(output) == 0 {
			return merr.WrapErrFunctionFailedMsg("MinHash embedding failed: runner returned empty output")
		}

		var ok bool
		fieldData, ok = output[0].(*schemapb.FieldData)
		if !ok {
			return merr.WrapErrFunctionFailedMsg("MinHash embedding failed: MinHash functionRunner return unknown data")
		}
		return nil
	})
	if err != nil {
		return err
	}
	if !ok {
		// Same transitional state as buildBM25IDF: the function was dropped in
		// an applied-but-not-yet-published schema, so the latest runner version
		// no longer maps this field. Retriable — see buildBM25IDF.
		return errors.Wrapf(merr.ErrCollectionSchemaVersionNotReady,
			"function runner for field %d is not available; the schema is transitioning past the published snapshot", req.GetFieldId())
	}

	vectorField := fieldData.GetVectors()
	if vectorField == nil {
		return merr.WrapErrFunctionFailedMsg("MinHash embedding failed: output is not a vector field")
	}

	binaryVector := vectorField.GetBinaryVector()
	if binaryVector == nil {
		return merr.WrapErrFunctionFailedMsg("MinHash embedding failed: output is not a binary vector")
	}

	req.PlaceholderGroup, err = funcutil.FieldDataToPlaceholderGroupBytes(fieldData)
	if err != nil {
		return err
	}
	return nil
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
	snap, err := sd.schemaReady.resolve()
	if err != nil {
		return nil, err
	}
	result := []*querypb.HighlightResult{}
	for _, task := range req.GetTasks() {
		if len(task.GetTexts()) != int(task.GetSearchTextNum()+task.GetCorpusTextNum())+len(task.GetQueries()) {
			return nil, merr.WrapErrServiceInternalMsg("package highlight texts error, num of texts not equal the expected num %d:%d", len(task.GetTexts()), int(task.GetSearchTextNum()+task.GetCorpusTextNum())+len(task.GetQueries()))
		}
		topks := req.GetTopks()
		var results [][]*milvuspb.AnalyzerToken
		var analyzeErr error
		ok, err := sd.runWithAnalyzer(ctx, snap.schema, task.GetFieldId(), func(analyzer function.Analyzer) error {
			if len(analyzer.GetInputFields()) == 1 {
				results, analyzeErr = analyzer.BatchAnalyze(true, false, task.GetTexts())
				return analyzeErr
			}
			if len(analyzer.GetInputFields()) == 2 {
				analyzerNames, err := normalizeHighlightAnalyzerNames(task.GetAnalyzerNames(), len(task.GetTexts()))
				if err != nil {
					return err
				}
				results, analyzeErr = analyzer.BatchAnalyze(true, false, task.GetTexts(), analyzerNames)
				return analyzeErr
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, merr.WrapErrParameterInvalidMsg("get highlight failed, the highlight field not found, %s:%d", task.GetFieldName(), task.GetFieldId())
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

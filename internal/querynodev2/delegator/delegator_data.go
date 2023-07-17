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
	"math/rand"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
	"github.com/milvus-io/milvus/internal/querynodev2/cluster"
	"github.com/milvus-io/milvus/internal/querynodev2/delegator/deletebuffer"
	"github.com/milvus-io/milvus/internal/querynodev2/pkoracle"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/retry"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// delegator data related part

// InsertData
type InsertData struct {
	RowIDs        []int64
	PrimaryKeys   []storage.PrimaryKey
	Timestamps    []uint64
	InsertRecord  *segcorepb.InsertRecord
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

func (sd *shardDelegator) newGrowing(segmentID int64, insertData *InsertData) segments.Segment {
	log := sd.getLogger(context.Background()).With(zap.Int64("segmentID", segmentID))

	segment, err := segments.NewSegment(sd.collection, segmentID, insertData.PartitionID, sd.collectionID, sd.vchannelName, segments.SegmentTypeGrowing, 0, insertData.StartPosition, insertData.StartPosition)
	if err != nil {
		log.Error("failed to create new segment", zap.Error(err))
		panic(err)
	}

	sd.pkOracle.Register(segment, paramtable.GetNodeID())

	sd.segmentManager.Put(segments.SegmentTypeGrowing, segment)
	sd.addGrowing(SegmentEntry{
		NodeID:        paramtable.GetNodeID(),
		SegmentID:     segmentID,
		PartitionID:   insertData.PartitionID,
		Version:       0,
		TargetVersion: initialTargetVersion,
	})
	return segment
}

// ProcessInsert handles insert data in delegator.
func (sd *shardDelegator) ProcessInsert(insertRecords map[int64]*InsertData) {
	log := sd.getLogger(context.Background())
	for segmentID, insertData := range insertRecords {
		growing := sd.segmentManager.GetGrowing(segmentID)
		if growing == nil {
			growing = sd.newGrowing(segmentID, insertData)
		}

		err := growing.Insert(insertData.RowIDs, insertData.Timestamps, insertData.InsertRecord)
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

		log.Debug("insert into growing segment",
			zap.Int64("collectionID", growing.Collection()),
			zap.Int64("segmentID", segmentID),
			zap.Int("rowCount", len(insertData.RowIDs)),
			zap.Uint64("maxTimestamp", insertData.Timestamps[len(insertData.Timestamps)-1]),
		)
	}
}

// ProcessDelete handles delete data in delegator.
// delegator puts deleteData into buffer first,
// then dispatch data to segments acoording to the result of pkOracle.
func (sd *shardDelegator) ProcessDelete(deleteData []*DeleteData, ts uint64) {
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

	// segment => delete data
	delRecords := make(map[int64]DeleteData)
	for _, data := range deleteData {
		for i, pk := range data.PrimaryKeys {
			segmentIDs, err := sd.pkOracle.Get(pk, pkoracle.WithPartitionID(data.PartitionID))
			if err != nil {
				log.Warn("failed to get delete candidates for pk", zap.Any("pk", pk.GetValue()))
				continue
			}
			for _, segmentID := range segmentIDs {
				delRecord := delRecords[segmentID]
				delRecord.PrimaryKeys = append(delRecord.PrimaryKeys, pk)
				delRecord.Timestamps = append(delRecord.Timestamps, data.Timestamps[i])
				delRecord.RowCount++
				delRecords[segmentID] = delRecord
			}
		}
	}

	offlineSegments := typeutil.NewConcurrentSet[int64]()

	sealed, growing, version := sd.distribution.GetSegments(false)

	eg, ctx := errgroup.WithContext(context.Background())
	for _, entry := range sealed {
		entry := entry
		eg.Go(func() error {
			worker, err := sd.workerManager.GetWorker(entry.NodeID)
			if err != nil {
				log.Warn("failed to get worker",
					zap.Int64("nodeID", paramtable.GetNodeID()),
					zap.Error(err),
				)
				// skip if node down
				// delete will be processed after loaded again
				return nil
			}
			offlineSegments.Upsert(sd.applyDelete(ctx, entry.NodeID, worker, delRecords, entry.Segments)...)
			return nil
		})
	}
	if len(growing) > 0 {
		eg.Go(func() error {
			worker, err := sd.workerManager.GetWorker(paramtable.GetNodeID())
			if err != nil {
				log.Error("failed to get worker(local)",
					zap.Int64("nodeID", paramtable.GetNodeID()),
					zap.Error(err),
				)
				// panic here, local worker shall not have error
				panic(err)
			}
			offlineSegments.Upsert(sd.applyDelete(ctx, paramtable.GetNodeID(), worker, delRecords, growing)...)
			return nil
		})
	}

	// not error return in apply delete
	_ = eg.Wait()

	sd.distribution.FinishUsage(version)
	offlineSegIDs := offlineSegments.Collect()
	if len(offlineSegIDs) > 0 {
		log.Warn("failed to apply delete, mark segment offline", zap.Int64s("offlineSegments", offlineSegIDs))
		sd.markSegmentOffline(offlineSegIDs...)
	}
}

// applyDelete handles delete record and apply them to corresponding workers.
func (sd *shardDelegator) applyDelete(ctx context.Context, nodeID int64, worker cluster.Worker, delRecords map[int64]DeleteData, entries []SegmentEntry) []int64 {
	var offlineSegments []int64
	log := sd.getLogger(ctx)
	for _, segmentEntry := range entries {
		log := log.With(
			zap.Int64("segmentID", segmentEntry.SegmentID),
			zap.Int64("workerID", nodeID),
		)
		delRecord, ok := delRecords[segmentEntry.SegmentID]
		if ok {
			log.Debug("delegator plan to applyDelete via worker")
			err := retry.Do(ctx, func() error {
				err := worker.Delete(ctx, &querypb.DeleteRequest{
					Base:         commonpbutil.NewMsgBase(commonpbutil.WithTargetID(nodeID)),
					CollectionId: sd.collectionID,
					PartitionId:  segmentEntry.PartitionID,
					VchannelName: sd.vchannelName,
					SegmentId:    segmentEntry.SegmentID,
					PrimaryKeys:  storage.ParsePrimaryKeys2IDs(delRecord.PrimaryKeys),
					Timestamps:   delRecord.Timestamps,
				})
				if errors.Is(err, merr.ErrSegmentNotFound) {
					log.Warn("try to delete data of released segment")
					return nil
				} else if err != nil {
					log.Warn("worker failed to delete on segment",
						zap.Error(err),
					)
					return err
				}
				return nil
			}, retry.Attempts(10))
			if err != nil {
				log.Warn("apply delete for segment failed, marking it offline")
				offlineSegments = append(offlineSegments, segmentEntry.SegmentID)
			}
		}
	}
	return offlineSegments
}

// markSegmentOffline makes segment go offline and waits for QueryCoord to fix.
func (sd *shardDelegator) markSegmentOffline(segmentIDs ...int64) {
	sd.distribution.AddOfflines(segmentIDs...)
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

	segmentIDs = lo.Map(loaded, func(segment segments.Segment, _ int) int64 { return segment.ID() })
	log.Info("load growing segments done", zap.Int64s("segmentIDs", segmentIDs))

	for _, candidate := range loaded {
		sd.pkOracle.Register(candidate, paramtable.GetNodeID())
	}
	sd.addGrowing(lo.Map(loaded, func(segment segments.Segment, _ int) SegmentEntry {
		return SegmentEntry{
			NodeID:        paramtable.GetNodeID(),
			SegmentID:     segment.ID(),
			PartitionID:   segment.Partition(),
			Version:       version,
			TargetVersion: sd.distribution.getTargetVersion(),
		}
	})...)
	return nil
}

// LoadSegments load segments local or remotely depends on the target node.
func (sd *shardDelegator) LoadSegments(ctx context.Context, req *querypb.LoadSegmentsRequest) error {
	log := sd.getLogger(ctx)

	targetNodeID := req.GetDstNodeID()
	// add common log fields
	log = log.With(
		zap.Int64("workID", req.GetDstNodeID()),
		zap.Int64s("segments", lo.Map(req.GetInfos(), func(info *querypb.SegmentLoadInfo, _ int) int64 { return info.GetSegmentID() })),
	)

	worker, err := sd.workerManager.GetWorker(targetNodeID)
	if err != nil {
		log.Warn("delegator failed to find worker", zap.Error(err))
		return err
	}

	// load bloom filter only when candidate not exists
	infos := lo.Filter(req.GetInfos(), func(info *querypb.SegmentLoadInfo, _ int) bool {
		return !sd.pkOracle.Exists(pkoracle.NewCandidateKey(info.GetSegmentID(), info.GetPartitionID(), commonpb.SegmentState_Sealed), targetNodeID)
	})
	candidates, err := sd.loader.LoadBloomFilterSet(ctx, req.GetCollectionID(), req.GetVersion(), infos...)
	if err != nil {
		log.Warn("failed to load bloom filter set for segment", zap.Error(err))
		return err
	}

	req.Base.TargetID = req.GetDstNodeID()
	log.Info("worker loads segments...")
	err = worker.LoadSegments(ctx, req)
	if err != nil {
		log.Warn("worker failed to load segments", zap.Error(err))
		return err
	}
	log.Info("work loads segments done")

	log.Info("load delete...")
	err = sd.loadStreamDelete(ctx, candidates, infos, req.GetDeltaPositions(), targetNodeID, worker)
	if err != nil {
		log.Warn("load stream delete failed", zap.Error(err))
		return err
	}
	log.Info("load delete done")
	// alter distribution
	entries := lo.Map(req.GetInfos(), func(info *querypb.SegmentLoadInfo, _ int) SegmentEntry {
		return SegmentEntry{
			SegmentID:     info.GetSegmentID(),
			PartitionID:   info.GetPartitionID(),
			NodeID:        req.GetDstNodeID(),
			Version:       req.GetVersion(),
			TargetVersion: info.GetReadableVersion(),
		}
	})
	sd.distribution.AddDistributions(entries...)

	return nil
}

func (sd *shardDelegator) loadStreamDelete(ctx context.Context,
	candidates []*pkoracle.BloomFilterSet,
	infos []*querypb.SegmentLoadInfo,
	deltaPositions []*msgpb.MsgPosition,
	targetNodeID int64, worker cluster.Worker) error {
	log := sd.getLogger(ctx)

	idCandidates := lo.SliceToMap(candidates, func(candidate *pkoracle.BloomFilterSet) (int64, *pkoracle.BloomFilterSet) {
		return candidate.ID(), candidate
	})

	sd.deleteMut.Lock()
	defer sd.deleteMut.Unlock()
	// apply buffered delete for new segments
	// no goroutines here since qnv2 has no load merging logic
	for i, info := range infos {
		candidate := idCandidates[info.GetSegmentID()]
		position := info.GetDeltaPosition()
		if position == nil { // for compatibility of rolling upgrade from 2.2.x to 2.3
			position = deltaPositions[i]
		}

		deleteData := &storage.DeleteData{}
		// start position is dml position for segment
		// if this position is before deleteBuffer's safe ts, it means some delete shall be read from msgstream
		if position.GetTimestamp() < sd.deleteBuffer.SafeTs() {
			log.Info("load delete from stream...")
			var err error
			deleteData, err = sd.readDeleteFromMsgstream(ctx, position, sd.deleteBuffer.SafeTs(), candidate)
			if err != nil {
				log.Warn("failed to read delete data from msgstream", zap.Error(err))
				return err
			}
			log.Info("load delete from stream done")
		}

		// list buffered delete
		deleteRecords := sd.deleteBuffer.ListAfter(position.GetTimestamp())
		for _, entry := range deleteRecords {
			for _, record := range entry.Data {
				if record.PartitionID != common.InvalidPartitionID && candidate.Partition() != record.PartitionID {
					continue
				}
				for i, pk := range record.DeleteData.Pks {
					if candidate.MayPkExist(pk) {
						deleteData.Pks = append(deleteData.Pks, pk)
						deleteData.Tss = append(deleteData.Tss, record.DeleteData.Tss[i])
						deleteData.RowCount++
					}
				}
			}
		}
		// if delete count not empty, apply
		if deleteData.RowCount > 0 {
			log.Info("forward delete to worker...", zap.Int64("deleteRowNum", deleteData.RowCount))
			err := worker.Delete(ctx, &querypb.DeleteRequest{
				Base:         commonpbutil.NewMsgBase(commonpbutil.WithTargetID(targetNodeID)),
				CollectionId: info.GetCollectionID(),
				PartitionId:  info.GetPartitionID(),
				SegmentId:    info.GetSegmentID(),
				PrimaryKeys:  storage.ParsePrimaryKeys2IDs(deleteData.Pks),
				Timestamps:   deleteData.Tss,
			})
			if err != nil {
				log.Warn("failed to apply delete when LoadSegment", zap.Error(err))
				return err
			}
		}
	}

	// add candidate after load success
	for _, candidate := range candidates {
		log.Info("register sealed segment bfs into pko candidates",
			zap.Int64("segmentID", candidate.ID()),
		)
		sd.pkOracle.Register(candidate, targetNodeID)
	}

	return nil
}

func (sd *shardDelegator) readDeleteFromMsgstream(ctx context.Context, position *msgpb.MsgPosition, safeTs uint64, candidate *pkoracle.BloomFilterSet) (*storage.DeleteData, error) {

	log := sd.getLogger(ctx).With(
		zap.String("channel", position.ChannelName),
		zap.Int64("segmentID", candidate.ID()),
	)
	stream, err := sd.factory.NewTtMsgStream(ctx)
	if err != nil {
		return nil, err
	}
	defer stream.Close()
	vchannelName := position.ChannelName
	pChannelName := funcutil.ToPhysicalChannel(vchannelName)
	position.ChannelName = pChannelName

	ts, _ := tsoutil.ParseTS(position.Timestamp)

	// Random the subname in case we trying to load same delta at the same time
	subName := fmt.Sprintf("querynode-delta-loader-%d-%d-%d", paramtable.GetNodeID(), sd.collectionID, rand.Int())
	log.Info("from dml check point load delete", zap.Any("position", position), zap.String("subName", subName), zap.Time("positionTs", ts))
	stream.AsConsumer([]string{pChannelName}, subName, mqwrapper.SubscriptionPositionUnknown)

	err = stream.Seek([]*msgpb.MsgPosition{position})
	if err != nil {
		return nil, err
	}

	result := &storage.DeleteData{}
	hasMore := true
	for hasMore {
		select {
		case <-ctx.Done():
			log.Debug("read delta msg from seek position done", zap.Error(ctx.Err()))
			return nil, ctx.Err()
		case msgPack, ok := <-stream.Chan():
			if !ok {
				err = fmt.Errorf("stream channel closed, pChannelName=%v, msgID=%v", pChannelName, position.GetMsgID())
				log.Warn("fail to read delta msg",
					zap.String("pChannelName", pChannelName),
					zap.Binary("msgID", position.GetMsgID()),
					zap.Error(err),
				)
				return nil, err
			}

			if msgPack == nil {
				continue
			}

			for _, tsMsg := range msgPack.Msgs {
				if tsMsg.Type() == commonpb.MsgType_Delete {
					dmsg := tsMsg.(*msgstream.DeleteMsg)
					if dmsg.CollectionID != sd.collectionID || dmsg.GetPartitionID() != candidate.Partition() {
						continue
					}

					for idx, pk := range storage.ParseIDs2PrimaryKeys(dmsg.GetPrimaryKeys()) {
						if candidate.MayPkExist(pk) {
							result.Pks = append(result.Pks, pk)
							result.Tss = append(result.Tss, dmsg.Timestamps[idx])
						}
					}
				}
			}

			// reach safe ts
			if safeTs <= msgPack.EndPositions[0].GetTimestamp() {
				hasMore = false
				break
			}
		}
	}

	return result, nil
}

// ReleaseSegments releases segments local or remotely depending on the target node.
func (sd *shardDelegator) ReleaseSegments(ctx context.Context, req *querypb.ReleaseSegmentsRequest, force bool) error {
	log := sd.getLogger(ctx)

	targetNodeID := req.GetNodeID()
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
	if len(sealed) > 0 {
		sd.pkOracle.Remove(
			pkoracle.WithSegmentIDs(lo.Map(sealed, func(entry SegmentEntry, _ int) int64 { return entry.SegmentID })...),
			pkoracle.WithSegmentType(commonpb.SegmentState_Sealed),
			pkoracle.WithWorkerID(targetNodeID),
		)
	}
	if len(growing) > 0 {
		sd.pkOracle.Remove(
			pkoracle.WithSegmentIDs(lo.Map(growing, func(entry SegmentEntry, _ int) int64 { return entry.SegmentID })...),
			pkoracle.WithSegmentType(commonpb.SegmentState_Growing),
		)
	}

	if !force {
		worker, err := sd.workerManager.GetWorker(targetNodeID)
		if err != nil {
			log.Warn("delegator failed to find worker",
				zap.Error(err),
			)
			return err
		}
		req.Base.TargetID = targetNodeID
		err = worker.ReleaseSegments(ctx, req)
		if err != nil {
			log.Warn("worker failed to release segments",
				zap.Error(err),
			)
		}
		return err
	}

	return nil
}

func (sd *shardDelegator) SyncTargetVersion(newVersion int64, growingInTarget []int64, sealedInTarget []int64) {
	sd.distribution.SyncTargetVersion(newVersion, growingInTarget, sealedInTarget)
}

func (sd *shardDelegator) GetTargetVersion() int64 {
	return sd.distribution.getTargetVersion()
}

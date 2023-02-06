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

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
	"github.com/milvus-io/milvus/internal/querynodev2/cluster"
	"github.com/milvus-io/milvus/internal/querynodev2/pkoracle"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/commonpbutil"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/samber/lo"
	"go.uber.org/zap"
)

// delegator data related part

// InsertData
type InsertData struct {
	RowIDs        []int64
	PrimaryKeys   []storage.PrimaryKey
	Timestamps    []uint64
	InsertRecord  *segcorepb.InsertRecord
	StartPosition *internalpb.MsgPosition
	PartitionID   int64
}

type DeleteData struct {
	PartitionID int64
	PrimaryKeys []storage.PrimaryKey
	Timestamps  []uint64
	RowCount    int64
}

func (sd *shardDelegator) newGrowing(segmentID int64, insertData *InsertData) segments.Segment {
	log := sd.getLogger(context.Background()).With(zap.Int64("segmentID", segmentID))

	// try add partition
	if sd.collection.GetLoadType() == loadTypeCollection {
		sd.collection.AddPartition(insertData.PartitionID)
	}

	segment, err := segments.NewSegment(sd.collection, segmentID, insertData.PartitionID, sd.collectionID, sd.vchannelName, segments.SegmentTypeGrowing, 0, insertData.StartPosition, insertData.StartPosition)
	if err != nil {
		log.Error("failed to create new segment", zap.Error(err))
		panic(err)
	}

	sd.pkOracle.Register(segment, paramtable.GetNodeID())

	sd.segmentManager.Put(segments.SegmentTypeGrowing, segment)
	sd.addGrowing(SegmentEntry{
		NodeID:      paramtable.GetNodeID(),
		SegmentID:   segmentID,
		PartitionID: insertData.PartitionID,
		Version:     0,
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
			// panic here, insert failure
			panic(err)
		}
		growing.UpdateBloomFilter(insertData.PrimaryKeys)

		log.Debug("insert into growing segment",
			zap.Int64("collectionID", growing.Collection()),
			zap.Int64("segmentID", segmentID),
			zap.Int("rowCount", len(insertData.RowIDs)),
			zap.Uint64s("tss", insertData.Timestamps),
		)
	}
}

// ProcessDelete handles delete data in delegator.
// delegator puts deleteData into buffer first,
// then dispatch data to segments acoording to the result of pkOracle.
func (sd *shardDelegator) ProcessDelete(deleteData []*DeleteData, ts uint64) {
	log := sd.getLogger(context.Background())

	log.Debug("start to process delete", zap.Uint64("ts", ts))
	// add deleteData into buffer.
	cacheItems := make([]deleteBufferItem, 0, len(deleteData))
	for _, entry := range deleteData {
		cacheItems = append(cacheItems, deleteBufferItem{
			partitionID: entry.PartitionID,
			deleteData: storage.DeleteData{
				Pks:      entry.PrimaryKeys,
				Tss:      entry.Timestamps,
				RowCount: entry.RowCount,
			},
		})
	}
	sd.deleteBuffer.Cache(ts, cacheItems)

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
				delRecords[segmentID] = delRecord
			}
		}
	}

	applyDelete := func(nodeID int64, worker cluster.Worker, entries []SegmentEntry) {
		for _, segmentEntry := range entries {
			delRecord, ok := delRecords[segmentEntry.SegmentID]
			if ok {
				log.Debug("delegator plan to applyDelete via worker", zap.Int64("nodeID", nodeID), zap.Int64("segmentID", segmentEntry.SegmentID))
				err := worker.Delete(context.Background(), &querypb.DeleteRequest{
					Base:         commonpbutil.NewMsgBase(commonpbutil.WithTargetID(nodeID)),
					CollectionId: sd.collectionID,
					PartitionId:  segmentEntry.PartitionID,
					VchannelName: sd.vchannelName,
					SegmentId:    segmentEntry.SegmentID,
					PrimaryKeys:  storage.ParsePrimaryKeys2IDs(delRecord.PrimaryKeys),
					Timestamps:   delRecord.Timestamps,
				})
				if err != nil {
					log.Warn("worker failed to delete on segment",
						zap.Int64("segmentID", segmentEntry.SegmentID),
						zap.Int64("workerID", nodeID),
						zap.Error(err),
					)
					continue
				}
			}
		}
	}

	sealed, growing, version := sd.distribution.GetCurrent()
	defer sd.distribution.FinishUsage(version)
	for _, entry := range sealed {
		worker, err := sd.workerManager.GetWorker(entry.NodeID)
		if err != nil {
			log.Warn("failed to get worker",
				zap.Int64("nodeID", paramtable.GetNodeID()),
				zap.Error(err),
			)
			// skip if node down
			// delete will be processed after loaded again
			continue
		}
		applyDelete(entry.NodeID, worker, entry.Segments)
	}
	if len(growing) > 0 {
		worker, err := sd.workerManager.GetWorker(paramtable.GetNodeID())
		if err != nil {
			log.Error("failed to get worker(local)",
				zap.Int64("nodeID", paramtable.GetNodeID()),
				zap.Error(err),
			)
			// panic here, local worker shall not have error
			panic(err)
		}
		applyDelete(paramtable.GetNodeID(), worker, growing)
	}
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

	loaded, err := sd.loader.Load(ctx, sd.collectionID, segments.SegmentTypeGrowing, version, infos...)
	if err != nil {
		log.Warn("failed to load growing segment", zap.Error(err))
		for _, segment := range loaded {
			segments.DeleteSegment(segment.(*segments.LocalSegment))
		}
		return err
	}
	for _, candidate := range loaded {
		sd.pkOracle.Register(candidate, paramtable.GetNodeID())
	}
	sd.segmentManager.Put(segments.SegmentTypeGrowing, loaded...)
	sd.addGrowing(lo.Map(loaded, func(segment segments.Segment, _ int) SegmentEntry {
		return SegmentEntry{
			NodeID:      paramtable.GetNodeID(),
			SegmentID:   segment.ID(),
			PartitionID: segment.Partition(),
			Version:     version,
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
		zap.Int64("dstNodeID", req.GetDstNodeID()),
		zap.Int64s("segments", lo.Map(req.GetInfos(), func(info *querypb.SegmentLoadInfo, _ int) int64 { return info.GetSegmentID() })),
	)

	worker, err := sd.workerManager.GetWorker(targetNodeID)
	if err != nil {
		log.Warn("delegator failed to find worker",
			zap.Error(err),
		)
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
	err = worker.LoadSegments(ctx, req)
	if err != nil {
		log.Warn("worker failed to load segments",
			zap.Error(err),
		)
		return err
	}
	// add candidate after load success
	for _, candidate := range candidates {
		log.Info("register sealed segment bfs into pko candidates",
			zap.Int64("segmentID", candidate.ID()),
		)
		sd.pkOracle.Register(candidate, targetNodeID)
	}
	idCandidates := lo.SliceToMap(candidates, func(candidate *pkoracle.BloomFilterSet) (int64, *pkoracle.BloomFilterSet) {
		return candidate.ID(), candidate
	})

	// apply buffered delete for new segments
	for _, info := range infos {
		candidate := idCandidates[info.GetSegmentID()]
		deleteRecords := sd.deleteBuffer.List(info.GetStartPosition().GetTimestamp())
		if len(deleteRecords) > 0 {
			merged := &storage.DeleteData{}
			for _, entry := range deleteRecords {
				for _, record := range entry {
					if record.partitionID != common.InvalidPartitionID && candidate.Partition() != record.partitionID {
						continue
					}
					for i, pk := range record.deleteData.Pks {
						if candidate.MayPkExist(pk) {
							merged.Pks = append(merged.Pks, pk)
							merged.Tss = append(merged.Tss, record.deleteData.Tss[i])
							merged.RowCount++
						}
					}
				}
			}
			if merged.RowCount > 0 {
				worker.Delete(ctx, &querypb.DeleteRequest{
					Base:         commonpbutil.NewMsgBase(commonpbutil.WithTargetID(req.GetDstNodeID())),
					CollectionId: info.GetCollectionID(),
					PartitionId:  info.GetPartitionID(),
					SegmentId:    info.GetSegmentID(),
					PrimaryKeys:  storage.ParsePrimaryKeys2IDs(merged.Pks),
					Timestamps:   merged.Tss,
				})
			}
		}
	}

	// alter distribution
	entries := lo.Map(req.GetInfos(), func(info *querypb.SegmentLoadInfo, _ int) SegmentEntry {
		return SegmentEntry{
			SegmentID:   info.GetSegmentID(),
			PartitionID: info.GetPartitionID(),
			NodeID:      req.GetDstNodeID(),
			Version:     req.GetVersion(),
		}
	})
	removed := sd.distribution.AddDistributions(entries...)

	// call worker release async
	if len(removed) > 0 {
		go func() {
			worker, err := sd.workerManager.GetWorker(paramtable.GetNodeID())
			if err != nil {
				log.Warn("failed to get local worker when try to release related growing", zap.Error(err))
				return
			}
			err = worker.ReleaseSegments(context.Background(), &querypb.ReleaseSegmentsRequest{
				Base:         commonpbutil.NewMsgBase(commonpbutil.WithTargetID(paramtable.GetNodeID())),
				CollectionID: sd.collectionID,
				NodeID:       paramtable.GetNodeID(),
				Scope:        querypb.DataScope_Streaming,
				SegmentIDs:   removed,
				Shard:        sd.vchannelName,
				NeedTransfer: false,
			})
			if err != nil {
				log.Warn("failed to call release segments(local)", zap.Error(err))
			}
		}()
	}

	return nil
}

// ReleaseSegments releases segments local or remotely depends ont the target node.
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
	if force {
		targetNodeID = wildcardNodeID
	}

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

// TruncateDeleteBuffer truncates delete buffer base on channel checkpoint.
func (sd *shardDelegator) TruncateDeleteBuffer(pos *internalpb.MsgPosition) {
	sd.deleteBuffer.TruncateBefore(pos.GetTimestamp())
}

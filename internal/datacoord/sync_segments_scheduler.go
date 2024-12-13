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
	"sync"
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/logutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type SyncSegmentsScheduler struct {
	ctx        context.Context
	cancelFunc context.CancelFunc
	quit       chan struct{}
	wg         sync.WaitGroup

	meta           *meta
	channelManager ChannelManager
	sessions       session.DataNodeManager
}

func newSyncSegmentsScheduler(m *meta, channelManager ChannelManager, sessions session.DataNodeManager) *SyncSegmentsScheduler {
	ctx, cancel := context.WithCancel(context.Background())
	return &SyncSegmentsScheduler{
		ctx:            ctx,
		cancelFunc:     cancel,
		quit:           make(chan struct{}),
		wg:             sync.WaitGroup{},
		meta:           m,
		channelManager: channelManager,
		sessions:       sessions,
	}
}

func (sss *SyncSegmentsScheduler) Start() {
	sss.quit = make(chan struct{})
	sss.wg.Add(1)

	go func() {
		defer logutil.LogPanic()
		ticker := time.NewTicker(Params.DataCoordCfg.SyncSegmentsInterval.GetAsDuration(time.Second))
		defer sss.wg.Done()

		for {
			select {
			case <-sss.quit:
				log.Info("sync segments scheduler quit")
				ticker.Stop()
				return
			case <-ticker.C:
				sss.SyncSegmentsForCollections(sss.ctx)
			}
		}
	}()
	log.Info("SyncSegmentsScheduler started...")
}

func (sss *SyncSegmentsScheduler) Stop() {
	sss.cancelFunc()
	close(sss.quit)
	sss.wg.Wait()
}

func (sss *SyncSegmentsScheduler) SyncSegmentsForCollections(ctx context.Context) {
	collIDs := sss.meta.ListCollections()
	for _, collID := range collIDs {
		collInfo := sss.meta.GetCollection(collID)
		if collInfo == nil {
			log.Warn("collection info is nil, skip it", zap.Int64("collectionID", collID))
			continue
		}
		pkField, err := typeutil.GetPrimaryFieldSchema(collInfo.Schema)
		if err != nil {
			log.Warn("get primary field from schema failed", zap.Int64("collectionID", collID),
				zap.Error(err))
			continue
		}
		for _, channelName := range collInfo.VChannelNames {
			nodeID, err := sss.channelManager.FindWatcher(ctx, channelName)
			if err != nil {
				log.Warn("find watcher for channel failed", zap.Int64("collectionID", collID),
					zap.String("channelName", channelName), zap.Error(err))
				continue
			}
			if err := sss.SyncSegments(ctx, collID, channelName, nodeID, pkField.GetFieldID()); err != nil {
				log.Warn("sync segment with channel failed, retry next ticker",
					zap.Int64("collectionID", collID),
					zap.String("channel", channelName),
					zap.Error(err))
				continue
			}
		}
	}
}

func (sss *SyncSegmentsScheduler) SyncSegments(ctx context.Context, collectionID int64, channelName string, nodeID, pkFieldID int64) error {
	log := log.With(zap.Int64("collectionID", collectionID),
		zap.String("channelName", channelName), zap.Int64("nodeID", nodeID))
	// sync all healthy segments, but only check flushed segments on datanode. Because L0 growing segments may not in datacoord's meta.
	// upon receiving the SyncSegments request, the datanode's segment state may have already transitioned from Growing/Flushing
	// to Flushed, so the view must include this segment.
	channelSegments := sss.meta.SelectSegments(ctx, WithChannel(channelName), SegmentFilterFunc(func(info *SegmentInfo) bool {
		return info.GetLevel() != datapb.SegmentLevel_L0 && isSegmentHealthy(info)
	}))

	partitionSegments := lo.GroupBy(channelSegments, func(segment *SegmentInfo) int64 {
		return segment.GetPartitionID()
	})
	for partitionID, segments := range partitionSegments {
		req := &datapb.SyncSegmentsRequest{
			ChannelName:  channelName,
			PartitionId:  partitionID,
			CollectionId: collectionID,
			SegmentInfos: make(map[int64]*datapb.SyncSegmentInfo),
		}

		for _, seg := range segments {
			req.SegmentInfos[seg.ID] = &datapb.SyncSegmentInfo{
				SegmentId: seg.GetID(),
				State:     seg.GetState(),
				Level:     seg.GetLevel(),
				NumOfRows: seg.GetNumOfRows(),
			}
			statsLogs := make([]*datapb.Binlog, 0)
			for _, statsLog := range seg.GetStatslogs() {
				if statsLog.GetFieldID() == pkFieldID {
					statsLogs = append(statsLogs, statsLog.GetBinlogs()...)
				}
			}
			req.SegmentInfos[seg.ID].PkStatsLog = &datapb.FieldBinlog{
				FieldID: pkFieldID,
				Binlogs: statsLogs,
			}
		}

		if err := sss.sessions.SyncSegments(ctx, nodeID, req); err != nil {
			log.Warn("fail to sync segments with node", zap.Error(err))
			return err
		}
		log.Info("sync segments success", zap.Int64("partitionID", partitionID), zap.Int64s("segments", lo.Map(segments, func(t *SegmentInfo, i int) int64 {
			return t.GetID()
		})))
	}

	return nil
}

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
	"sync"
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/logutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type SyncSegmentsScheduler struct {
	quit chan struct{}
	wg   sync.WaitGroup

	meta           *meta
	channelManager ChannelManager
	sessions       SessionManager
}

func newSyncSegmentsScheduler(m *meta, channelManager ChannelManager, sessions SessionManager) *SyncSegmentsScheduler {
	return &SyncSegmentsScheduler{
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
				sss.SyncSegmentsForCollections()
			}
		}
	}()
	log.Info("SyncSegmentsScheduler started...")
}

func (sss *SyncSegmentsScheduler) Stop() {
	close(sss.quit)
	sss.wg.Wait()
}

func (sss *SyncSegmentsScheduler) SyncSegmentsForCollections() {
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
			nodeID, err := sss.channelManager.FindWatcher(channelName)
			if err != nil {
				log.Warn("find watcher for channel failed", zap.Int64("collectionID", collID),
					zap.String("channelName", channelName), zap.Error(err))
				continue
			}
			for _, partitionID := range collInfo.Partitions {
				if err := sss.SyncSegments(collID, partitionID, channelName, nodeID, pkField.GetFieldID()); err != nil {
					log.Warn("sync segment with channel failed, retry next ticker",
						zap.Int64("collectionID", collID),
						zap.Int64("partitionID", partitionID),
						zap.String("channel", channelName),
						zap.Error(err))
					continue
				}
			}
		}
	}
}

func (sss *SyncSegmentsScheduler) SyncSegments(collectionID, partitionID int64, channelName string, nodeID, pkFieldID int64) error {
	log := log.With(zap.Int64("collectionID", collectionID), zap.Int64("partitionID", partitionID),
		zap.String("channelName", channelName), zap.Int64("nodeID", nodeID))
	segments := sss.meta.SelectSegments(WithChannel(channelName), SegmentFilterFunc(func(info *SegmentInfo) bool {
		return info.GetPartitionID() == partitionID && isSegmentHealthy(info)
	}))
	req := &datapb.SyncSegmentsRequest{
		ChannelName:  channelName,
		PartitionId:  partitionID,
		CollectionId: collectionID,
		SegmentInfos: make(map[int64]*datapb.SyncSegmentInfo),
	}

	for _, seg := range segments {
		for _, statsLog := range seg.GetStatslogs() {
			if statsLog.GetFieldID() == pkFieldID {
				req.SegmentInfos[seg.ID] = &datapb.SyncSegmentInfo{
					SegmentId:  seg.GetID(),
					PkStatsLog: statsLog,
					State:      seg.GetState(),
					Level:      seg.GetLevel(),
					NumOfRows:  seg.GetNumOfRows(),
				}
			}
		}
	}

	if err := sss.sessions.SyncSegments(nodeID, req); err != nil {
		log.Warn("fail to sync segments with node", zap.Error(err))
		return err
	}
	log.Info("sync segments success", zap.Int64s("segments", lo.Map(segments, func(t *SegmentInfo, i int) int64 {
		return t.GetID()
	})))
	return nil
}

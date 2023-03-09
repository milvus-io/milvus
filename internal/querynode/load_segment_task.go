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

package querynode

import (
	"context"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus/internal/log"
	queryPb "github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/samber/lo"
)

type loadSegmentsTask struct {
	baseTask
	req  *queryPb.LoadSegmentsRequest
	node *QueryNode
}

// loadSegmentsTask
func (l *loadSegmentsTask) PreExecute(ctx context.Context) error {
	log.Ctx(ctx).Info("LoadSegmentTask PreExecute start")
	var err error
	// init meta
	collectionID := l.req.GetCollectionID()
	l.node.metaReplica.addCollection(collectionID, l.req.GetSchema())
	for _, partitionID := range l.req.GetLoadMeta().GetPartitionIDs() {
		err = l.node.metaReplica.addPartition(collectionID, partitionID)
		if err != nil {
			return err
		}
	}

	// filter segments that are already loaded in this querynode
	var filteredInfos []*queryPb.SegmentLoadInfo
	for _, info := range l.req.Infos {
		has, err := l.node.metaReplica.hasSegment(info.SegmentID, segmentTypeSealed)
		if err != nil {
			return err
		}
		if !has {
			filteredInfos = append(filteredInfos, info)
		} else {
			log.Info("ignore segment that is already loaded", zap.Int64("collectionID", info.CollectionID), zap.Int64("segmentID", info.SegmentID))
		}
	}
	l.req.Infos = filteredInfos
	log.Info("LoadSegmentTask PreExecute done")
	return nil
}

func (l *loadSegmentsTask) Execute(ctx context.Context) error {
	log.Ctx(ctx).Info("LoadSegmentTask Execute start")

	if len(l.req.Infos) == 0 {
		log.Info("all segments loaded")
		return nil
	}

	segmentIDs := lo.Map(l.req.Infos, func(info *queryPb.SegmentLoadInfo, idx int) UniqueID { return info.SegmentID })
	l.node.metaReplica.addSegmentsLoadingList(segmentIDs)
	defer l.node.metaReplica.removeSegmentsLoadingList(segmentIDs)
	loadDoneSegmentIDs, loadErr := l.node.loader.LoadSegment(l.ctx, l.req, segmentTypeSealed)
	if len(loadDoneSegmentIDs) > 0 {
		vchanName := make([]string, 0)
		for _, deltaPosition := range l.req.DeltaPositions {
			vchanName = append(vchanName, deltaPosition.ChannelName)
		}

		// TODO delta channel need to released 1. if other watchDeltaChannel fail 2. when segment release
		err := l.watchDeltaChannel(vchanName)
		if err != nil {
			// roll back
			for _, segment := range l.req.Infos {
				l.node.metaReplica.removeSegment(segment.SegmentID, segmentTypeSealed)
			}
			log.Warn("failed to watch Delta channel while load segment", zap.Int64("collectionID", l.req.CollectionID),
				zap.Int64("replicaID", l.req.ReplicaID), zap.Error(err))
			return err
		}

		runningGroup, groupCtx := errgroup.WithContext(l.ctx)
		for _, deltaPosition := range l.req.DeltaPositions {
			pos := deltaPosition
			runningGroup.Go(func() error {
				// reload data from dml channel
				return l.node.loader.FromDmlCPLoadDelete(groupCtx, l.req.CollectionID, pos,
					lo.FilterMap(l.req.Infos, func(info *queryPb.SegmentLoadInfo, _ int) (int64, bool) {
						return info.GetSegmentID(), funcutil.SliceContain(loadDoneSegmentIDs, info.SegmentID) && info.GetInsertChannel() == pos.GetChannelName()
					}))
			})
		}
		err = runningGroup.Wait()
		if err != nil {
			for _, segment := range l.req.Infos {
				l.node.metaReplica.removeSegment(segment.SegmentID, segmentTypeSealed)
			}
			for _, vchannel := range vchanName {
				l.node.dataSyncService.removeEmptyFlowGraphByChannel(l.req.CollectionID, vchannel)
			}
			log.Warn("failed to load delete data while load segment", zap.Int64("collectionID", l.req.CollectionID),
				zap.Int64("replicaID", l.req.ReplicaID), zap.Error(err))
			return err
		}
	}
	if loadErr != nil {
		log.Warn("failed to load segment", zap.Int64("collectionID", l.req.CollectionID),
			zap.Int64("replicaID", l.req.ReplicaID), zap.Error(loadErr))
		return loadErr
	}

	log.Info("LoadSegmentTask Execute done", zap.Int64("collectionID", l.req.CollectionID),
		zap.Int64("replicaID", l.req.ReplicaID))
	return nil
}

// internal helper function to subscribe delta channel
func (l *loadSegmentsTask) watchDeltaChannel(dmlChannels []string) error {
	var (
		collectionID    = l.req.CollectionID
		vDeltaChannels  []string
		VPDeltaChannels = make(map[string]string)
	)
	log := log.With(
		zap.Int64("collectionID", collectionID),
		zap.Strings("dmlChannels", dmlChannels),
	)
	for _, v := range dmlChannels {
		dc, err := funcutil.ConvertChannelName(v, Params.CommonCfg.RootCoordDml.GetValue(), Params.CommonCfg.RootCoordDelta.GetValue())
		if err != nil {
			log.Warn("watchDeltaChannels, failed to convert deltaChannel from dmlChannel",
				zap.String("dmlChannel", v),
				zap.Error(err),
			)
			return err
		}
		p := funcutil.ToPhysicalChannel(dc)
		vDeltaChannels = append(vDeltaChannels, dc)
		VPDeltaChannels[dc] = p
	}
	log.Info("Starting WatchDeltaChannels ...", zap.Strings("deltaChannels", vDeltaChannels))

	coll, err := l.node.metaReplica.getCollectionByID(collectionID)
	if err != nil {
		return err
	}

	// add collection meta and fg with mutex protection.
	channel2FlowGraph, err := l.node.dataSyncService.addFlowGraphsForDeltaChannels(collectionID, vDeltaChannels, VPDeltaChannels)
	if err != nil {
		log.Warn("watchDeltaChannel, failed to add flowGraph for deltaChannels",
			zap.Strings("deltaChannels", vDeltaChannels),
			zap.Error(err))
		return err
	}

	if len(channel2FlowGraph) == 0 {
		log.Warn("all delta channels have been added before", zap.Strings("deltaChannels", vDeltaChannels))
		return nil
	}

	// use valid channel names only.
	vDeltaChannels = make([]string, 0, len(channel2FlowGraph))
	for ch := range channel2FlowGraph {
		vDeltaChannels = append(vDeltaChannels, ch)
	}
	defer func() {
		if err != nil {
			for _, vDeltaChannel := range vDeltaChannels {
				coll.removeVDeltaChannel(vDeltaChannel)
			}
		}
	}()

	log.Info("watchDeltaChannel, add flowGraph for deltaChannel success", zap.Strings("deltaChannels", vDeltaChannels))

	// create tSafe
	for _, channel := range vDeltaChannels {
		l.node.tSafeReplica.addTSafe(channel)
	}

	// add tsafe watch in query shard if exists, we find no way to handle it if query shard not exist
	for _, channel := range vDeltaChannels {
		dmlChannel, err := funcutil.ConvertChannelName(channel, Params.CommonCfg.RootCoordDelta.GetValue(), Params.CommonCfg.RootCoordDml.GetValue())
		if err != nil {
			log.Error("failed to convert delta channel to dml", zap.String("deltaChannel", channel), zap.Error(err))
			panic(err)
		}
		err = l.node.queryShardService.addQueryShard(collectionID, dmlChannel, l.req.GetReplicaID())
		if err != nil {
			log.Error("failed to add shard Service to query shard",
				zap.String("dmlChannel", dmlChannel),
				zap.String("deltaChannel", channel),
				zap.Error(err))
			panic(err)
		}
	}

	// start flow graphs
	for _, fg := range channel2FlowGraph {
		fg.flowGraph.Start()
	}

	log.Info("WatchDeltaChannels done", zap.Strings("deltaChannels", vDeltaChannels))
	return nil
}

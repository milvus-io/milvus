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
	"errors"
	"runtime/debug"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	queryPb "github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type task interface {
	ID() UniqueID // return ReqID
	Ctx() context.Context
	Timestamp() Timestamp
	PreExecute(ctx context.Context) error
	Execute(ctx context.Context) error
	PostExecute(ctx context.Context) error
	WaitToFinish() error
	Notify(err error)
	OnEnqueue() error
}

type baseTask struct {
	done chan error
	ctx  context.Context
	id   UniqueID
	ts   Timestamp
}

func (b *baseTask) Ctx() context.Context {
	return b.ctx
}

func (b *baseTask) OnEnqueue() error {
	return nil
}

func (b *baseTask) ID() UniqueID {
	return b.id
}

func (b *baseTask) Timestamp() Timestamp {
	return b.ts
}

func (b *baseTask) PreExecute(ctx context.Context) error {
	return nil
}

func (b *baseTask) PostExecute(ctx context.Context) error {
	return nil
}

func (b *baseTask) WaitToFinish() error {
	err := <-b.done
	return err
}

func (b *baseTask) Notify(err error) {
	b.done <- err
}

type releaseCollectionTask struct {
	baseTask
	req  *queryPb.ReleaseCollectionRequest
	node *QueryNode
}

type releasePartitionsTask struct {
	baseTask
	req  *queryPb.ReleasePartitionsRequest
	node *QueryNode
}

func (r *releaseCollectionTask) Execute(ctx context.Context) error {
	log.Info("Execute release collection task", zap.Any("collectionID", r.req.CollectionID))

	collection, err := r.node.metaReplica.getCollectionByID(r.req.CollectionID)
	if err != nil {
		if errors.Is(err, ErrCollectionNotFound) {
			log.Info("collection has been released",
				zap.Int64("collectionID", r.req.GetCollectionID()),
				zap.Error(err),
			)
			return nil
		}
		return err
	}
	// set release time
	log.Info("set release time", zap.Any("collectionID", r.req.CollectionID))
	collection.setReleaseTime(r.req.Base.Timestamp, true)

	// remove all flow graphs of the target collection
	vChannels := collection.getVChannels()
	vDeltaChannels := collection.getVDeltaChannels()
	r.node.dataSyncService.removeFlowGraphsByDMLChannels(vChannels)
	r.node.dataSyncService.removeFlowGraphsByDeltaChannels(vDeltaChannels)

	// remove all tSafes of the target collection
	for _, channel := range vChannels {
		r.node.tSafeReplica.removeTSafe(channel)
	}
	for _, channel := range vDeltaChannels {
		r.node.tSafeReplica.removeTSafe(channel)
	}
	log.Info("Release tSafe in releaseCollectionTask",
		zap.Int64("collectionID", r.req.CollectionID),
		zap.Strings("vChannels", vChannels),
		zap.Strings("vDeltaChannels", vDeltaChannels),
	)

	r.node.metaReplica.removeExcludedSegments(r.req.CollectionID)
	r.node.queryShardService.releaseCollection(r.req.CollectionID)
	r.node.ShardClusterService.releaseCollection(r.req.CollectionID)
	err = r.node.metaReplica.removeCollection(r.req.CollectionID)
	if err != nil {
		return err
	}

	debug.FreeOSMemory()
	log.Info("ReleaseCollection done", zap.Int64("collectionID", r.req.CollectionID))
	return nil
}

// releasePartitionsTask
func (r *releasePartitionsTask) Execute(ctx context.Context) error {
	log.Info("Execute release partition task",
		zap.Int64("collectionID", r.req.GetCollectionID()),
		zap.Int64s("partitionIDs", r.req.GetPartitionIDs()))

	coll, err := r.node.metaReplica.getCollectionByID(r.req.CollectionID)
	if err != nil {
		// skip error if collection not found, do clean up job below
		log.Warn("failed to get collection for release partitions", zap.Int64("collectionID", r.req.GetCollectionID()),
			zap.Int64s("partitionIDs", r.req.GetPartitionIDs()))

	}
	log.Info("start release partition", zap.Int64("collectionID", r.req.GetCollectionID()), zap.Int64s("partitionIDs", r.req.GetPartitionIDs()))

	// shall be false if coll is nil
	releaseAll := r.isAllPartitionsReleased(coll)

	if releaseAll {
		// set release time
		log.Info("set release time", zap.Int64("collectionID", r.req.CollectionID))
		coll.setReleaseTime(r.req.Base.Timestamp, true)

		// remove all flow graphs of the target collection
		vChannels := coll.getVChannels()
		vDeltaChannels := coll.getVDeltaChannels()
		r.node.dataSyncService.removeFlowGraphsByDMLChannels(vChannels)
		r.node.dataSyncService.removeFlowGraphsByDeltaChannels(vDeltaChannels)

		// remove all tSafes of the target collection
		for _, channel := range vChannels {
			r.node.tSafeReplica.removeTSafe(channel)
		}
		for _, channel := range vDeltaChannels {
			r.node.tSafeReplica.removeTSafe(channel)
		}
		log.Info("Release tSafe in releaseCollectionTask",
			zap.Int64("collectionID", r.req.CollectionID),
			zap.Strings("vChannels", vChannels),
			zap.Strings("vDeltaChannels", vDeltaChannels),
		)

		r.node.metaReplica.removeExcludedSegments(r.req.CollectionID)
		r.node.queryShardService.releaseCollection(r.req.CollectionID)
		r.node.ShardClusterService.releaseCollection(r.req.CollectionID)
		err = r.node.metaReplica.removeCollection(r.req.CollectionID)
		if err != nil {
			log.Warn("failed to remove collection", zap.Int64("collectionID", r.req.GetCollectionID()),
				zap.Int64s("partitionIDs", r.req.GetPartitionIDs()), zap.Error(err))
		}
	} else {
		for _, id := range r.req.PartitionIDs {
			// remove partition from streaming and historical
			hasPartition := r.node.metaReplica.hasPartition(id)
			if hasPartition {
				err := r.node.metaReplica.removePartition(id)
				if err != nil {
					// not return, try to release all partitions
					log.Warn(err.Error())
				}
			}
		}
	}

	log.Info("Release partition task done",
		zap.Int64("collectionID", r.req.CollectionID),
		zap.Int64s("partitionIDs", r.req.PartitionIDs))
	return nil
}

func (r *releasePartitionsTask) isAllPartitionsReleased(coll *Collection) bool {
	if coll == nil {
		return false
	}
	if len(r.req.GetPartitionIDs()) < len(coll.partitionIDs) && len(coll.partitionIDs) > 0 {
		return false
	}
	parts := make(typeutil.UniqueSet)
	for _, partID := range r.req.GetPartitionIDs() {
		parts.Insert(partID)
	}

	return parts.Contain(coll.partitionIDs...)
}

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

package rootcoord

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	ms "github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
)

//go:generate mockery --name=GarbageCollector --outpkg=mockrootcoord --filename=garbage_collector.go --with-expecter --testonly
type GarbageCollector interface {
	ReDropCollection(collMeta *model.Collection, ts Timestamp)
	RemoveCreatingCollection(collMeta *model.Collection)
	ReDropPartition(dbID int64, pChannels, vchannels []string, partition *model.Partition, ts Timestamp)
	RemoveCreatingPartition(dbID int64, partition *model.Partition, ts Timestamp)
	GcCollectionData(ctx context.Context, coll *model.Collection) (ddlTs Timestamp, err error)
	GcPartitionData(ctx context.Context, pChannels, vchannels []string, partition *model.Partition) (ddlTs Timestamp, err error)
}

type bgGarbageCollector struct {
	s *Core
}

func newBgGarbageCollector(s *Core) *bgGarbageCollector {
	return &bgGarbageCollector{s: s}
}

func (c *bgGarbageCollector) ReDropCollection(collMeta *model.Collection, ts Timestamp) {
	// TODO: remove this after data gc can be notified by rpc.
	c.s.chanTimeTick.addDmlChannels(collMeta.PhysicalChannelNames...)

	redo := newBaseRedoTask(c.s.stepExecutor)
	redo.AddAsyncStep(&releaseCollectionStep{
		baseStep:     baseStep{core: c.s},
		collectionID: collMeta.CollectionID,
	})
	redo.AddAsyncStep(&dropIndexStep{
		baseStep: baseStep{core: c.s},
		collID:   collMeta.CollectionID,
		partIDs:  nil,
	})
	redo.AddAsyncStep(&deleteCollectionDataStep{
		baseStep: baseStep{core: c.s},
		coll:     collMeta,
		isSkip:   !Params.CommonCfg.TTMsgEnabled.GetAsBool(),
	})
	redo.AddAsyncStep(&removeDmlChannelsStep{
		baseStep:  baseStep{core: c.s},
		pChannels: collMeta.PhysicalChannelNames,
	})
	redo.AddAsyncStep(newConfirmGCStep(c.s, collMeta.CollectionID, allPartition))
	redo.AddAsyncStep(&deleteCollectionMetaStep{
		baseStep:     baseStep{core: c.s},
		collectionID: collMeta.CollectionID,
		// This ts is less than the ts when we notify data nodes to drop collection, but it's OK since we have already
		// marked this collection as deleted. If we want to make this ts greater than the notification's ts, we should
		// wrap a step who will have these three children and connect them with ts.
		ts: ts,
	})

	// err is ignored since no sync steps will be executed.
	_ = redo.Execute(context.Background())
}

func (c *bgGarbageCollector) RemoveCreatingCollection(collMeta *model.Collection) {
	// TODO: remove this after data gc can be notified by rpc.
	c.s.chanTimeTick.addDmlChannels(collMeta.PhysicalChannelNames...)

	redo := newBaseRedoTask(c.s.stepExecutor)

	redo.AddAsyncStep(&unwatchChannelsStep{
		baseStep:     baseStep{core: c.s},
		collectionID: collMeta.CollectionID,
		channels: collectionChannels{
			virtualChannels:  collMeta.VirtualChannelNames,
			physicalChannels: collMeta.PhysicalChannelNames,
		},
		isSkip: !Params.CommonCfg.TTMsgEnabled.GetAsBool(),
	})
	redo.AddAsyncStep(&removeDmlChannelsStep{
		baseStep:  baseStep{core: c.s},
		pChannels: collMeta.PhysicalChannelNames,
	})
	redo.AddAsyncStep(&deleteCollectionMetaStep{
		baseStep:     baseStep{core: c.s},
		collectionID: collMeta.CollectionID,
		// When we undo createCollectionTask, this ts may be less than the ts when unwatch channels.
		ts: collMeta.CreateTime,
	})
	// err is ignored since no sync steps will be executed.
	_ = redo.Execute(context.Background())
}

func (c *bgGarbageCollector) ReDropPartition(dbID int64, pChannels, vchannels []string, partition *model.Partition, ts Timestamp) {
	// TODO: remove this after data gc can be notified by rpc.
	c.s.chanTimeTick.addDmlChannels(pChannels...)

	redo := newBaseRedoTask(c.s.stepExecutor)
	redo.AddAsyncStep(&deletePartitionDataStep{
		baseStep:  baseStep{core: c.s},
		pchans:    pChannels,
		vchans:    vchannels,
		partition: partition,
		isSkip:    !Params.CommonCfg.TTMsgEnabled.GetAsBool(),
	})
	redo.AddAsyncStep(&removeDmlChannelsStep{
		baseStep:  baseStep{core: c.s},
		pChannels: pChannels,
	})
	redo.AddAsyncStep(newConfirmGCStep(c.s, partition.CollectionID, partition.PartitionID))
	redo.AddAsyncStep(&removePartitionMetaStep{
		baseStep:     baseStep{core: c.s},
		dbID:         dbID,
		collectionID: partition.CollectionID,
		partitionID:  partition.PartitionID,
		// This ts is less than the ts when we notify data nodes to drop partition, but it's OK since we have already
		// marked this partition as deleted. If we want to make this ts greater than the notification's ts, we should
		// wrap a step who will have these children and connect them with ts.
		ts: ts,
	})

	// err is ignored since no sync steps will be executed.
	_ = redo.Execute(context.Background())
}

func (c *bgGarbageCollector) RemoveCreatingPartition(dbID int64, partition *model.Partition, ts Timestamp) {
	redoTask := newBaseRedoTask(c.s.stepExecutor)

	redoTask.AddAsyncStep(&releasePartitionsStep{
		baseStep:     baseStep{core: c.s},
		collectionID: partition.CollectionID,
		partitionIDs: []int64{partition.PartitionID},
	})

	redoTask.AddAsyncStep(&removePartitionMetaStep{
		baseStep:     baseStep{core: c.s},
		dbID:         dbID,
		collectionID: partition.CollectionID,
		partitionID:  partition.PartitionID,
		ts:           ts,
	})

	// err is ignored since no sync steps will be executed.
	_ = redoTask.Execute(context.Background())
}

func (c *bgGarbageCollector) notifyCollectionGc(ctx context.Context, coll *model.Collection) (ddlTs Timestamp, err error) {
	if streamingutil.IsStreamingServiceEnabled() {
		return c.notifyCollectionGcByStreamingService(ctx, coll)
	}

	ts, err := c.s.tsoAllocator.GenerateTSO(1)
	if err != nil {
		return 0, err
	}

	msgPack := ms.MsgPack{}
	msg := &ms.DropCollectionMsg{
		BaseMsg: ms.BaseMsg{
			Ctx:            ctx,
			BeginTimestamp: ts,
			EndTimestamp:   ts,
			HashValues:     []uint32{0},
		},
		DropCollectionRequest: c.generateDropRequest(coll, ts),
	}
	msgPack.Msgs = append(msgPack.Msgs, msg)
	if err := c.s.chanTimeTick.broadcastDmlChannels(coll.PhysicalChannelNames, &msgPack); err != nil {
		return 0, err
	}

	return ts, nil
}

func (c *bgGarbageCollector) generateDropRequest(coll *model.Collection, ts uint64) *msgpb.DropCollectionRequest {
	return &msgpb.DropCollectionRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_DropCollection),
			commonpbutil.WithTimeStamp(ts),
			commonpbutil.WithSourceID(c.s.session.ServerID),
		),
		CollectionName: coll.Name,
		CollectionID:   coll.CollectionID,
	}
}

func (c *bgGarbageCollector) notifyCollectionGcByStreamingService(ctx context.Context, coll *model.Collection) (uint64, error) {
	req := c.generateDropRequest(coll, 0) // ts is given by streamingnode.

	msgs := make([]message.MutableMessage, 0, len(coll.VirtualChannelNames))
	for _, vchannel := range coll.VirtualChannelNames {
		msg, err := message.NewDropCollectionMessageBuilderV1().
			WithVChannel(vchannel).
			WithHeader(&message.DropCollectionMessageHeader{
				CollectionId: coll.CollectionID,
			}).
			WithBody(req).
			BuildMutable()
		if err != nil {
			return 0, err
		}
		msgs = append(msgs, msg)
	}
	resp := streaming.WAL().AppendMessages(ctx, msgs...)
	if err := resp.UnwrapFirstError(); err != nil {
		return 0, err
	}
	return resp.MaxTimeTick(), nil
}

func (c *bgGarbageCollector) notifyPartitionGc(ctx context.Context, pChannels []string, partition *model.Partition) (ddlTs Timestamp, err error) {
	ts, err := c.s.tsoAllocator.GenerateTSO(1)
	if err != nil {
		return 0, err
	}

	msgPack := ms.MsgPack{}
	msg := &ms.DropPartitionMsg{
		BaseMsg: ms.BaseMsg{
			Ctx:            ctx,
			BeginTimestamp: ts,
			EndTimestamp:   ts,
			HashValues:     []uint32{0},
		},
		DropPartitionRequest: &msgpb.DropPartitionRequest{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithMsgType(commonpb.MsgType_DropPartition),
				commonpbutil.WithTimeStamp(ts),
				commonpbutil.WithSourceID(c.s.session.ServerID),
			),
			PartitionName: partition.PartitionName,
			CollectionID:  partition.CollectionID,
			PartitionID:   partition.PartitionID,
		},
	}
	msgPack.Msgs = append(msgPack.Msgs, msg)
	if err := c.s.chanTimeTick.broadcastDmlChannels(pChannels, &msgPack); err != nil {
		return 0, err
	}

	return ts, nil
}

func (c *bgGarbageCollector) notifyPartitionGcByStreamingService(ctx context.Context, vchannels []string, partition *model.Partition) (uint64, error) {
	req := &msgpb.DropPartitionRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_DropPartition),
			commonpbutil.WithTimeStamp(0), // Timetick is given by streamingnode.
			commonpbutil.WithSourceID(c.s.session.ServerID),
		),
		PartitionName: partition.PartitionName,
		CollectionID:  partition.CollectionID,
		PartitionID:   partition.PartitionID,
	}

	msgs := make([]message.MutableMessage, 0, len(vchannels))
	for _, vchannel := range vchannels {
		msg, err := message.NewDropPartitionMessageBuilderV1().
			WithVChannel(vchannel).
			WithHeader(&message.DropPartitionMessageHeader{
				CollectionId: partition.CollectionID,
				PartitionId:  partition.PartitionID,
			}).
			WithBody(req).
			BuildMutable()
		if err != nil {
			return 0, err
		}
		msgs = append(msgs, msg)
	}
	// Ts is used as barrier time tick to ensure the message's time tick are given after the barrier time tick.
	resp := streaming.WAL().AppendMessages(ctx, msgs...)
	if err := resp.UnwrapFirstError(); err != nil {
		return 0, err
	}
	return resp.MaxTimeTick(), nil
}

func (c *bgGarbageCollector) GcCollectionData(ctx context.Context, coll *model.Collection) (ddlTs Timestamp, err error) {
	c.s.ddlTsLockManager.Lock()
	c.s.ddlTsLockManager.AddRefCnt(1)
	defer c.s.ddlTsLockManager.AddRefCnt(-1)
	defer c.s.ddlTsLockManager.Unlock()

	ddlTs, err = c.notifyCollectionGc(ctx, coll)
	if err != nil {
		return 0, err
	}
	c.s.ddlTsLockManager.UpdateLastTs(ddlTs)
	return ddlTs, nil
}

func (c *bgGarbageCollector) GcPartitionData(ctx context.Context, pChannels, vchannels []string, partition *model.Partition) (ddlTs Timestamp, err error) {
	c.s.ddlTsLockManager.Lock()
	c.s.ddlTsLockManager.AddRefCnt(1)
	defer c.s.ddlTsLockManager.AddRefCnt(-1)
	defer c.s.ddlTsLockManager.Unlock()

	if streamingutil.IsStreamingServiceEnabled() {
		ddlTs, err = c.notifyPartitionGcByStreamingService(ctx, vchannels, partition)
	} else {
		ddlTs, err = c.notifyPartitionGc(ctx, pChannels, partition)
	}
	if err != nil {
		return 0, err
	}
	c.s.ddlTsLockManager.UpdateLastTs(ddlTs)
	return ddlTs, nil
}

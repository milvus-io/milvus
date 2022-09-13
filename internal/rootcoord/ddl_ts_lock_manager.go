package rootcoord

import (
	"context"
	"sync"

	ms "github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"

	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/internal/metastore/model"
)

type DdlTsLockManager interface {
	GetMinDdlTs() Timestamp
	NotifyCollectionGc(ctx context.Context, coll *model.Collection) error
	NotifyPartitionGc(ctx context.Context, pChannels []string, partition *model.Partition) error
}

type ddlTsLockManager struct {
	lastTs        atomic.Uint64
	inProgressCnt atomic.Uint32
	s             *Core
	sync.Mutex
}

func (c *ddlTsLockManager) updateLastTs(ts Timestamp) {
	c.lastTs.Store(ts)
}

func (c *ddlTsLockManager) GetMinDdlTs() Timestamp {
	// In fact, `TryLock` can replace the `inProgressCnt` but it's not recommended.
	if c.inProgressCnt.Load() > 0 {
		return c.lastTs.Load()
	}
	c.Lock()
	defer c.Unlock()
	ts, err := c.s.tsoAllocator.GenerateTSO(1)
	if err != nil {
		return c.lastTs.Load()
	}
	c.updateLastTs(ts)
	return ts
}

func (c *ddlTsLockManager) NotifyCollectionGc(ctx context.Context, coll *model.Collection) error {
	c.Lock()
	defer c.Unlock()

	c.inProgressCnt.Inc()
	defer c.inProgressCnt.Dec()

	ts, err := c.s.tsoAllocator.GenerateTSO(1)
	if err != nil {
		return err
	}

	msgPack := ms.MsgPack{}
	baseMsg := ms.BaseMsg{
		Ctx:            ctx,
		BeginTimestamp: ts,
		EndTimestamp:   ts,
		HashValues:     []uint32{0},
	}
	msg := &ms.DropCollectionMsg{
		BaseMsg: baseMsg,
		DropCollectionRequest: internalpb.DropCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DropCollection,
				Timestamp: ts,
				SourceID:  c.s.session.ServerID,
			},
			CollectionName: coll.Name,
			CollectionID:   coll.CollectionID,
		},
	}
	msgPack.Msgs = append(msgPack.Msgs, msg)
	if err := c.s.chanTimeTick.broadcastDmlChannels(coll.PhysicalChannelNames, &msgPack); err != nil {
		return err
	}

	// TODO: remove this after gc can be notified by rpc. Without this tt, DropCollectionMsg cannot be seen by
	// 		datanodes.
	c.updateLastTs(ts)
	return c.s.chanTimeTick.sendTimeTickToChannel(coll.PhysicalChannelNames, ts)
}

func (c *ddlTsLockManager) NotifyPartitionGc(ctx context.Context, pChannels []string, partition *model.Partition) error {
	c.Lock()
	defer c.Unlock()

	c.inProgressCnt.Inc()
	defer c.inProgressCnt.Dec()

	ts, err := c.s.tsoAllocator.GenerateTSO(1)
	if err != nil {
		return err
	}

	msgPack := ms.MsgPack{}
	baseMsg := ms.BaseMsg{
		Ctx:            ctx,
		BeginTimestamp: ts,
		EndTimestamp:   ts,
		HashValues:     []uint32{0},
	}
	msg := &ms.DropPartitionMsg{
		BaseMsg: baseMsg,
		DropPartitionRequest: internalpb.DropPartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DropPartition,
				Timestamp: ts,
				SourceID:  c.s.session.ServerID,
			},
			PartitionName: partition.PartitionName,
			CollectionID:  partition.CollectionID,
			PartitionID:   partition.PartitionID,
		},
	}
	msgPack.Msgs = append(msgPack.Msgs, msg)
	if err := c.s.chanTimeTick.broadcastDmlChannels(pChannels, &msgPack); err != nil {
		return err
	}

	// TODO: remove this after gc can be notified by rpc. Without this tt, DropCollectionMsg cannot be seen by
	// 		datanodes.
	c.updateLastTs(ts)
	return c.s.chanTimeTick.sendTimeTickToChannel(pChannels, ts)
}

func newDdlTsLockManager(s *Core) *ddlTsLockManager {
	return &ddlTsLockManager{
		lastTs:        *atomic.NewUint64(0),
		inProgressCnt: *atomic.NewUint32(0),
		s:             s,
	}
}

package rootcoord

import (
	"context"

	pb "github.com/milvus-io/milvus/internal/proto/etcdpb"

	"github.com/milvus-io/milvus/internal/metastore/model"
)

type Step interface {
	Execute(ctx context.Context) error
}

type baseStep struct {
	core *Core
}

type AddCollectionMetaStep struct {
	baseStep
	coll *model.Collection
}

func (s *AddCollectionMetaStep) Execute(ctx context.Context) error {
	return s.core.meta.AddCollection(ctx, s.coll)
}

type DeleteCollectionMetaStep struct {
	baseStep
	collectionID UniqueID
	ts           Timestamp
}

func (s *DeleteCollectionMetaStep) Execute(ctx context.Context) error {
	return s.core.meta.RemoveCollection(ctx, s.collectionID, s.ts)
}

type RemoveDmlChannelsStep struct {
	baseStep
	pchannels []string
}

func (s *RemoveDmlChannelsStep) Execute(ctx context.Context) error {
	s.core.chanTimeTick.removeDmlChannels(s.pchannels...)
	return nil
}

type WatchChannelsStep struct {
	baseStep
	info *watchInfo
}

func (s *WatchChannelsStep) Execute(ctx context.Context) error {
	return s.core.broker.WatchChannels(ctx, s.info)
}

type UnwatchChannelsStep struct {
	baseStep
	collectionID UniqueID
	channels     collectionChannels
}

func (s *UnwatchChannelsStep) Execute(ctx context.Context) error {
	return s.core.broker.UnwatchChannels(ctx, &watchInfo{collectionID: s.collectionID, vChannels: s.channels.virtualChannels})
}

type ChangeCollectionStateStep struct {
	baseStep
	collectionID UniqueID
	state        pb.CollectionState
	ts           Timestamp
}

func (s *ChangeCollectionStateStep) Execute(ctx context.Context) error {
	return s.core.meta.ChangeCollectionState(ctx, s.collectionID, s.state, s.ts)
}

type ExpireCacheStep struct {
	baseStep
	collectionNames []string
	collectionID    UniqueID
	ts              Timestamp
}

func (s *ExpireCacheStep) Execute(ctx context.Context) error {
	return s.core.ExpireMetaCache(ctx, s.collectionNames, s.collectionID, s.ts)
}

type DeleteCollectionDataStep struct {
	baseStep
	coll *model.Collection
	ts   Timestamp
}

func (s *DeleteCollectionDataStep) Execute(ctx context.Context) error {
	return s.core.garbageCollector.GcCollectionData(ctx, s.coll, s.ts)
}

type DeletePartitionDataStep struct {
	baseStep
	pchans    []string
	partition *model.Partition
	ts        Timestamp
}

func (s *DeletePartitionDataStep) Execute(ctx context.Context) error {
	return s.core.garbageCollector.GcPartitionData(ctx, s.pchans, s.partition, s.ts)
}

type ReleaseCollectionStep struct {
	baseStep
	collectionID UniqueID
}

func (s *ReleaseCollectionStep) Execute(ctx context.Context) error {
	return s.core.broker.ReleaseCollection(ctx, s.collectionID)
}

type DropIndexStep struct {
	baseStep
	collID UniqueID
}

func (s *DropIndexStep) Execute(ctx context.Context) error {
	return s.core.broker.DropCollectionIndex(ctx, s.collID)
}

type AddPartitionMetaStep struct {
	baseStep
	partition *model.Partition
}

func (s *AddPartitionMetaStep) Execute(ctx context.Context) error {
	return s.core.meta.AddPartition(ctx, s.partition)
}

type ChangePartitionStateStep struct {
	baseStep
	collectionID UniqueID
	partitionID  UniqueID
	state        pb.PartitionState
	ts           Timestamp
}

func (s *ChangePartitionStateStep) Execute(ctx context.Context) error {
	return s.core.meta.ChangePartitionState(ctx, s.collectionID, s.partitionID, s.state, s.ts)
}

type RemovePartitionMetaStep struct {
	baseStep
	collectionID UniqueID
	partitionID  UniqueID
	ts           Timestamp
}

func (s *RemovePartitionMetaStep) Execute(ctx context.Context) error {
	return s.core.meta.RemovePartition(ctx, s.collectionID, s.partitionID, s.ts)
}

type NullStep struct {
}

func (s *NullStep) Execute(ctx context.Context) error {
	return nil
}

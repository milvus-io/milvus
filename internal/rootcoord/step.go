package rootcoord

import (
	"context"
	"fmt"

	pb "github.com/milvus-io/milvus/internal/proto/etcdpb"

	"github.com/milvus-io/milvus/internal/metastore/model"
)

type stepPriority int

const (
	stepPriorityLow = iota
	stepPriorityNormal
	stepPriorityImportant
	stepPriorityUrgent
)

type nestedStep interface {
	Execute(ctx context.Context) ([]nestedStep, error)
	Desc() string
	Weight() stepPriority
}

type baseStep struct {
	core *Core
}

func (s baseStep) Desc() string {
	return ""
}

func (s baseStep) Weight() stepPriority {
	return stepPriorityLow
}

type addCollectionMetaStep struct {
	baseStep
	coll *model.Collection
}

func (s *addCollectionMetaStep) Execute(ctx context.Context) ([]nestedStep, error) {
	err := s.core.meta.AddCollection(ctx, s.coll)
	return nil, err
}

func (s *addCollectionMetaStep) Desc() string {
	return fmt.Sprintf("add collection to meta table, name: %s, id: %d, ts: %d", s.coll.Name, s.coll.CollectionID, s.coll.CreateTime)
}

type deleteCollectionMetaStep struct {
	baseStep
	collectionID UniqueID
	ts           Timestamp
}

func (s *deleteCollectionMetaStep) Execute(ctx context.Context) ([]nestedStep, error) {
	err := s.core.meta.RemoveCollection(ctx, s.collectionID, s.ts)
	return nil, err
}

func (s *deleteCollectionMetaStep) Desc() string {
	return fmt.Sprintf("delete collection from meta table, id: %d, ts: %d", s.collectionID, s.ts)
}

func (s *deleteCollectionMetaStep) Weight() stepPriority {
	return stepPriorityNormal
}

type removeDmlChannelsStep struct {
	baseStep
	pChannels []string
}

func (s *removeDmlChannelsStep) Execute(ctx context.Context) ([]nestedStep, error) {
	s.core.chanTimeTick.removeDmlChannels(s.pChannels...)
	return nil, nil
}

func (s *removeDmlChannelsStep) Desc() string {
	// this shouldn't be called.
	return fmt.Sprintf("remove dml channels: %v", s.pChannels)
}

func (s *removeDmlChannelsStep) Weight() stepPriority {
	// avoid too frequent tt.
	return stepPriorityUrgent
}

type watchChannelsStep struct {
	baseStep
	info *watchInfo
}

func (s *watchChannelsStep) Execute(ctx context.Context) ([]nestedStep, error) {
	err := s.core.broker.WatchChannels(ctx, s.info)
	return nil, err
}

func (s *watchChannelsStep) Desc() string {
	return fmt.Sprintf("watch channels, ts: %d, collection: %d, partition: %d, vChannels: %v",
		s.info.ts, s.info.collectionID, s.info.partitionID, s.info.vChannels)
}

type unwatchChannelsStep struct {
	baseStep
	collectionID UniqueID
	channels     collectionChannels
}

func (s *unwatchChannelsStep) Execute(ctx context.Context) ([]nestedStep, error) {
	err := s.core.broker.UnwatchChannels(ctx, &watchInfo{collectionID: s.collectionID, vChannels: s.channels.virtualChannels})
	return nil, err
}

func (s *unwatchChannelsStep) Desc() string {
	return fmt.Sprintf("unwatch channels, collection: %d, pChannels: %v, vChannels: %v",
		s.collectionID, s.channels.physicalChannels, s.channels.virtualChannels)
}

func (s *unwatchChannelsStep) Weight() stepPriority {
	return stepPriorityNormal
}

type changeCollectionStateStep struct {
	baseStep
	collectionID UniqueID
	state        pb.CollectionState
	ts           Timestamp
}

func (s *changeCollectionStateStep) Execute(ctx context.Context) ([]nestedStep, error) {
	err := s.core.meta.ChangeCollectionState(ctx, s.collectionID, s.state, s.ts)
	return nil, err
}

func (s *changeCollectionStateStep) Desc() string {
	return fmt.Sprintf("change collection state, collection: %d, ts: %d, state: %s",
		s.collectionID, s.ts, s.state.String())
}

type expireCacheStep struct {
	baseStep
	collectionNames []string
	collectionID    UniqueID
	ts              Timestamp
}

func (s *expireCacheStep) Execute(ctx context.Context) ([]nestedStep, error) {
	err := s.core.ExpireMetaCache(ctx, s.collectionNames, s.collectionID, s.ts)
	return nil, err
}

func (s *expireCacheStep) Desc() string {
	return fmt.Sprintf("expire cache, collection id: %d, collection names: %s, ts: %d",
		s.collectionID, s.collectionNames, s.ts)
}

type deleteCollectionDataStep struct {
	baseStep
	coll *model.Collection
}

func (s *deleteCollectionDataStep) Execute(ctx context.Context) ([]nestedStep, error) {
	ddlTs, err := s.core.garbageCollector.GcCollectionData(ctx, s.coll)
	if err != nil {
		return nil, err
	}
	// wait for ts synced.
	children := make([]nestedStep, 0, len(s.coll.PhysicalChannelNames))
	for _, channel := range s.coll.PhysicalChannelNames {
		children = append(children, &waitForTsSyncedStep{
			baseStep: baseStep{core: s.core},
			ts:       ddlTs,
			channel:  channel,
		})
	}
	return children, nil
}

func (s *deleteCollectionDataStep) Desc() string {
	return fmt.Sprintf("delete collection data, collection: %d", s.coll.CollectionID)
}

func (s *deleteCollectionDataStep) Weight() stepPriority {
	return stepPriorityImportant
}

// waitForTsSyncedStep child step of deleteCollectionDataStep.
type waitForTsSyncedStep struct {
	baseStep
	ts      Timestamp
	channel string
}

func (s *waitForTsSyncedStep) Execute(ctx context.Context) ([]nestedStep, error) {
	syncedTs := s.core.chanTimeTick.getSyncedTimeTick(s.channel)
	if syncedTs < s.ts {
		// TODO: there may be frequent log here.
		// time.Sleep(Params.ProxyCfg.TimeTickInterval)
		return nil, fmt.Errorf("ts not synced yet, channel: %s, synced: %d, want: %d", s.channel, syncedTs, s.ts)
	}
	return nil, nil
}

func (s *waitForTsSyncedStep) Desc() string {
	return fmt.Sprintf("wait for ts synced, channel: %s, want: %d", s.channel, s.ts)
}

func (s *waitForTsSyncedStep) Weight() stepPriority {
	return stepPriorityNormal
}

type deletePartitionDataStep struct {
	baseStep
	pchans    []string
	partition *model.Partition
}

func (s *deletePartitionDataStep) Execute(ctx context.Context) ([]nestedStep, error) {
	_, err := s.core.garbageCollector.GcPartitionData(ctx, s.pchans, s.partition)
	return nil, err
}

func (s *deletePartitionDataStep) Desc() string {
	return fmt.Sprintf("delete partition data, collection: %d, partition: %d", s.partition.CollectionID, s.partition.PartitionID)
}

func (s *deletePartitionDataStep) Weight() stepPriority {
	return stepPriorityImportant
}

type releaseCollectionStep struct {
	baseStep
	collectionID UniqueID
}

func (s *releaseCollectionStep) Execute(ctx context.Context) ([]nestedStep, error) {
	err := s.core.broker.ReleaseCollection(ctx, s.collectionID)
	return nil, err
}

func (s *releaseCollectionStep) Desc() string {
	return fmt.Sprintf("release collection: %d", s.collectionID)
}

func (s *releaseCollectionStep) Weight() stepPriority {
	return stepPriorityUrgent
}

type dropIndexStep struct {
	baseStep
	collID  UniqueID
	partIDs []UniqueID
}

func (s *dropIndexStep) Execute(ctx context.Context) ([]nestedStep, error) {
	err := s.core.broker.DropCollectionIndex(ctx, s.collID, s.partIDs)
	return nil, err
}

func (s *dropIndexStep) Desc() string {
	return fmt.Sprintf("drop collection index: %d", s.collID)
}

func (s *dropIndexStep) Weight() stepPriority {
	return stepPriorityNormal
}

type addPartitionMetaStep struct {
	baseStep
	partition *model.Partition
}

func (s *addPartitionMetaStep) Execute(ctx context.Context) ([]nestedStep, error) {
	err := s.core.meta.AddPartition(ctx, s.partition)
	return nil, err
}

func (s *addPartitionMetaStep) Desc() string {
	return fmt.Sprintf("add partition to meta table, collection: %d, partition: %d", s.partition.CollectionID, s.partition.PartitionID)
}

type changePartitionStateStep struct {
	baseStep
	collectionID UniqueID
	partitionID  UniqueID
	state        pb.PartitionState
	ts           Timestamp
}

func (s *changePartitionStateStep) Execute(ctx context.Context) ([]nestedStep, error) {
	err := s.core.meta.ChangePartitionState(ctx, s.collectionID, s.partitionID, s.state, s.ts)
	return nil, err
}

func (s *changePartitionStateStep) Desc() string {
	return fmt.Sprintf("change partition step, collection: %d, partition: %d, state: %s, ts: %d",
		s.collectionID, s.partitionID, s.state.String(), s.ts)
}

type removePartitionMetaStep struct {
	baseStep
	collectionID UniqueID
	partitionID  UniqueID
	ts           Timestamp
}

func (s *removePartitionMetaStep) Execute(ctx context.Context) ([]nestedStep, error) {
	err := s.core.meta.RemovePartition(ctx, s.collectionID, s.partitionID, s.ts)
	return nil, err
}

func (s *removePartitionMetaStep) Desc() string {
	return fmt.Sprintf("remove partition meta, collection: %d, partition: %d, ts: %d", s.collectionID, s.partitionID, s.ts)
}

func (s *removePartitionMetaStep) Weight() stepPriority {
	return stepPriorityNormal
}

type nullStep struct {
}

func (s *nullStep) Execute(ctx context.Context) ([]nestedStep, error) {
	return nil, nil
}

func (s *nullStep) Desc() string {
	return ""
}

func (s *nullStep) Weight() stepPriority {
	return stepPriorityLow
}

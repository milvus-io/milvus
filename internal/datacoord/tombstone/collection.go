package tombstone

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

const defaultTombstoneTTL = 7 * 24 * time.Hour

type (
	DroppingCollection = datapb.CollectionTombstoneImpl
	DroppingPartition  = datapb.PartitionTombstoneImpl
)

// collectionTombstoneImpl is a tombstone that is used to mark a resource as dropping.
// It is used to perform drop operation atomic and idempotent between coords.
// TODO: It can be removed after we have a global unique coordination mechanism (merge all coord into one).
type collectionTombstoneImpl struct {
	mu              sync.Mutex
	collection      *typeutil.ConcurrentMap[int64, *datapb.CollectionTombstoneImpl]
	partition       *typeutil.ConcurrentMap[int64, *datapb.PartitionTombstoneImpl]
	virtualChannels *typeutil.ConcurrentMap[string, int64]
	catalog         metastore.DataCoordCatalog
	notifier        *syncutil.AsyncTaskNotifier[struct{}]
}

// recoverCollectionTombstone recovers the collection tombstone from the metastore.
func recoverCollectionTombstone(ctx context.Context, catalog metastore.DataCoordCatalog) (*collectionTombstoneImpl, error) {
	tombstones, err := catalog.ListCollectionTombstone(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to list collection tombstones")
	}
	collectionTombstones := typeutil.NewConcurrentMap[int64, *datapb.CollectionTombstoneImpl]()
	partitionTombstones := typeutil.NewConcurrentMap[int64, *datapb.PartitionTombstoneImpl]()
	virtualChannels := typeutil.NewConcurrentMap[string, int64]()

	for _, tombstone := range tombstones {
		switch tombstone := tombstone.Tombstone.Tombstone.(type) {
		case *datapb.CollectionTombstone_Collection:
			collectionTombstones.Insert(tombstone.Collection.GetCollectionId(), tombstone.Collection)
			for _, vchannel := range tombstone.Collection.GetVchannels() {
				virtualChannels.Insert(vchannel, tombstone.Collection.GetCollectionId())
			}
		case *datapb.CollectionTombstone_Partition:
			partitionTombstones.Insert(tombstone.Partition.GetPartitionId(), tombstone.Partition)
		default:
			panic("unexpected tombstone type")
		}
	}

	ct := &collectionTombstoneImpl{
		mu:              sync.Mutex{},
		collection:      collectionTombstones,
		partition:       partitionTombstones,
		virtualChannels: virtualChannels,
		catalog:         catalog,
		notifier:        syncutil.NewAsyncTaskNotifier[struct{}](),
	}
	go ct.background()
	return ct, nil
}

// MarkCollectionAsDropped marks a collection as dropped.
func (dt *collectionTombstoneImpl) MarkCollectionAsDropping(ctx context.Context, collection *DroppingCollection) error {
	if collection.GetCollectionId() == 0 {
		return merr.WrapErrParameterMissing("collectionID")
	}
	if len(collection.GetVchannels()) == 0 {
		return merr.WrapErrParameterMissing("vchannelNames")
	}

	dt.mu.Lock()
	defer dt.mu.Unlock()
	if _, ok := dt.collection.Get(collection.GetCollectionId()); ok {
		return nil
	}

	// The tombstone's creation is always idempotent.
	collection.CreateTimestamp = time.Now().Unix()
	tombstone := model.NewCollectionTombstone(collection)
	if err := dt.catalog.SaveCollectionTombstone(ctx, tombstone); err != nil {
		return err
	}
	dt.collection.Insert(collection.GetCollectionId(), collection)
	for _, vchannel := range collection.GetVchannels() {
		dt.virtualChannels.Insert(vchannel, collection.GetCollectionId())
	}
	return nil
}

// MarkPartitionAsDropped marks a partition as dropped.
func (dt *collectionTombstoneImpl) MarkPartitionAsDropping(ctx context.Context, partition *DroppingPartition) error {
	if partition.GetCollectionId() == 0 {
		return merr.WrapErrParameterMissing("collectionID")
	}
	if partition.GetPartitionId() == 0 {
		return merr.WrapErrParameterMissing("partitionID")
	}

	dt.mu.Lock()
	defer dt.mu.Unlock()
	if _, ok := dt.collection.Get(partition.GetCollectionId()); ok {
		return nil
	}
	if _, ok := dt.partition.Get(partition.GetPartitionId()); ok {
		return nil
	}

	// The tombstone's creation is always idempotent.
	partition.CreateTimestamp = time.Now().Unix()
	tombstone := model.NewParititionTombstone(partition)
	if err := dt.catalog.SaveCollectionTombstone(ctx, tombstone); err != nil {
		return err
	}
	dt.partition.Insert(partition.GetPartitionId(), partition)
	return nil
}

// IsPartitionDropped checks if a partition is dropped.
func (dt *collectionTombstoneImpl) CheckIfPartitionDropped(collectionID int64, partitionID int64) error {
	if _, ok := dt.collection.Get(collectionID); ok {
		return merr.WrapErrCollectionDropped(collectionID)
	}
	if _, ok := dt.partition.Get(partitionID); ok {
		return merr.WrapErrPartitionDropped(partitionID)
	}
	return nil
}

func (dt *collectionTombstoneImpl) CheckIfVChannelDropped(vchannel string) error {
	dt.mu.Lock()
	defer dt.mu.Unlock()

	if collectionID, ok := dt.virtualChannels.Get(vchannel); ok {
		return merr.WrapErrCollectionDropped(collectionID)
	}
	return nil
}

// background periodically checks and removes the expired tombstones.
func (dt *collectionTombstoneImpl) background() {
	defer dt.notifier.Finish(struct{}{})

	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-dt.notifier.Context().Done():
			return
		case <-ticker.C:
			dt.expire(dt.notifier.Context(), defaultTombstoneTTL)
		}
	}
}

// expire expires the tombstone that is older than the ttl.
func (dt *collectionTombstoneImpl) expire(ctx context.Context, ttl time.Duration) {
	dt.partition.Range(func(key int64, value *datapb.PartitionTombstoneImpl) bool {
		if time.Since(time.Unix(value.CreateTimestamp, 0)) > ttl {
			if err := dt.removeParitionTombstone(ctx, key); err != nil {
				log.Warn("failed to remove partition tombstone", zap.Int64("partitionID", key), zap.Error(err))
			} else {
				log.Info("remove partition tombstone", zap.Int64("partitionID", key))
			}
		}
		return true
	})

	dt.collection.Range(func(collectionID int64, value *datapb.CollectionTombstoneImpl) bool {
		if time.Since(time.Unix(value.CreateTimestamp, 0)) > ttl {
			if err := dt.removeCollectionTombstone(ctx, collectionID); err != nil {
				log.Warn("failed to remove collection tombstone", zap.Int64("collectionID", collectionID), zap.Error(err))
			} else {
				log.Info("remove collection tombstone", zap.Int64("collectionID", collectionID))
			}
		}
		return true
	})
}

// removeCollectionTombstone removes the collection tombstone from the metastore.
func (dt *collectionTombstoneImpl) removeCollectionTombstone(ctx context.Context, collectionID int64) error {
	dt.mu.Lock()
	defer dt.mu.Unlock()

	tombstone, ok := dt.collection.Get(collectionID)
	if !ok {
		return nil
	}

	if err := dt.catalog.DropCollectionTombstone(ctx, model.NewCollectionTombstone(tombstone)); err != nil {
		return err
	}
	for _, vchannel := range tombstone.GetVchannels() {
		dt.virtualChannels.Remove(vchannel)
	}
	dt.collection.Remove(collectionID)
	return nil
}

// removeParitionTombstone removes the partition tombstone from the metastore.
func (dt *collectionTombstoneImpl) removeParitionTombstone(ctx context.Context, partitionID int64) error {
	dt.mu.Lock()
	defer dt.mu.Unlock()

	tombstone, ok := dt.partition.Get(partitionID)
	if !ok {
		return nil
	}

	if err := dt.catalog.DropCollectionTombstone(ctx, model.NewParititionTombstone(tombstone)); err != nil {
		return err
	}
	dt.partition.Remove(partitionID)
	return nil
}

// Close closes the collection tombstone.
func (dt *collectionTombstoneImpl) Close() {
	dt.notifier.Cancel()
	dt.notifier.BlockUntilFinish()
}

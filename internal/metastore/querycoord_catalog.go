package metastore

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

type QueryCoordCatalog interface {
	SaveCollection(ctx context.Context, collection *querypb.CollectionLoadInfo, partitions ...*querypb.PartitionLoadInfo) error
	SavePartition(ctx context.Context, info ...*querypb.PartitionLoadInfo) error
	SaveReplica(ctx context.Context, replicas ...*querypb.Replica) error
	GetCollections(ctx context.Context) ([]*querypb.CollectionLoadInfo, error)
	GetPartitions(ctx context.Context, collectionIDs []int64) (map[int64][]*querypb.PartitionLoadInfo, error)
	GetReplicas(ctx context.Context) ([]*querypb.Replica, error)
	ReleaseCollection(ctx context.Context, collection int64) error
	ReleasePartition(ctx context.Context, collection int64, partitions ...int64) error
	ReleaseReplicas(ctx context.Context, collectionID int64) error
	ReleaseReplica(ctx context.Context, collection int64, replicas ...int64) error
	SaveResourceGroup(ctx context.Context, rgs ...*querypb.ResourceGroup) error
	RemoveResourceGroup(ctx context.Context, rgName string) error
	GetResourceGroups(ctx context.Context) ([]*querypb.ResourceGroup, error)

	SaveCollectionTargets(ctx context.Context, target ...*querypb.CollectionTarget) error
	RemoveCollectionTarget(ctx context.Context, collectionID int64) error
	RemoveCollectionTargets(ctx context.Context) error
	GetCollectionTargets(ctx context.Context) (map[int64]*querypb.CollectionTarget, error)
}

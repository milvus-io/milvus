package dbmodel

import "context"

//go:generate mockery --name=IMetaDomain
type IMetaDomain interface {
	CollectionDb(ctx context.Context) ICollectionDb
	FieldDb(ctx context.Context) IFieldDb
	CollChannelDb(ctx context.Context) ICollChannelDb
	CollAliasDb(ctx context.Context) ICollAliasDb
	PartitionDb(ctx context.Context) IPartitionDb
	IndexDb(ctx context.Context) IIndexDb
	SegmentIndexDb(ctx context.Context) ISegmentIndexDb
	UserDb(ctx context.Context) IUserDb
}

type ITransaction interface {
	Transaction(ctx context.Context, fn func(txCtx context.Context) error) error
}

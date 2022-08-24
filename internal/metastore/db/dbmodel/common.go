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
	RoleDb(ctx context.Context) IRoleDb
	UserRoleDb(ctx context.Context) IUserRoleDb
	GrantDb(ctx context.Context) IGrantDb
	GrantIDDb(ctx context.Context) IGrantIDDb
}

type ITransaction interface {
	Transaction(ctx context.Context, fn func(txCtx context.Context) error) error
}

func GetCommonCondition(tenant string, isDelete bool) map[string]interface{} {
	return map[string]interface{}{
		"tenant_id":  tenant,
		"is_deleted": isDelete,
	}
}

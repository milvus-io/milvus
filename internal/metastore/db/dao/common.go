package dao

import (
	"context"

	"github.com/milvus-io/milvus/internal/metastore/db/dbcore"
	"github.com/milvus-io/milvus/internal/metastore/db/dbmodel"
)

type metaDomain struct{}

func NewMetaDomain() *metaDomain {
	return &metaDomain{}
}

func (*metaDomain) CollectionDb(ctx context.Context) dbmodel.ICollectionDb {
	return &collectionDb{dbcore.GetDB(ctx)}
}

func (*metaDomain) FieldDb(ctx context.Context) dbmodel.IFieldDb {
	return &fieldDb{dbcore.GetDB(ctx)}
}

func (*metaDomain) CollChannelDb(ctx context.Context) dbmodel.ICollChannelDb {
	return &collChannelDb{dbcore.GetDB(ctx)}
}

func (*metaDomain) CollAliasDb(ctx context.Context) dbmodel.ICollAliasDb {
	return &collAliasDb{dbcore.GetDB(ctx)}
}

func (*metaDomain) PartitionDb(ctx context.Context) dbmodel.IPartitionDb {
	return &partitionDb{dbcore.GetDB(ctx)}
}

func (*metaDomain) IndexDb(ctx context.Context) dbmodel.IIndexDb {
	return &indexDb{dbcore.GetDB(ctx)}
}

func (*metaDomain) SegmentIndexDb(ctx context.Context) dbmodel.ISegmentIndexDb {
	return &segmentIndexDb{dbcore.GetDB(ctx)}
}

func (*metaDomain) UserDb(ctx context.Context) dbmodel.IUserDb {
	return &userDb{dbcore.GetDB(ctx)}
}

func (d *metaDomain) RoleDb(ctx context.Context) dbmodel.IRoleDb {
	return &roleDb{dbcore.GetDB(ctx)}
}

func (d *metaDomain) UserRoleDb(ctx context.Context) dbmodel.IUserRoleDb {
	return &userRoleDb{dbcore.GetDB(ctx)}
}

func (d *metaDomain) GrantDb(ctx context.Context) dbmodel.IGrantDb {
	return &grantDb{dbcore.GetDB(ctx)}
}

func (d *metaDomain) GrantIDDb(ctx context.Context) dbmodel.IGrantIDDb {
	return &grantIDDb{dbcore.GetDB(ctx)}
}

package dao

import (
	"errors"
	"fmt"

	"github.com/milvus-io/milvus/internal/common"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metastore/db/dbmodel"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type grantDb struct {
	db *gorm.DB
}

func (g *grantDb) GetGrants(tenantID string, roleID int64, object string, objectName string) ([]*dbmodel.Grant, error) {
	var (
		grants []*dbmodel.Grant
		err    error
	)
	err = g.db.Model(&dbmodel.Grant{}).Where(&dbmodel.Grant{RoleID: roleID, Object: object, ObjectName: objectName}).Where(dbmodel.GetCommonCondition(tenantID, false)).Preload("Role").Find(&grants).Error
	if err != nil {
		log.Error("fail to get grants", zap.String("tenant_id", tenantID), zap.Int64("roleID", roleID), zap.String("object", object), zap.String("object_name", objectName), zap.Error(err))
		return nil, err
	}
	return grants, nil
}

func (g *grantDb) Insert(in *dbmodel.Grant) error {
	var (
		sqlWhere    = &dbmodel.Grant{RoleID: in.RoleID, Object: in.Object, ObjectName: in.ObjectName}
		dbGrant     *dbmodel.Grant
		newDbDetail string
		err         error
	)
	err = g.db.Model(&dbmodel.Grant{}).Where(sqlWhere).Where(dbmodel.GetCommonCondition(in.TenantID, false)).Take(&dbGrant).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		err = g.db.Create(in).Error
		if err != nil {
			log.Error("fail to insert the grant", zap.Any("in", in), zap.Error(err))
		}
		return err
	}
	if err != nil {
		log.Error("fail to take the origin grant", zap.Any("in", in), zap.Error(err))
		return err
	}
	if newDbDetail, err = dbmodel.EncodeGrantDetailForString(dbGrant.Detail, in.Detail, true); err != nil {
		log.Error("fail to encode the grant detail", zap.Any("in", in), zap.Error(err))
		return err
	}
	err = g.db.Model(dbmodel.Grant{}).Where(sqlWhere).Where(dbmodel.GetCommonCondition(in.TenantID, false)).Update("detail", newDbDetail).Error
	if err != nil {
		log.Error("fail to update the grant", zap.Any("in", in), zap.Error(err))
	}
	return err
}

func (g *grantDb) Delete(tenantID string, roleID int64, object string, objectName string, privilege string) error {
	var (
		sqlWhere    = &dbmodel.Grant{RoleID: roleID, Object: object, ObjectName: objectName}
		dbGrant     *dbmodel.Grant
		newDbDetail string
		db          *gorm.DB
		err         error
	)

	err = g.db.Model(&dbmodel.Grant{}).Where(sqlWhere).Where(dbmodel.GetCommonCondition(tenantID, false)).Take(&dbGrant).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return common.NewIgnorableError(fmt.Errorf("the privilege[%s-%s-%s] isn't granted", object, objectName, privilege))
	}
	if err != nil {
		log.Error("fail to take the origin grant", zap.Any("where", sqlWhere), zap.Error(err))
		return err
	}
	if newDbDetail, err = dbmodel.EncodeGrantDetail(dbGrant.Detail, "", privilege, false); err != nil {
		log.Error("fail to encode the grant detail", zap.Any("detail", dbGrant.Detail), zap.String("privilege", privilege), zap.Error(err))
		return err
	}
	db = g.db.Model(dbmodel.Grant{}).Where(sqlWhere).Where(dbmodel.GetCommonCondition(tenantID, false))
	if newDbDetail == "" {
		err = db.Update("is_deleted", true).Error
	} else {
		err = db.Update("detail", newDbDetail).Error
	}
	if err != nil {
		log.Error("fail to delete the grant", zap.Bool("is_delete", newDbDetail == ""), zap.Any("where", sqlWhere), zap.String("privilege", privilege), zap.Error(err))
	}
	return err
}

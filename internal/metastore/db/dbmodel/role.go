package dbmodel

import "github.com/milvus-io/milvus/api/milvuspb"

type Role struct {
	Base
	Name string `gorm:"name"`
}

func (r *Role) TableName() string {
	return "role"
}

func (r *Role) Unmarshal() *milvuspb.RoleEntity {
	return &milvuspb.RoleEntity{Name: r.Name}
}

//go:generate mockery --name=IRoleDb
type IRoleDb interface {
	GetRoles(tenantID string, name string) ([]*Role, error)
	Insert(in *Role) error
	Delete(tenantID string, name string) error
}

package dbmodel

type Grant struct {
	Base
	RoleID     int64  `gorm:"role_id"`
	Role       Role   `gorm:"foreignKey:RoleID"`
	Object     string `gorm:"object"`
	ObjectName string `gorm:"object_name"`
}

func (g *Grant) TableName() string {
	return "grant"
}

//go:generate mockery --name=IGrantDb
type IGrantDb interface {
	GetGrants(tenantID string, roleID int64, object string, objectName string) ([]*Grant, error)
	Insert(in *Grant) error
	Delete(tenantID string, roleID int64, object string, objectName string) error
}

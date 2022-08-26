package dbmodel

type GrantID struct {
	Base
	GrantID   int64  `gorm:"grant_id"`
	Grant     Grant  `gorm:"foreignKey:GrantID"`
	Privilege string `gorm:"privilege"`
	GrantorID int64  `gorm:"grantor_id"`
	Grantor   User   `gorm:"foreignKey:GrantorID"`
}

func (g *GrantID) TableName() string {
	return "grant_id"
}

//go:generate mockery --name=IGrantIDDb
type IGrantIDDb interface {
	GetGrantIDs(tenantID string, grantID int64, privilege string, preloadGrant bool, preloadGrantor bool) ([]*GrantID, error)
	Insert(in *GrantID) error
	Delete(tenantID string, grantID int64, privilege string) error
}

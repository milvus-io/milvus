package dbmodel

type UserRole struct {
	Base
	UserID int64 `gorm:"user_id"`
	RoleID int64 `gorm:"role_id"`

	User User `gorm:"foreignKey:UserID"`
	Role Role `gorm:"foreignKey:RoleID"`
}

func (u *UserRole) TableName() string {
	return "user_role"
}

//go:generate mockery --name=IUserRoleDb
type IUserRoleDb interface {
	GetUserRoles(tenantID string, userID int64, roleID int64) ([]*UserRole, error)
	Insert(in *UserRole) error
	Delete(tenantID string, userID int64, roleID int64) error
}

package dbmodel

import (
	"encoding/json"
	"fmt"

	"github.com/milvus-io/milvus/internal/common"
)

type Grant struct {
	Base
	RoleID     int64  `gorm:"role_id"`
	Role       Role   `gorm:"foreignKey:RoleID"`
	Object     string `gorm:"object"`
	ObjectName string `gorm:"object_name"`
	Detail     string `gorm:"detail"`
}

func (g *Grant) TableName() string {
	return "grant"
}

//go:generate mockery --name=IGrantDb
type IGrantDb interface {
	GetGrants(tenantID string, roleID int64, object string, objectName string) ([]*Grant, error)
	Insert(in *Grant) error
	Delete(tenantID string, roleID int64, object string, objectName string, privilege string) error
}

func EncodeGrantDetail(detail string, grantor string, privilege string, isAdd bool) (string, error) {
	var (
		grant        = []string{grantor, privilege}
		resBytes     []byte
		originGrants [][]string
		index        = -1
		err          error

		handleGrant = func(grants [][]string) (string, error) {
			if resBytes, err = json.Marshal(grants); err != nil {
				return "", err
			}
			return string(resBytes), nil
		}
	)

	if detail == "" {
		if !isAdd {
			return "", common.NewIgnorableError(fmt.Errorf("the empty detail can't be remove"))
		}
		return handleGrant(append(originGrants, grant))
	}
	if originGrants, err = DecodeGrantDetail(detail); err != nil {
		return "", err
	}

	for i, origin := range originGrants {
		if origin[1] == privilege {
			index = i
			break
		}
	}
	if isAdd {
		if index != -1 {
			return detail, common.NewIgnorableError(fmt.Errorf("the grant[%s-%s] is existed", grantor, privilege))
		}
		return handleGrant(append(originGrants, grant))
	}
	if index == -1 {
		return detail, common.NewIgnorableError(fmt.Errorf("the grant[%s-%s] isn't existed", grantor, privilege))
	}
	if len(originGrants) == 1 {
		return "", nil
	}
	return handleGrant(append(originGrants[:index], originGrants[index+1:]...))
}

func EncodeGrantDetailForString(originDetail string, operateDetail string, isAdd bool) (string, error) {
	var (
		operateGrant [][]string
		err          error
	)

	if operateGrant, err = DecodeGrantDetail(operateDetail); err != nil {
		return "", err
	}
	if len(operateGrant) != 1 || len(operateGrant[0]) != 2 {
		return "", fmt.Errorf("invalid operateDetail: [%s], decode result: %+v", operateDetail, operateGrant)
	}
	return EncodeGrantDetail(originDetail, operateGrant[0][0], operateGrant[0][1], isAdd)
}

func DecodeGrantDetail(detail string) ([][]string, error) {
	var (
		grants [][]string
		err    error
	)
	if err = json.Unmarshal([]byte(detail), &grants); err != nil {
		return grants, err
	}
	return grants, nil
}

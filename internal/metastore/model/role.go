package model

import "encoding/json"

type Role struct {
	Name        string
	Description string
}

type roleInfo struct {
	Description string `json:"description"`
}

func MarshalRoleModel(role *Role) (string, error) {
	if role == nil {
		return "", nil
	}
	value, err := json.Marshal(&roleInfo{Description: role.Description})
	if err != nil {
		return "", err
	}
	return string(value), nil
}

func UnmarshalRoleModel(name string, value string) (*Role, error) {
	role := &Role{Name: name}
	if value == "" {
		return role, nil
	}
	info := &roleInfo{}
	if err := json.Unmarshal([]byte(value), info); err != nil {
		return nil, err
	}
	role.Description = info.Description
	return role, nil
}

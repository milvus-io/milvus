package model

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMarshalRoleModel(t *testing.T) {
	role := &Role{
		Name:        "role1",
		Description: "role description",
	}

	value, err := MarshalRoleModel(role)
	require.NoError(t, err)
	require.JSONEq(t, `{"description":"role description"}`, value)

	unmarshaled, err := UnmarshalRoleModel("role1", value)
	require.NoError(t, err)
	require.Equal(t, role, unmarshaled)
}

func TestUnmarshalRoleModelLegacyEmptyValue(t *testing.T) {
	role, err := UnmarshalRoleModel("legacy_role", "")
	require.NoError(t, err)
	require.Equal(t, &Role{Name: "legacy_role"}, role)
}

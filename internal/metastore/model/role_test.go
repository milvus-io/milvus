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

func TestMarshalRoleModelNil(t *testing.T) {
	value, err := MarshalRoleModel(nil)
	require.NoError(t, err)
	require.Empty(t, value)
}

func TestUnmarshalRoleModelLegacyEmptyValue(t *testing.T) {
	role, err := UnmarshalRoleModel("legacy_role", "")
	require.NoError(t, err)
	require.Equal(t, &Role{Name: "legacy_role"}, role)
}

func TestUnmarshalRoleModelInvalidJSON(t *testing.T) {
	role, err := UnmarshalRoleModel("invalid_role", "{")
	require.Error(t, err)
	require.Nil(t, role)
}

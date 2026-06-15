package rbacutil

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

func TestValidateRoleDescription(t *testing.T) {
	require.NoError(t, ValidateRoleDescription("1234", 4))

	err := ValidateRoleDescription("12345", 4)
	require.Error(t, err)
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
}

package roles

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRoles(t *testing.T) {
	r := MilvusRoles{}
	assert.False(t, r.HasAnyRole())

	assert.True(t, r.EnvValue("1"))
	assert.True(t, r.EnvValue(" 1 "))
	assert.True(t, r.EnvValue("True"))
	assert.True(t, r.EnvValue(" True "))
	assert.True(t, r.EnvValue(" TRue "))
	assert.False(t, r.EnvValue("0"))
	assert.False(t, r.EnvValue(" 0 "))
	assert.False(t, r.EnvValue(" false "))
	assert.False(t, r.EnvValue(" False "))
	assert.False(t, r.EnvValue(" abc "))

	ss := strings.SplitN("abcdef", "=", 2)
	assert.Equal(t, len(ss), 1)
	ss = strings.SplitN("adb=def", "=", 2)
	assert.Equal(t, len(ss), 2)

	{
		var roles MilvusRoles
		roles.EnableMaster = true
		assert.True(t, roles.HasAnyRole())
	}

	{
		var roles MilvusRoles
		roles.EnableProxyService = true
		assert.True(t, roles.HasAnyRole())
	}

	{
		var roles MilvusRoles
		roles.EnableProxyNode = true
		assert.True(t, roles.HasAnyRole())
	}

	{
		var roles MilvusRoles
		roles.EnableQueryService = true
		assert.True(t, roles.HasAnyRole())
	}

	{
		var roles MilvusRoles
		roles.EnableQueryNode = true
		assert.True(t, roles.HasAnyRole())
	}

	{
		var roles MilvusRoles
		roles.EnableDataService = true
		assert.True(t, roles.HasAnyRole())
	}

	{
		var roles MilvusRoles
		roles.EnableDataNode = true
		assert.True(t, roles.HasAnyRole())
	}

	{
		var roles MilvusRoles
		roles.EnableIndexService = true
		assert.True(t, roles.HasAnyRole())
	}

	{
		var roles MilvusRoles
		roles.EnableIndexNode = true
		assert.True(t, roles.HasAnyRole())
	}

	{
		var roles MilvusRoles
		roles.EnableMsgStreamService = true
		assert.True(t, roles.HasAnyRole())
	}
}

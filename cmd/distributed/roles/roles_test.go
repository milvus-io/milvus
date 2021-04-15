package roles

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRoles(t *testing.T) {
	r := MilvusRoles{}

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
}

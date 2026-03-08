package typeutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestVersion(t *testing.T) {
	assert.True(t, VersionInt64(1).GT(VersionInt64(0)))
	assert.True(t, VersionInt64(0).EQ(VersionInt64(0)))
	v := VersionInt64(0)
	assert.True(t, VersionInt64(1).GT(&v))
	assert.True(t, VersionInt64(0).EQ(&v))
	assert.Panics(t, func() {
		VersionInt64(0).GT(VersionInt64Pair{Global: 1, Local: 1})
	})

	assert.True(t, VersionInt64Pair{Global: 1, Local: 2}.GT(VersionInt64Pair{Global: 1, Local: 1}))
	assert.True(t, VersionInt64Pair{Global: 2, Local: 0}.GT(VersionInt64Pair{Global: 1, Local: 1}))
	assert.True(t, VersionInt64Pair{Global: 1, Local: 1}.EQ(VersionInt64Pair{Global: 1, Local: 1}))
	v2 := VersionInt64Pair{Global: 1, Local: 1}
	assert.True(t, VersionInt64Pair{Global: 1, Local: 2}.GT(&v2))
	assert.True(t, VersionInt64Pair{Global: 2, Local: 0}.GT(&v2))
	assert.True(t, VersionInt64Pair{Global: 1, Local: 1}.EQ(&v2))
	assert.Panics(t, func() {
		VersionInt64Pair{Global: 1, Local: 2}.GT(VersionInt64(0))
	})
}

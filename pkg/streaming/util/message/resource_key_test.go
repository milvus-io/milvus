package message

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
)

func TestResourceKey(t *testing.T) {
	rk := NewSharedClusterResourceKey()
	assert.Equal(t, rk.Domain, messagespb.ResourceDomain_ResourceDomainCluster)
	assert.Equal(t, rk.Key, "")
	assert.Equal(t, rk.Shared, true)
	rk = NewExclusiveClusterResourceKey()
	assert.Equal(t, rk.Domain, messagespb.ResourceDomain_ResourceDomainCluster)
	assert.Equal(t, rk.Key, "")
	assert.Equal(t, rk.Shared, false)

	rk = NewSharedCollectionNameResourceKey("test", "test")
	assert.Equal(t, rk.Domain, messagespb.ResourceDomain_ResourceDomainCollectionName)
	assert.Equal(t, rk.Key, "test:test")
	assert.Equal(t, rk.Shared, true)
	rk = NewExclusiveCollectionNameResourceKey("test", "test")
	assert.Equal(t, rk.Domain, messagespb.ResourceDomain_ResourceDomainCollectionName)
	assert.Equal(t, rk.Key, "test:test")
	assert.Equal(t, rk.Shared, false)

	rk = NewSharedDBNameResourceKey("test")
	assert.Equal(t, rk.Domain, messagespb.ResourceDomain_ResourceDomainDBName)
	assert.Equal(t, rk.Key, "test")
	assert.Equal(t, rk.Shared, true)
	rk = NewExclusiveDBNameResourceKey("test")
	assert.Equal(t, rk.Domain, messagespb.ResourceDomain_ResourceDomainDBName)
	assert.Equal(t, rk.Key, "test")
	assert.Equal(t, rk.Shared, false)

	rk = NewExclusivePrivilegeResourceKey()
	assert.Equal(t, rk.Domain, messagespb.ResourceDomain_ResourceDomainPrivilege)
	assert.Equal(t, rk.Key, "")
	assert.Equal(t, rk.Shared, false)
}

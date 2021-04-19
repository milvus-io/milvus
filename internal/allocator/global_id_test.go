package allocator

import (
	"os"
	"testing"

	"github.com/zilliztech/milvus-distributed/internal/util/funcutil"

	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-distributed/internal/util/tsoutil"
)

var gTestIDAllocator *GlobalIDAllocator

func TestGlobalTSOAllocator_All(t *testing.T) {
	etcdAddress := os.Getenv("ETCD_ADDRESS")
	if etcdAddress == "" {
		ip := funcutil.GetLocalIP()
		etcdAddress = ip + ":2379"
	}
	gTestIDAllocator = NewGlobalIDAllocator("idTimestamp", tsoutil.NewTSOKVBase([]string{etcdAddress}, "/test/root/kv", "gidTest"))

	t.Run("Initialize", func(t *testing.T) {
		err := gTestIDAllocator.Initialize()
		assert.Nil(t, err)
	})

	t.Run("AllocOne", func(t *testing.T) {
		one, err := gTestIDAllocator.AllocOne()
		assert.Nil(t, err)
		ano, err := gTestIDAllocator.AllocOne()
		assert.Nil(t, err)
		assert.NotEqual(t, one, ano)
	})

	t.Run("Alloc", func(t *testing.T) {
		count := uint32(2 << 10)
		idStart, idEnd, err := gTestIDAllocator.Alloc(count)
		assert.Nil(t, err)
		assert.Equal(t, count, uint32(idEnd-idStart))
	})

}

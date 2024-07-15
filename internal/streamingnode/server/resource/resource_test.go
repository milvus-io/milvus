package resource

import (
	"testing"

	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/internal/mocks"
)

func TestInit(t *testing.T) {
	assert.Panics(t, func() {
		Init()
	})
	assert.Panics(t, func() {
		Init(OptETCD(&clientv3.Client{}))
	})
	assert.Panics(t, func() {
		Init(OptRootCoordClient(mocks.NewMockRootCoordClient(t)))
	})
	Init(OptETCD(&clientv3.Client{}), OptRootCoordClient(mocks.NewMockRootCoordClient(t)))

	assert.NotNil(t, Resource().TimestampAllocator())
	assert.NotNil(t, Resource().ETCD())
	assert.NotNil(t, Resource().RootCoordClient())
}

func TestInitForTest(t *testing.T) {
	InitForTest()
}

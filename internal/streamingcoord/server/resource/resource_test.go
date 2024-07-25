package resource

import (
	"testing"

	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
)

func TestInit(t *testing.T) {
	assert.Panics(t, func() {
		Init()
	})
	assert.Panics(t, func() {
		Init(OptETCD(&clientv3.Client{}))
	})
	assert.Panics(t, func() {
		Init(OptStreamingCatalog(
			mock_metastore.NewMockStreamingCoordCataLog(t),
		))
	})
}

func TestInitForTest(t *testing.T) {
	InitForTest()
}

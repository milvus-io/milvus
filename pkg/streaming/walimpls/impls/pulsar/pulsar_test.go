package pulsar

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/registry"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestMain(m *testing.M) {
	paramtable.Init()
	m.Run()
}

func TestRegistry(t *testing.T) {
	registeredB := registry.MustGetBuilder(walName)
	assert.NotNil(t, registeredB)
	assert.Equal(t, walName, registeredB.Name())

	id, err := message.UnmarshalMessageID(walName,
		newMessageIDOfPulsar(1, 2, 3).Marshal())
	assert.NoError(t, err)
	assert.True(t, id.EQ(newMessageIDOfPulsar(1, 2, 3)))
}

func TestPulsar(t *testing.T) {
	walimpls.NewWALImplsTestFramework(t, 100, &builderImpl{}).Run()
}

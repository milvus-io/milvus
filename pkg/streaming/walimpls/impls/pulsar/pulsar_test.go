package pulsar

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/registry"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestMain(m *testing.M) {
	paramtable.Init()
	m.Run()
}

func TestTenant(t *testing.T) {
	tenant := tenant{
		tenant:    "milvus",
		namespace: "aaa",
	}
	assert.Equal(t, "milvus/aaa/test", tenant.MustGetFullTopicName("test"))
}

func TestRegistry(t *testing.T) {
	registeredB := registry.MustGetBuilder(message.WALNamePulsar)
	assert.NotNil(t, registeredB)
	assert.Equal(t, message.WALNamePulsar, registeredB.Name())

	id, err := message.UnmarshalMessageID(&commonpb.MessageID{
		WALName: commonpb.WALName(message.WALNamePulsar),
		Id:      newMessageIDOfPulsar(1, 2, 3).Marshal(),
	})
	assert.NoError(t, err)
	assert.True(t, id.EQ(newMessageIDOfPulsar(1, 2, 3)))
}

func TestPulsar(t *testing.T) {
	walimpls.NewWALImplsTestFramework(t, 100, &builderImpl{}).Run()
}

func TestReadOnlyMissingTopicDoesNotAutoCreate(t *testing.T) {
	opener, err := (&builderImpl{}).Build()
	require.NoError(t, err)
	defer opener.Close()

	topic := fmt.Sprintf("missing-historical-topic-%d", time.Now().UnixNano())
	wal, err := opener.Open(context.Background(), &walimpls.OpenOption{
		Channel: types.PChannelInfo{Name: topic, AccessMode: types.AccessModeRO},
	})
	require.Nil(t, wal)
	require.ErrorIs(t, err, merr.ErrMqTopicNotFound)
}

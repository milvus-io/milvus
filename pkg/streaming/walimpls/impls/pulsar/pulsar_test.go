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

func TestPulsarExclusiveProducer(t *testing.T) {
	opener, err := (&builderImpl{}).Build()
	require.NoError(t, err)
	defer opener.Close()

	pchannel := types.PChannelInfo{
		Name:       fmt.Sprintf("test-exclusive-producer-%d", time.Now().UnixNano()),
		Term:       1,
		AccessMode: types.AccessModeRW,
	}
	first, err := opener.Open(context.Background(), &walimpls.OpenOption{Channel: pchannel})
	require.NoError(t, err)
	firstClosed := false
	defer func() {
		if !firstClosed {
			first.Close()
		}
	}()

	pchannel.Term = 2
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	second, err := opener.Open(ctx, &walimpls.OpenOption{Channel: pchannel})
	cancel()
	require.ErrorIs(t, err, context.DeadlineExceeded, "the second open should keep failing while the first producer is connected")
	require.ErrorContains(t, err, "create pulsar producer")
	require.Nil(t, second)

	first.Close()
	firstClosed = true

	ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	second, err = opener.Open(ctx, &walimpls.OpenOption{Channel: pchannel})
	require.NoError(t, err)
	defer second.Close()
	_, err = second.Append(ctx, message.CreateTestEmptyInsertMesage(1, map[string]string{}))
	require.NoError(t, err)
}

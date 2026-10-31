package rmq

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/mq/mqimpl/rocksmq/server"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/registry"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestMain(m *testing.M) {
	paramtable.Init()
	tmpPath, err := os.MkdirTemp("", "rocksdb_test")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpPath)
	server.InitRocksMQ(tmpPath)
	defer server.CloseRocksMQ()
	m.Run()
}

func TestRegistry(t *testing.T) {
	registeredB := registry.MustGetBuilder(message.WALNameRocksmq)
	assert.NotNil(t, registeredB)
	assert.Equal(t, message.WALNameRocksmq, registeredB.Name())

	id, err := message.UnmarshalMessageID(&commonpb.MessageID{
		WALName: commonpb.WALName(message.WALNameRocksmq),
		Id:      rmqID(1).Marshal(),
	})
	assert.NoError(t, err)
	assert.True(t, id.EQ(rmqID(1)))

	id, err = message.UnmarshalMessageID(rmqID(-1).IntoProto())
	assert.NoError(t, err)
	assert.True(t, id.EQ(rmqID(-1)))
}

func TestReadWriteWALClosePreservesHistoryForCatchUp(t *testing.T) {
	opener, err := (&builderImpl{}).Build()
	require.NoError(t, err)
	defer opener.Close()

	channel := types.PChannelInfo{
		Name:       "historical-read-after-rw-close",
		AccessMode: types.AccessModeRW,
	}
	w, err := opener.Open(context.Background(), &walimpls.OpenOption{Channel: channel})
	require.NoError(t, err)
	msgID, err := w.Append(context.Background(), message.CreateTestEmptyInsertMesage(1, map[string]string{"id": "1"}))
	require.NoError(t, err)
	w.Close()

	channel.AccessMode = types.AccessModeRO
	roWAL, err := opener.Open(context.Background(), &walimpls.OpenOption{Channel: channel})
	require.NoError(t, err)
	defer roWAL.Close()

	scanner, err := roWAL.Read(context.Background(), walimpls.ReadOption{
		Name:          "historical-read-after-rw-close",
		DeliverPolicy: options.DeliverPolicyAll(),
	})
	require.NoError(t, err)
	defer scanner.Close()

	msg := <-scanner.Chan()
	require.NotNil(t, msg)
	require.True(t, msgID.EQ(msg.MessageID()))
}

func TestWAL(t *testing.T) {
	walimpls.NewWALImplsTestFramework(t, 1000, &builderImpl{}).Run()
}

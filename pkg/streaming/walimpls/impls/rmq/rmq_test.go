package rmq

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/mq/mqimpl/rocksmq/server"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/registry"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var testRocksMQPath string

func TestMain(m *testing.M) {
	paramtable.Init()
	paramtable.SetRole(typeutil.StandaloneRole)
	var err error
	testRocksMQPath, err = os.MkdirTemp("", "rocksdb_test")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(testRocksMQPath)
	paramtable.Get().Save(paramtable.Get().RocksmqCfg.Path.Key, testRocksMQPath)
	defer server.CloseRocksMQ()
	m.Run()
}

func TestBuilderRejectsClusterMode(t *testing.T) {
	oldRole := paramtable.GetRole()
	paramtable.SetRole(typeutil.StreamingNodeRole)
	defer paramtable.SetRole(oldRole)

	opener, err := (&builderImpl{}).Build()
	require.Nil(t, opener)
	require.Error(t, err)
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

func TestBuilderLazyInitializesRocksMQ(t *testing.T) {
	const historicalTopic = "historical-topic"
	existingRocksMQ, err := server.NewRocksMQ(testRocksMQPath)
	require.NoError(t, err)
	require.NoError(t, existingRocksMQ.CreateTopic(historicalTopic))
	existingRocksMQ.Close()

	require.Nil(t, server.Rmq)
	opener, err := (&builderImpl{}).Build()
	require.NoError(t, err)
	require.NotNil(t, opener)
	require.NotNil(t, server.Rmq)
	require.NoError(t, server.Rmq.CheckTopicValid(historicalTopic))
	opener.Close()
}

func TestWAL(t *testing.T) {
	walimpls.NewWALImplsTestFramework(t, 1000, &builderImpl{}).Run()
}

func TestMissingPositionBehaviorByAccessMode(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	opener, err := (&builderImpl{}).Build()
	require.NoError(t, err)
	defer opener.Close()

	channel := types.PChannelInfo{
		Name:       "rmq-missing-position-by-access-mode",
		Term:       1,
		AccessMode: types.AccessModeRW,
	}
	rwWAL, err := opener.Open(ctx, &walimpls.OpenOption{Channel: channel})
	require.NoError(t, err)
	defer rwWAL.Close()

	appendedID, err := rwWAL.Append(ctx, message.CreateTestEmptyInsertMesage(1, nil))
	require.NoError(t, err)

	missingID := rmqID(1 << 30)
	roChannel := channel
	roChannel.AccessMode = types.AccessModeRO
	roWAL, err := opener.Open(ctx, &walimpls.OpenOption{Channel: roChannel})
	require.NoError(t, err)
	defer roWAL.Close()

	_, err = roWAL.Read(ctx, walimpls.ReadOption{
		Name:          "ro-missing-position",
		DeliverPolicy: options.DeliverPolicyStartFrom(missingID),
	})
	require.ErrorIs(t, err, merr.ErrMqTopicNotFound)

	rwScanner, err := rwWAL.Read(ctx, walimpls.ReadOption{
		Name:          "rw-missing-position",
		DeliverPolicy: options.DeliverPolicyStartFrom(missingID),
	})
	require.NoError(t, err)
	defer rwScanner.Close()

	select {
	case msg := <-rwScanner.Chan():
		require.NotNil(t, msg)
		require.True(t, msg.MessageID().EQ(appendedID))
	case <-ctx.Done():
		t.Fatal("current RocksMQ reader did not recover from the missing position")
	}
}

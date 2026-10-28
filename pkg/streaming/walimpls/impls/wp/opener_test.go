package wp

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	mocks_log_handle "github.com/zilliztech/woodpecker/mocks/mocks_woodpecker/mocks_log_handle"
	"github.com/zilliztech/woodpecker/woodpecker"
	wplog "github.com/zilliztech/woodpecker/woodpecker/log"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
)

type testWoodpeckerClient struct {
	woodpecker.Client
	logHandle wplog.LogHandle
}

func (c *testWoodpeckerClient) LogExists(context.Context, string) (bool, error) {
	return true, nil
}

func (c *testWoodpeckerClient) OpenLog(context.Context, string) (wplog.LogHandle, error) {
	return c.logHandle, nil
}

// The dependency's generated mock trails optional LogHandle methods. Embed the
// complete interface and forward the operations this opener actually uses.
type testOpenerLogHandle struct {
	wplog.LogHandle
	mock *mocks_log_handle.LogHandle
}

func (l testOpenerLogHandle) OpenLogWriter(ctx context.Context) (wplog.LogWriter, error) {
	return l.mock.OpenLogWriter(ctx)
}

func (l testOpenerLogHandle) OpenLogReader(ctx context.Context, from *wplog.LogMessageId, name string) (wplog.LogReader, error) {
	return l.mock.OpenLogReader(ctx, from, name)
}
func (l testOpenerLogHandle) Close(ctx context.Context) error { return l.mock.Close(ctx) }

func TestOpenReadOnlyWALDoesNotOpenWriter(t *testing.T) {
	logHandle := mocks_log_handle.NewLogHandle(t)
	logReader := mocks_log_handle.NewLogReader(t)
	logReader.EXPECT().ReadNext(mock.Anything).RunAndReturn(func(ctx context.Context) (*wplog.LogMessage, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}).Once()
	logReader.EXPECT().Close(mock.Anything).Return(nil).Once()
	logHandle.EXPECT().OpenLogReader(mock.Anything, mock.Anything, "read-only-test").Return(logReader, nil).Once()
	logHandle.EXPECT().Close(mock.Anything).Return(nil).Once()
	client := &testWoodpeckerClient{
		logHandle: testOpenerLogHandle{mock: logHandle},
	}
	opener := &openerImpl{c: client}

	opened, err := opener.Open(context.Background(), &walimpls.OpenOption{
		Channel: types.PChannelInfo{
			Name:       "test-channel",
			Term:       1,
			AccessMode: types.AccessModeRO,
		},
	})
	require.NoError(t, err)
	w := opened.(*walImpl)
	assert.Nil(t, w.p)
	scanner, err := w.Read(context.Background(), walimpls.ReadOption{
		Name:          "read-only-test",
		DeliverPolicy: options.DeliverPolicyAll(),
	})
	require.NoError(t, err)
	require.NoError(t, scanner.Close())
	w.Close()
}

package wp

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	wpmeta "github.com/zilliztech/woodpecker/meta"
	mocks_log_handle "github.com/zilliztech/woodpecker/mocks/mocks_woodpecker/mocks_log_handle"
	"github.com/zilliztech/woodpecker/woodpecker"
	wplog "github.com/zilliztech/woodpecker/woodpecker/log"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
)

type testWoodpeckerClient struct {
	logExists bool
	logHandle wplog.LogHandle
	created   bool
}

var _ woodpecker.Client = (*testWoodpeckerClient)(nil)

func (c *testWoodpeckerClient) CreateLog(context.Context, string) error {
	c.created = true
	return nil
}

func (c *testWoodpeckerClient) OpenLog(context.Context, string) (wplog.LogHandle, error) {
	return c.logHandle, nil
}

func (c *testWoodpeckerClient) DeleteLog(context.Context, string) error {
	return nil
}

func (c *testWoodpeckerClient) DeleteAllLogs(context.Context) error {
	return nil
}

func (c *testWoodpeckerClient) LogExists(context.Context, string) (bool, error) {
	return c.logExists, nil
}

func (c *testWoodpeckerClient) GetAllLogs(context.Context) ([]string, error) {
	return nil, nil
}

func (c *testWoodpeckerClient) GetLogsWithPrefix(context.Context, string) ([]string, error) {
	return nil, nil
}

func (c *testWoodpeckerClient) GetMetadataProvider() wpmeta.MetadataProvider {
	return nil
}

func (c *testWoodpeckerClient) Close(context.Context) error {
	return nil
}

func TestOpenReadOnlyWALDoesNotOpenWriter(t *testing.T) {
	logHandle := mocks_log_handle.NewLogHandle(t)
	logHandle.EXPECT().Close(mock.Anything).Return(nil).Once()
	client := &testWoodpeckerClient{
		logExists: true,
		logHandle: logHandle,
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
	assert.False(t, client.created)
	w.Close()
}

func TestOpenMissingReadOnlyWALDoesNotCreateLog(t *testing.T) {
	client := &testWoodpeckerClient{logExists: false}
	opener := &openerImpl{c: client}

	opened, err := opener.Open(context.Background(), &walimpls.OpenOption{
		Channel: types.PChannelInfo{
			Name:       "missing-channel",
			Term:       1,
			AccessMode: types.AccessModeRO,
		},
	})
	require.Error(t, err)
	assert.Nil(t, opened)
	assert.False(t, client.created)
}

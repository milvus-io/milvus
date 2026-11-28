package kvfactory

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// fakeEtcdClientCreator replaces createEtcdClientFunc. The first `failures`
// calls return err; later calls return a client that never dials.
func fakeEtcdClientCreator(t *testing.T, failures int, err error) *int {
	t.Helper()
	calls := 0
	origCreate := createEtcdClientFunc
	origSleep, origMaxSleep := createEtcdClientRetrySleep, createEtcdClientRetryMaxSleep
	createEtcdClientRetrySleep, createEtcdClientRetryMaxSleep = time.Millisecond, 2*time.Millisecond
	createEtcdClientFunc = func() (*clientv3.Client, error) {
		calls++
		if calls <= failures {
			return nil, err
		}
		return clientv3.NewCtxClient(context.Background()), nil
	}
	t.Cleanup(func() {
		createEtcdClientFunc = origCreate
		createEtcdClientRetrySleep, createEtcdClientRetryMaxSleep = origSleep, origMaxSleep
		forgetEtcdClient()
	})
	forgetEtcdClient()
	return &calls
}

// forgetEtcdClient drops the cached client without closing it: the fake
// client has no connection, so Close would report context.Canceled and
// CloseEtcdClient would panic.
func forgetEtcdClient() {
	clientCreator.mu.Lock()
	defer clientCreator.mu.Unlock()
	clientCreator.client = nil
	clientCreator.rootpath = nil
}

func setSessionRetryTimes(t *testing.T, n int) {
	t.Helper()
	paramtable.Init()
	key := paramtable.Get().CommonCfg.SessionRetryTimes.Key
	paramtable.Get().Save(key, strconv.Itoa(n))
	t.Cleanup(func() { paramtable.Get().Reset(key) })
}

func TestGetEtcdAndPathRetriesWhileEtcdIsUnreachable(t *testing.T) {
	setSessionRetryTimes(t, 5)
	dialTimeout := fmt.Errorf("dial etcd: %w", context.DeadlineExceeded)

	tests := []struct {
		name      string
		failures  int
		err       error
		wantPanic bool
		wantCalls int
	}{
		{name: "etcd reachable at once", failures: 0, err: dialTimeout, wantCalls: 1},
		{name: "etcd comes back after a few dial timeouts", failures: 3, err: dialTimeout, wantCalls: 4},
		{name: "etcd stays unreachable", failures: 100, err: dialTimeout, wantPanic: true, wantCalls: 5},
		{name: "non-retriable error fails at once", failures: 100, err: errors.New("load etcd cert key pair error"), wantPanic: true, wantCalls: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calls := fakeEtcdClientCreator(t, tt.failures, tt.err)
			if tt.wantPanic {
				assert.Panics(t, func() { getEtcdAndPath() })
				assert.Equal(t, tt.wantCalls, *calls)
				return
			}
			var client *clientv3.Client
			var path string
			require.NotPanics(t, func() { client, path = getEtcdAndPath() })
			assert.NotNil(t, client)
			assert.Equal(t, paramtable.Get().EtcdCfg.MetaRootPath.GetValue(), path)
			assert.Equal(t, tt.wantCalls, *calls)

			// The client is cached: later calls don't create another one.
			again, _ := getEtcdAndPath()
			assert.Same(t, client, again)
			assert.Equal(t, tt.wantCalls, *calls)
		})
	}
}

func TestIsRetriableCreateEtcdClientErr(t *testing.T) {
	assert.True(t, isRetriableCreateEtcdClientErr(context.DeadlineExceeded))
	assert.True(t, isRetriableCreateEtcdClientErr(fmt.Errorf("wrapped: %w", context.DeadlineExceeded)))
	assert.False(t, isRetriableCreateEtcdClientErr(errors.New("load etcd CACert file error")))
	assert.False(t, isRetriableCreateEtcdClientErr(nil))
}

// With etcd auth enabled, clientv3.New authenticates before it returns, so an
// unreachable etcd fails client creation after etcd.dialTimeout. That error
// must be one createEtcdClientWithRetry retries, or the retry never happens.
// (Without auth, clientv3.New does not contact etcd at all: it dials with
// grpc.NewClient, which ignores grpc.WithBlock.)
func TestCreateEtcdClientWithAuthUnreachableIsRetriable(t *testing.T) {
	paramtable.Init()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())

	params := paramtable.Get()
	for key, value := range map[string]string{
		params.EtcdCfg.UseEmbedEtcd.Key:     "false",
		params.EtcdCfg.EtcdEnableAuth.Key:   "true",
		params.EtcdCfg.EtcdAuthUserName.Key: "test-user",
		params.EtcdCfg.EtcdAuthPassword.Key: "test-password",
		params.EtcdCfg.EtcdUseSSL.Key:       "false",
		params.EtcdCfg.Endpoints.Key:        addr,
		params.EtcdCfg.DialTimeout.Key:      "300",
	} {
		params.Save(key, value)
		t.Cleanup(func() { params.Reset(key) })
	}

	client, err := createEtcdClient()
	if client != nil {
		client.Close()
	}
	require.Error(t, err)
	assert.True(t, isRetriableCreateEtcdClientErr(err), "dial error %v should be retriable", err)
}

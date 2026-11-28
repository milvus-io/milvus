package kvfactory

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/etcd"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
)

var clientCreator = &etcdClientCreator{}

var getEtcdAndPathFunction = getEtcdAndPath

// createEtcdClientFunc is swapped in tests to simulate an unreachable etcd.
var createEtcdClientFunc = createEtcdClient

// Backoff between attempts to create the etcd client at startup. The number of
// attempts is common.session.retryTimes, the budget the session already uses
// for its own etcd requests.
var (
	createEtcdClientRetrySleep    = 200 * time.Millisecond
	createEtcdClientRetryMaxSleep = 3 * time.Second
)

type etcdClientCreator struct {
	mu       sync.Mutex
	client   *clientv3.Client
	rootpath *string
}

// Returns an Etcd client and the metaRootPath, if an error is hit, will panic.
// This client is reused by all calls in the current runtime.
func GetEtcdAndPath() (*clientv3.Client, string) {
	client, path := getEtcdAndPathFunction()
	return client, path
}

// Reset the stored client, mainly used during testing when paramtable params have changed
// during runtime.
func CloseEtcdClient() {
	clientCreator.mu.Lock()
	defer clientCreator.mu.Unlock()
	if clientCreator.client != nil {
		err := clientCreator.client.Close()
		if err != nil {
			panic(err)
		}
	}
	clientCreator.client = nil
	clientCreator.rootpath = nil
}

// Returns an Etcd client and the metaRootPath, if an error is hit, will panic
func getEtcdAndPath() (*clientv3.Client, string) {
	clientCreator.mu.Lock()
	defer clientCreator.mu.Unlock()
	// If client/path doesn’t exist, create a new one
	if clientCreator.client == nil {
		var err error
		clientCreator.client, err = createEtcdClientWithRetry()
		if err != nil {
			panic(fmt.Errorf("failed to create etcd client: %w", err))
		}
		path := paramtable.Get().EtcdCfg.MetaRootPath.GetValue()
		clientCreator.rootpath = &path
	}
	return clientCreator.client, *clientCreator.rootpath
}

// createEtcdClientWithRetry creates the etcd client, retrying with bounded
// exponential backoff while etcd is unreachable. Without it, a component that
// starts during a short etcd outage (a rolling etcd restart, a network blip, or
// a restart right after its session expired) panics after one dial timeout and
// Kubernetes puts it into CrashLoopBackOff, which outlasts the outage itself.
// Errors that retrying cannot fix, such as an unreadable TLS certificate or an
// authentication failure, are returned at once.
func createEtcdClientWithRetry() (*clientv3.Client, error) {
	attempts := paramtable.Get().CommonCfg.SessionRetryTimes.GetAsInt64()
	if attempts < 1 {
		attempts = 1
	}
	var client *clientv3.Client
	attempt := 0
	err := retry.Do(context.Background(), func() error {
		attempt++
		c, err := createEtcdClientFunc()
		if err != nil {
			mlog.Warn(context.TODO(), "failed to create etcd client, etcd may be unreachable",
				mlog.Int("attempt", attempt),
				mlog.Int64("maxAttempts", attempts),
				mlog.Err(err))
			return err
		}
		client = c
		return nil
	},
		retry.Attempts(uint(attempts)),
		retry.Sleep(createEtcdClientRetrySleep),
		retry.MaxSleepTime(createEtcdClientRetryMaxSleep),
		retry.RetryErr(isRetriableCreateEtcdClientErr),
	)
	if err != nil {
		return nil, err
	}
	return client, nil
}

// isRetriableCreateEtcdClientErr reports whether creating the etcd client may
// succeed later. With etcd auth enabled, clientv3.New authenticates before it
// returns and fails with context.DeadlineExceeded when etcd can't be reached
// within etcd.dialTimeout.
func isRetriableCreateEtcdClientErr(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || etcd.IsRetriableEtcdErr(err)
}

// Function that calls the Etcd constructor
func createEtcdClient() (*clientv3.Client, error) {
	cfg := &paramtable.Get().ServiceParam
	options := cfg.EtcdCfg.ClientOptions()
	return etcd.CreateEtcdClient(
		cfg.EtcdCfg.UseEmbedEtcd.GetAsBool(),
		cfg.EtcdCfg.EtcdEnableAuth.GetAsBool(),
		cfg.EtcdCfg.EtcdAuthUserName.GetValue(),
		cfg.EtcdCfg.EtcdAuthPassword.GetValue(),
		cfg.EtcdCfg.EtcdUseSSL.GetAsBool(),
		cfg.EtcdCfg.Endpoints.GetAsStrings(),
		cfg.EtcdCfg.EtcdTLSCert.GetValue(),
		cfg.EtcdCfg.EtcdTLSKey.GetValue(),
		cfg.EtcdCfg.EtcdTLSCACert.GetValue(),
		cfg.EtcdCfg.EtcdTLSMinVersion.GetValue(),
		options...)
}

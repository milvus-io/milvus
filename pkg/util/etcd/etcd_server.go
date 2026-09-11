package etcd

import (
	"context"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3client"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// EtcdServer is the singleton of embedded etcd server
var (
	initOnce   sync.Once
	closeOnce  sync.Once
	etcdServer *embed.Etcd
	// initError records the singleton's first initialization result. It is a
	// package-level variable on purpose: sync.Once never re-runs the init
	// closure, so a later InitEtcdServer call must still observe the failure
	// (otherwise it would return nil while etcdServer is nil).
	initError error
)

// GetEmbedEtcdClient returns client of embed etcd server
func GetEmbedEtcdClient() (*clientv3.Client, error) {
	if etcdServer == nil {
		return nil, merr.WrapErrServiceUnavailableMsg("embedded etcd server is not initialized")
	}
	client := v3client.New(etcdServer.Server)
	return client, nil
}

// InitEtcdServer initializes embedded etcd server singleton.
func InitEtcdServer(
	useEmbedEtcd bool,
	configPath string,
	dataDir string,
	logPath string,
	logLevel string,
) error {
	if useEmbedEtcd {
		initOnce.Do(func() {
			path := configPath
			var cfg *embed.Config
			if len(path) > 0 {
				cfgFromFile, err := embed.ConfigFromFile(path)
				if err != nil {
					initError = err
					return
				}
				cfg = cfgFromFile
			} else {
				cfg = embed.NewConfig()
			}
			cfg.Dir = dataDir
			cfg.LogOutputs = []string{logPath}
			cfg.LogLevel = logLevel
			e, err := embed.StartEtcd(cfg)
			if err != nil {
				mlog.Error(context.TODO(), "failed to init embedded Etcd server", mlog.Err(err))
				initError = err
				return
			}
			// embed.StartEtcd returns once the server is serving traffic, but a
			// single-member cluster has not necessarily elected itself leader
			// yet. Wait until etcd is ready (leader elected + member published)
			// before returning, otherwise components starting right after would
			// race the leader election and hit transient "etcdserver: leader
			// changed" errors during session initialization.
			if err := waitEtcdServerReady(e); err != nil {
				mlog.Error(context.TODO(), "embedded Etcd server failed to become ready", mlog.Err(err))
				initError = err
				return
			}
			// Only publish the singleton after etcd is fully ready. Assigning it
			// earlier would leave HasServer()/GetEmbedEtcdClient() pointing at a
			// stopped server if the readiness wait times out.
			etcdServer = e
			mlog.Info(context.TODO(), "finish init Etcd config", mlog.String("path", path), mlog.String("data", dataDir))
		})
		return initError
	}
	return nil
}

func HasServer() bool {
	return etcdServer != nil
}

// waitEtcdServerReady blocks until the embedded etcd server has elected a
// leader and published its member, i.e. it can serve linearizable requests.
// embed.StartEtcd returns as soon as the server is serving traffic, which for
// a single-member cluster happens before the leader election completes.
func waitEtcdServerReady(e *embed.Etcd) error {
	select {
	case <-e.Server.ReadyNotify():
		return nil
	case <-time.After(60 * time.Second):
		// Close releases the client/peer listeners (2379/2380) in addition to
		// stopping the server (Close already stops the server internally), so
		// the ports are freed even if the process keeps running (e.g. the
		// cmd/embedded export path).
		e.Close()
		return merr.WrapErrServiceInternalMsg("embedded etcd took too long to become ready")
	}
}

// StopEtcdServer stops embedded etcd server singleton.
func StopEtcdServer() {
	if etcdServer != nil {
		closeOnce.Do(func() {
			etcdServer.Close()
		})
	}
}

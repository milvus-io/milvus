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
	initError  error
	closeOnce  sync.Once
	etcdServer *embed.Etcd
)

// GetEmbedEtcdClient returns client of embed etcd server
func GetEmbedEtcdClient() (*clientv3.Client, error) {
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
			e, err := startEmbeddedEtcd(cfg, 60*time.Second)
			if err != nil {
				mlog.Error(context.TODO(), "failed to init embedded Etcd server", mlog.Err(err))
				initError = err
				return
			}
			etcdServer = e
			mlog.Info(context.TODO(), "finish init Etcd config", mlog.String("path", path), mlog.String("data", dataDir))
		})
		return initError
	}
	return nil
}

// startEmbeddedEtcd waits for the initial Raft election before exposing the
// server to in-process clients, which bypass the network serving readiness gate.
func startEmbeddedEtcd(cfg *embed.Config, timeout time.Duration) (*embed.Etcd, error) {
	e, err := embed.StartEtcd(cfg)
	if err != nil {
		return nil, err
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-e.Server.ReadyNotify():
		return e, nil
	case <-timer.C:
		err = merr.WrapErrServiceUnavailableMsg("embedded etcd did not become ready within %s", timeout)
	case <-e.Server.StopNotify():
		err = merr.WrapErrServiceUnavailableMsg("embedded etcd stopped before becoming ready")
	case err = <-e.Err():
		if err == nil {
			err = merr.WrapErrServiceUnavailableMsg("embedded etcd closed before becoming ready")
		}
	}
	// Client serving goroutines wait for readiness or server shutdown. Stop the
	// server first so Close can join them even if it never became ready.
	e.Server.Stop()
	e.Close()
	return nil, err
}

func HasServer() bool {
	return etcdServer != nil
}

// StopEtcdServer stops embedded etcd server singleton.
func StopEtcdServer() {
	if etcdServer != nil {
		closeOnce.Do(func() {
			etcdServer.Close()
		})
	}
}

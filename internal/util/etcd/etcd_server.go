package etcd

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/retry"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3client"
	"go.uber.org/zap"
)

// EtcdServer is the singleton of embedded etcd server
var (
	mutex      sync.Mutex
	etcdServer *embed.Etcd
)

// GetEmbedEtcdClient returns client of embed etcd server
func GetEmbedEtcdClient() (*clientv3.Client, error) {
	client := v3client.New(etcdServer.Server)
	return client, nil
}

// InitEtcdServer initializes embedded etcd server singleton.
func InitEtcdServer(etcdCfg *paramtable.EtcdConfig) error {
	if etcdCfg.UseEmbedEtcd {
		mutex.Lock()
		defer mutex.Unlock()
		if etcdServer != nil {
			return nil
		}
		path := etcdCfg.ConfigPath
		var cfg *embed.Config
		if len(path) > 0 {
			cfgFromFile, err := embed.ConfigFromFile(path)
			if err != nil {
				return err
			}
			cfg = cfgFromFile
		} else {
			cfg = embed.NewConfig()
		}
		cfg.Dir = etcdCfg.DataDir
		cfg.LogOutputs = []string{etcdCfg.EtcdLogPath}
		cfg.LogLevel = etcdCfg.EtcdLogLevel
		startFunc := func() error {
			e, err := embed.StartEtcd(cfg)
			if err != nil {
				return err
			}
			etcdServer = e
			return nil
		}
		err := retry.Do(context.TODO(), startFunc, retry.Attempts(10))
		if err != nil {
			return err
		}

		log.Info("finish init Etcd config", zap.String("path", path), zap.String("data", etcdCfg.DataDir))
		return nil
	}
	return nil
}

// StopEtcdServer stops embedded etcd server singleton.
func StopEtcdServer() {
	if etcdServer != nil {
		mutex.Lock()
		defer mutex.Unlock()
		if etcdServer != nil {
			log.Warn("etcd server closed")
			etcdServer.Close()
			etcdServer = nil
		}
	}
}

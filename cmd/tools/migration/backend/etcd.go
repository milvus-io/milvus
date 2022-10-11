package backend

import (
	"github.com/milvus-io/milvus/cmd/tools/migration/configs"
	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/util/etcd"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type etcdBasedBackend struct {
	cfg     *configs.MilvusConfig
	txn     kv.MetaKv
	etcdCli *clientv3.Client
}

func (b etcdBasedBackend) CleanWithPrefix(prefix string) error {
	return b.txn.RemoveWithPrefix(prefix)
}

func newEtcdBasedBackend(cfg *configs.MilvusConfig) (*etcdBasedBackend, error) {
	etcdCli, err := etcd.GetEtcdClient(cfg.EtcdCfg)
	if err != nil {
		return nil, err
	}
	txn := etcdkv.NewEtcdKV(etcdCli, cfg.EtcdCfg.MetaRootPath)
	b := &etcdBasedBackend{cfg: cfg, etcdCli: etcdCli, txn: txn}
	return b, nil
}

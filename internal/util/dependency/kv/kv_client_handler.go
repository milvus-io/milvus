package kvfactory

import (
	// "sync"

	// "github.com/cockroachdb/errors"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/txnkv"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

// Mocking purposes
var createETCD = createETCDClient
var createTiKV = createTiKVClient

func NewETCDClient(cfg *paramtable.ServiceParam) (*clientv3.Client, error) {
	return createETCD(cfg)
}

func NewTiKVClient(cfg *paramtable.ServiceParam) (*txnkv.Client, error) {
	return createTiKV(cfg)
}

// Function that calls the ETCD constructor
func createETCDClient(cfg *paramtable.ServiceParam) (*clientv3.Client, error) {
	client, err := etcd.GetEtcdClient(
		cfg.EtcdCfg.UseEmbedEtcd.GetAsBool(),
		cfg.EtcdCfg.EtcdUseSSL.GetAsBool(),
		cfg.EtcdCfg.Endpoints.GetAsStrings(),
		cfg.EtcdCfg.EtcdTLSCert.GetValue(),
		cfg.EtcdCfg.EtcdTLSKey.GetValue(),
		cfg.EtcdCfg.EtcdTLSCACert.GetValue(),
		cfg.EtcdCfg.EtcdTLSMinVersion.GetValue())
	if err != nil {
		return nil, err
	}
	return client, nil
}

// Function that calls the TiKV constructor
func createTiKVClient(cfg *paramtable.ServiceParam) (*txnkv.Client, error) {
	if cfg.TiKVCfg.TiKVUseSSL.GetAsBool() {
		f := func(conf *config.Config) {
			conf.Security = config.NewSecurity(cfg.TiKVCfg.TiKVTLSCACert.GetValue(), cfg.TiKVCfg.TiKVTLSCert.GetValue(), cfg.TiKVCfg.TiKVTLSKey.GetValue(), []string{})
		}
		config.UpdateGlobal(f)
		return txnkv.NewClient([]string{cfg.TiKVCfg.Endpoints.GetValue()})
	}
	return txnkv.NewClient([]string{cfg.TiKVCfg.Endpoints.GetValue()})
}

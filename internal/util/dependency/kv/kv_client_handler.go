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

func NewETCDWithRootWithPanic() (*clientv3.Client, string) {
	client, err := NewETCDClient()
	if err != nil {
		panic(err)
	}
	rootpath := GetETCDRootPath()
	return client, rootpath
}

func NewETCDClient() (*clientv3.Client, error) {
	return createETCD()
}

func NewTiKVClient() (*txnkv.Client, error) {
	return createTiKV()
}

// Function that calls the ETCD constructor
func createETCDClient() (*clientv3.Client, error) {
	cfg := &paramtable.Get().ServiceParam
	return etcd.GetEtcdClient(
		cfg.EtcdCfg.UseEmbedEtcd.GetAsBool(),
		cfg.EtcdCfg.EtcdUseSSL.GetAsBool(),
		cfg.EtcdCfg.Endpoints.GetAsStrings(),
		cfg.EtcdCfg.EtcdTLSCert.GetValue(),
		cfg.EtcdCfg.EtcdTLSKey.GetValue(),
		cfg.EtcdCfg.EtcdTLSCACert.GetValue(),
		cfg.EtcdCfg.EtcdTLSMinVersion.GetValue())
}

// Function that calls the TiKV constructor
func createTiKVClient() (*txnkv.Client, error) {
	cfg := paramtable.Get().ServiceParam
	if cfg.TiKVCfg.TiKVUseSSL.GetAsBool() {
		f := func(conf *config.Config) {
			conf.Security = config.NewSecurity(cfg.TiKVCfg.TiKVTLSCACert.GetValue(), cfg.TiKVCfg.TiKVTLSCert.GetValue(), cfg.TiKVCfg.TiKVTLSKey.GetValue(), []string{})
		}
		config.UpdateGlobal(f)
		return txnkv.NewClient([]string{cfg.TiKVCfg.Endpoints.GetValue()})
	}
	return txnkv.NewClient([]string{cfg.TiKVCfg.Endpoints.GetValue()})
}

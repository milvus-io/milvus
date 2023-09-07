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

// // Used to mock within kv_client_hander_test.go
// var generateETCDClient = CreateETCDClient
// var generateTiKVClient = CreateTiKVClient

// // A struct to hold all of our current clients
// type kvClientHandler struct {
// 	mu         sync.Mutex
// 	tikvClient *txnkv.Client
// 	etcdClient *clientv3.Client
// }

// // Global handler
// var singleKVClientHandler *kvClientHandler

// // Create the global handler
// func init() {
// 	singleKVClientHandler = &kvClientHandler{
// 		mu:         sync.Mutex{},
// 		tikvClient: nil,
// 		etcdClient: nil,
// 	}

// }

// // Function to grab the ETCD client, will create one if one doesnt exist already
// func GrabETCD(cfg *paramtable.ServiceParam) (*clientv3.Client, error) {
// 	// Make sure to lock before altering
// 	singleKVClientHandler.mu.Lock()
// 	defer singleKVClientHandler.mu.Unlock()
// 	// If client doesnt exist, make one
// 	if singleKVClientHandler.etcdClient == nil {
// 		new_client, err := generateETCDClient(cfg)
// 		if err != nil {
// 			return nil, errors.Wrapf(err, "Failed to create ETCD client")
// 		}
// 		singleKVClientHandler.etcdClient = new_client
// 	}
// 	return singleKVClientHandler.etcdClient, nil
// }

// Function that calls the ETCD constructor
func CreateETCDClient(cfg *paramtable.ServiceParam) (*clientv3.Client, error) {
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

// // Function to grab the TiKV client, will create one if one doesnt exist already
// func GrabTiKVClient(cfg *paramtable.ServiceParam) (*txnkv.Client, error) {
// 	// Make sure to lock before altering
// 	singleKVClientHandler.mu.Lock()
// 	defer singleKVClientHandler.mu.Unlock()
// 	// If client doesnt exist, make one
// 	if singleKVClientHandler.tikvClient == nil {
// 		new_client, err := generateTiKVClient(cfg)
// 		if err != nil {
// 			return nil, errors.Wrapf(err, "create tikv client failed")
// 		}
// 		singleKVClientHandler.tikvClient = new_client
// 	}
// 	return singleKVClientHandler.tikvClient, nil
// }

// Function that calls the TiKV constructor
func CreateTiKVClient(cfg *paramtable.ServiceParam) (*txnkv.Client, error) {
	if cfg.TiKVCfg.TiKVUseSSL.GetAsBool() {
		f := func(conf *config.Config) {
			conf.Security = config.NewSecurity(cfg.TiKVCfg.TiKVTLSCACert.GetValue(), cfg.TiKVCfg.TiKVTLSCert.GetValue(), cfg.TiKVCfg.TiKVTLSKey.GetValue(), []string{})
		}
		config.UpdateGlobal(f)
		return txnkv.NewClient([]string{cfg.TiKVCfg.Endpoints.GetValue()})
	}
	return txnkv.NewClient([]string{cfg.TiKVCfg.Endpoints.GetValue()})
}

// // Reset the handler and clear out any clients it is holding. Mainly for testing
// // purposes
// func ResetKVClientHandler() {
// 	singleKVClientHandler.mu.Lock()
// 	defer singleKVClientHandler.mu.Unlock()

// 	singleKVClientHandler.tikvClient = nil
// 	singleKVClientHandler.etcdClient = nil
// }

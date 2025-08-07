package kvfactory

import (
	"fmt"
	"sync"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/pkg/v2/util/etcd"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

var clientCreator = &etcdClientCreator{}

var getEtcdAndPathFunction = getEtcdAndPath

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
		clientCreator.client, err = createEtcdClient()
		if err != nil {
			panic(fmt.Errorf("failed to create etcd client: %w", err))
		}
		path := paramtable.Get().ServiceParam.EtcdCfg.MetaRootPath.GetValue()
		clientCreator.rootpath = &path
	}
	return clientCreator.client, *clientCreator.rootpath
}

// Function that calls the Etcd constructor
func createEtcdClient() (*clientv3.Client, error) {
	cfg := &paramtable.Get().ServiceParam
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
		cfg.EtcdCfg.EtcdTLSMinVersion.GetValue())
}

package kvfactory

import (
	"context"
	"errors"
	"sync"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

var clientCreator = &etcdClientCreator{}

var getEtcdAndPathFunction = getEtcdAndPath

type etcdClientCreator struct {
	mu       sync.Mutex
	client   *clientv3.Client
	rootpath string
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
		if err != nil && !errors.Is(err, context.Canceled) {
			panic(err)
		}
	}
	clientCreator.client = nil
	clientCreator.rootpath = ""
}

// Returns an Etcd client and the metaRootPath, if an error is hit, will panic
func getEtcdAndPath() (*clientv3.Client, string) {
	clientCreator.mu.Lock()
	defer clientCreator.mu.Unlock()
	if clientCreator.client == nil {
		client, err := paramtable.Get().EtcdCfg.NewClient(context.Background())
		if err != nil {
			panic(merr.Wrap(err, "failed to create etcd client"))
		}
		clientCreator.setClient(client)
	}
	return clientCreator.client, clientCreator.rootpath
}

// InitEtcdClient initializes the shared client before starting any component.
// Subsequent GetEtcdAndPath calls reuse it throughout the process lifetime.
func InitEtcdClient(ctx context.Context) error {
	clientCreator.mu.Lock()
	defer clientCreator.mu.Unlock()
	if clientCreator.client != nil {
		return nil
	}
	client, err := paramtable.Get().EtcdCfg.NewClientAtStartup(ctx)
	if err != nil {
		return err
	}
	clientCreator.setClient(client)
	return nil
}

func (creator *etcdClientCreator) setClient(client *clientv3.Client) {
	creator.rootpath = paramtable.Get().EtcdCfg.MetaRootPath.GetValue()
	creator.client = client
}

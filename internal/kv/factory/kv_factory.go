package kvfactory

import (
	"github.com/tikv/client-go/v2/txnkv"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	tikv "github.com/milvus-io/milvus/internal/kv/tikv"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"go.uber.org/zap"
)

// Factory interface for KV that supports creating MetaKv and TxnKV, reuses existing
// client.
type Factory interface {
	NewMetaKv() kv.MetaKv
	NewTxnKV() kv.TxnKV
	NewWatchKV() kv.WatchKV
	CloseKV()
}

var FatalLogger = FatalLogFunc

// ETCD specific factory that stores only the client
type ETCDFactory struct {
	etcdClient   *clientv3.Client
	metaRootPath string
}

// Create a new ETCD specific factory
func NewETCDFactory(cfg *paramtable.ServiceParam) *ETCDFactory {
	client, err := CreateETCDClient(cfg)
	if err != nil {
		FatalLogger("ETCDFactory failed to grab client", err)
		// We need a return when testing and disabling os.exit
		return nil
	}
	return &ETCDFactory{etcdClient: client, metaRootPath: cfg.EtcdCfg.MetaRootPath.GetValue()}
}

// Create a new Meta KV interface using ETCD
func (fact *ETCDFactory) NewMetaKv() kv.MetaKv {
	kv := etcdkv.NewEtcdKV(fact.etcdClient, fact.metaRootPath)
	return kv
}

// Create a new Txn KV interface using ETCD
func (fact *ETCDFactory) NewTxnKV() kv.TxnKV {
	kv := etcdkv.NewEtcdKV(fact.etcdClient, fact.metaRootPath)
	return kv
}

// Create a new Watch KV interface using ETCD
func (fact *ETCDFactory) NewWatchKV() kv.WatchKV {
	kv := etcdkv.NewEtcdKV(fact.etcdClient, fact.metaRootPath)
	return kv
}

// Close the underlying client
func (fact *ETCDFactory) CloseKV() {
	fact.etcdClient.Close()
}

// TiKV specific factory that stores only the client
type TiKVFactory struct {
	tikvClient   *txnkv.Client
	metaRootPath string
}

// Create a new TiKV specific factory
func NewTiKVFactory(cfg *paramtable.ServiceParam) *TiKVFactory {
	client, err := CreateTiKVClient(cfg)
	if err != nil {
		FatalLogger("TiKVFactory failed to grab client", err)
		// We need a return when testing and disabling os.exit
		return nil
	}
	return &TiKVFactory{tikvClient: client, metaRootPath: cfg.TiKVCfg.MetaRootPath.GetValue()}

}

// Create a new Meta KV interface using TiKV
func (fact *TiKVFactory) NewMetaKv() kv.MetaKv {
	kv := tikv.NewTiKV(fact.tikvClient, fact.metaRootPath)
	return kv
}

// Create a new Txn KV interface using TiKV
func (fact *TiKVFactory) NewTxnKV() kv.TxnKV {
	kv := tikv.NewTiKV(fact.tikvClient, fact.metaRootPath)
	return kv
}

// Create a new Watch KV interface using ETCD
func (fact *TiKVFactory) NewWatchKV() kv.WatchKV {
	log.Fatal("Unable to use TiKV as WatchKV")
	return nil
}

// Close the underlying client
func (fact *TiKVFactory) CloseKV() {
	fact.tikvClient.Close()
}

func FatalLogFunc(store string, err error) {
	log.Fatal("Failed to create "+store, zap.Error(err))
}

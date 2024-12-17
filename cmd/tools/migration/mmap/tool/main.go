package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/milvus-io/milvus/cmd/tools/migration/mmap"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	kv_tikv "github.com/milvus-io/milvus/internal/kv/tikv"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	kvmetestore "github.com/milvus-io/milvus/internal/metastore/kv/rootcoord"
	"github.com/milvus-io/milvus/internal/rootcoord"
	"github.com/milvus-io/milvus/internal/tso"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/kv"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/tikv"
)

func main() {
	configPtr := flag.String("config", "", "Path to the configuration file")
	flag.Parse()

	if *configPtr == "" {
		log.Error("Config file path is required")
		flag.Usage()
		os.Exit(1)
	}

	fmt.Printf("Using config file: %s\n", *configPtr)
	prepareParams(*configPtr)
	if paramtable.Get().QueryNodeCfg.MmapDirPath.GetValue() == "" {
		fmt.Println("mmap is not enabled")
		return
	}
	fmt.Printf("MmapDirPath: %s\n", paramtable.Get().QueryNodeCfg.MmapDirPath.GetValue())
	allocator := prepareTsoAllocator()
	rootCoordMeta := prepareRootCoordMeta(context.Background(), allocator)
	dataCoordCatalog := prepareDataCoordCatalog()
	m := mmap.NewMmapMigration(rootCoordMeta, allocator, dataCoordCatalog)
	m.Migrate(context.Background())
}

func prepareParams(yamlFile string) *paramtable.ComponentParam {
	paramtable.Get().Init(paramtable.NewBaseTableFromYamlOnly(yamlFile))
	return paramtable.Get()
}

func prepareTsoAllocator() tso.Allocator {
	var tsoKV kv.TxnKV
	var kvPath string
	if paramtable.Get().MetaStoreCfg.MetaStoreType.GetValue() == util.MetaStoreTypeTiKV {
		tikvCli, err := tikv.GetTiKVClient(&paramtable.Get().TiKVCfg)
		if err != nil {
			panic(err)
		}
		kvPath = paramtable.Get().TiKVCfg.KvRootPath.GetValue()
		tsoKV = tsoutil.NewTSOTiKVBase(tikvCli, kvPath, "gid")
	} else {
		etcdConfig := &paramtable.Get().EtcdCfg
		etcdCli, err := etcd.CreateEtcdClient(
			etcdConfig.UseEmbedEtcd.GetAsBool(),
			etcdConfig.EtcdEnableAuth.GetAsBool(),
			etcdConfig.EtcdAuthUserName.GetValue(),
			etcdConfig.EtcdAuthPassword.GetValue(),
			etcdConfig.EtcdUseSSL.GetAsBool(),
			etcdConfig.Endpoints.GetAsStrings(),
			etcdConfig.EtcdTLSCert.GetValue(),
			etcdConfig.EtcdTLSKey.GetValue(),
			etcdConfig.EtcdTLSCACert.GetValue(),
			etcdConfig.EtcdTLSMinVersion.GetValue())
		if err != nil {
			panic(err)
		}
		kvPath = paramtable.Get().EtcdCfg.KvRootPath.GetValue()
		tsoKV = tsoutil.NewTSOKVBase(etcdCli, kvPath, "gid")
	}
	tsoAllocator := tso.NewGlobalTSOAllocator("idTimestamp", tsoKV)
	if err := tsoAllocator.Initialize(); err != nil {
		panic(err)
	}
	return tsoAllocator
}

func metaKVCreator() (kv.MetaKv, error) {
	if paramtable.Get().MetaStoreCfg.MetaStoreType.GetValue() == util.MetaStoreTypeTiKV {
		tikvCli, err := tikv.GetTiKVClient(&paramtable.Get().TiKVCfg)
		if err != nil {
			panic(err)
		}
		return kv_tikv.NewTiKV(tikvCli, paramtable.Get().TiKVCfg.MetaRootPath.GetValue(),
			kv_tikv.WithRequestTimeout(paramtable.Get().ServiceParam.TiKVCfg.RequestTimeout.GetAsDuration(time.Millisecond))), nil
	}
	etcdConfig := &paramtable.Get().EtcdCfg
	etcdCli, err := etcd.CreateEtcdClient(
		etcdConfig.UseEmbedEtcd.GetAsBool(),
		etcdConfig.EtcdEnableAuth.GetAsBool(),
		etcdConfig.EtcdAuthUserName.GetValue(),
		etcdConfig.EtcdAuthPassword.GetValue(),
		etcdConfig.EtcdUseSSL.GetAsBool(),
		etcdConfig.Endpoints.GetAsStrings(),
		etcdConfig.EtcdTLSCert.GetValue(),
		etcdConfig.EtcdTLSKey.GetValue(),
		etcdConfig.EtcdTLSCACert.GetValue(),
		etcdConfig.EtcdTLSMinVersion.GetValue())
	if err != nil {
		panic(err)
	}
	return etcdkv.NewEtcdKV(etcdCli, paramtable.Get().EtcdCfg.MetaRootPath.GetValue(),
		etcdkv.WithRequestTimeout(paramtable.Get().ServiceParam.EtcdCfg.RequestTimeout.GetAsDuration(time.Millisecond))), nil
}

func prepareRootCoordMeta(ctx context.Context, allocator tso.Allocator) rootcoord.IMetaTable {
	var catalog metastore.RootCoordCatalog
	var err error

	switch paramtable.Get().MetaStoreCfg.MetaStoreType.GetValue() {
	case util.MetaStoreTypeEtcd:
		var metaKV kv.MetaKv
		var ss *kvmetestore.SuffixSnapshot
		var err error

		if metaKV, err = metaKVCreator(); err != nil {
			panic(err)
		}

		if ss, err = kvmetestore.NewSuffixSnapshot(metaKV, kvmetestore.SnapshotsSep, paramtable.Get().EtcdCfg.MetaRootPath.GetValue(), kvmetestore.SnapshotPrefix); err != nil {
			panic(err)
		}
		catalog = &kvmetestore.Catalog{Txn: metaKV, Snapshot: ss}
	case util.MetaStoreTypeTiKV:
		log.Ctx(ctx).Info("Using tikv as meta storage.")
		var metaKV kv.MetaKv
		var ss *kvmetestore.SuffixSnapshot
		var err error

		if metaKV, err = metaKVCreator(); err != nil {
			panic(err)
		}

		if ss, err = kvmetestore.NewSuffixSnapshot(metaKV, kvmetestore.SnapshotsSep, paramtable.Get().TiKVCfg.MetaRootPath.GetValue(), kvmetestore.SnapshotPrefix); err != nil {
			panic(err)
		}
		catalog = &kvmetestore.Catalog{Txn: metaKV, Snapshot: ss}
	default:
		panic(fmt.Sprintf("MetaStoreType %s not supported", paramtable.Get().MetaStoreCfg.MetaStoreType.GetValue()))
	}

	var meta rootcoord.IMetaTable
	if meta, err = rootcoord.NewMetaTable(ctx, catalog, allocator); err != nil {
		panic(err)
	}

	return meta
}

func prepareDataCoordCatalog() metastore.DataCoordCatalog {
	kv, err := metaKVCreator()
	if err != nil {
		panic(err)
	}
	return datacoord.NewCatalog(kv, "", "")
}

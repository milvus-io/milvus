package main

import (
	"flag"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/tikv/client-go/v2/txnkv"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-catalog/server"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	tikvkv "github.com/milvus-io/milvus/internal/kv/tikv"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func main() {
	port := flag.Int("port", 19510, "gRPC listen port")
	metaStore := flag.String("meta-store", "etcd", "metadata store type: etcd or tikv")
	etcdAddr := flag.String("etcd", "localhost:2379", "etcd endpoints (comma separated)")
	tikvPD := flag.String("tikv-pd", "localhost:2389", "TiKV PD endpoints (comma separated)")
	defaultRootPath := flag.String("default-root-path", "", "fallback root-path for clients that don't send x-root-path metadata")
	flag.Parse()

	paramtable.Init()

	// KV client factory: creates a MetaKv for a given rootPath.
	newEtcdKv := func(rootPath string) kv.MetaKv {
		client, err := clientv3.New(clientv3.Config{Endpoints: strings.Split(*etcdAddr, ",")})
		if err != nil {
			log.Fatal("failed to connect etcd", zap.Error(err))
		}
		return etcdkv.NewEtcdKV(client, rootPath)
	}
	newTikvKv := func(rootPath string) kv.MetaKv {
		client, err := txnkv.NewClient(strings.Split(*tikvPD, ","))
		if err != nil {
			log.Fatal("failed to connect TiKV PD", zap.Error(err))
		}
		return tikvkv.NewTiKV(client, rootPath, tikvkv.WithRequestTimeout(10*time.Second))
	}

	var newKv server.KvFactory
	switch *metaStore {
	case "tikv":
		newKv = newTikvKv
		log.Info("catalog-server backend: TiKV", zap.String("pd", *tikvPD))
	case "dual-write":
		newKv = func(rootPath string) kv.MetaKv {
			return server.NewDualWriteMetaKv(newEtcdKv(rootPath), newTikvKv(rootPath))
		}
		log.Info("catalog-server backend: dual-write (etcd + TiKV)",
			zap.String("etcd", *etcdAddr), zap.String("tikv-pd", *tikvPD))
	default:
		newKv = newEtcdKv
		log.Info("catalog-server backend: etcd", zap.String("endpoints", *etcdAddr))
	}

	pool := server.NewImplPool(newKv)

	cfg := server.DefaultConfig()
	cfg.MetaStoreType = *metaStore
	cfg.Server.Port = *port

	var defaultRP string
	if *defaultRootPath != "" {
		defaultRP = *defaultRootPath + "/meta"
		log.Info("default root-path configured", zap.String("defaultRootPath", defaultRP))
	}

	srv, err := server.NewCatalogGRPCServer(cfg, pool, defaultRP)
	if err != nil {
		log.Fatal("failed to create gRPC server", zap.Error(err))
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Info("received signal, shutting down", zap.String("signal", sig.String()))
		srv.Stop()
	}()

	log.Info("catalog server starting",
		zap.Int("port", *port),
		zap.String("meta-store", *metaStore))
	if err := srv.Start(); err != nil {
		log.Fatal("server failed", zap.Error(err))
	}
}

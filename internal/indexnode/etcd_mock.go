package indexnode

import (
	"fmt"
	"net/url"
	"os"
	"sync"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3client"
)

const (
	etcdListenPort = 2389
	etcdPeerPort   = 2390
)

var (
	startSvr sync.Once
	stopSvr  sync.Once
	etcdSvr  *embed.Etcd
)

func startEmbedEtcd() {
	startSvr.Do(func() {
		dir, err := os.MkdirTemp(os.TempDir(), "milvus_ut_etcd")
		if err != nil {
			panic(err)
		}

		config := embed.NewConfig()
		config.Dir = dir

		config.LogLevel = "warn"
		config.LogOutputs = []string{"default"}
		u, err := url.Parse(fmt.Sprintf("http://localhost:%d", etcdListenPort))
		if err != nil {
			panic(err)
		}
		config.ListenClientUrls = []url.URL{*u}

		u, err = url.Parse(fmt.Sprintf("http://localhost:%d", etcdPeerPort))
		if err != nil {
			panic(err)
		}
		config.ListenPeerUrls = []url.URL{*u}
		etcdSvr, err = embed.StartEtcd(config)
		if err != nil {
			panic(err)
		}
	})
}

func stopEmbedEtcd() {
	stopSvr.Do(func() {
		etcdSvr.Close()
		os.RemoveAll(etcdSvr.Config().Dir)
	})
}

func getEtcdClient() *clientv3.Client {
	return v3client.New(etcdSvr.Server)
}

package rootcoord

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/util/funcutil"

	"github.com/milvus-io/milvus/internal/util/etcd"

	"github.com/milvus-io/milvus/internal/tso"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func getTestEtcdCli() *clientv3.Client {
	Params.InitOnce()
	cli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	if err != nil {
		panic(err)
	}
	return cli
}

func cleanTestEtcdEnv(cli *clientv3.Client, rootPath string) {
	ctx := context.Background()
	if _, err := cli.Delete(ctx, rootPath, clientv3.WithPrefix()); err != nil {
		panic(err)
	}
	log.Debug("remove root path on etcd", zap.String("rootPath", rootPath))
}

func newBenchTSOAllocator(etcdCli *clientv3.Client, rootPath, subPath, key string) *tso.GlobalTSOAllocator {
	tsoKV := tsoutil.NewTSOKVBase(etcdCli, rootPath, subPath)
	tsoAllocator := tso.NewGlobalTSOAllocator(key, tsoKV)
	if err := tsoAllocator.Initialize(); err != nil {
		panic(err)
	}
	return tsoAllocator
}

func Benchmark_RootCoord_AllocTimestamp(b *testing.B) {
	rootPath := funcutil.GenRandomStr()
	subPath := funcutil.GenRandomStr()
	key := funcutil.GenRandomStr()
	log.Info("benchmark for allocating ts", zap.String("rootPath", rootPath), zap.String("subPath", subPath), zap.String("key", key))

	ctx := context.Background()
	cli := getTestEtcdCli()
	tsoAllocator := newBenchTSOAllocator(cli, rootPath, subPath, key)
	c := newTestCore(withHealthyCode(),
		withTsoAllocator(tsoAllocator))

	defer cleanTestEtcdEnv(cli, rootPath)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := rootcoordpb.AllocTimestampRequest{
			Count: 1,
		}
		_, err := c.AllocTimestamp(ctx, &req)
		assert.Nil(b, err)

	}
	b.StopTimer()
}

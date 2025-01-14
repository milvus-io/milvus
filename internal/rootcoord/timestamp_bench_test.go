// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rootcoord

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/tso"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func getTestEtcdCli() *clientv3.Client {
	paramtable.Init()
	cli, err := etcd.GetEtcdClient(
		Params.EtcdCfg.UseEmbedEtcd.GetAsBool(),
		Params.EtcdCfg.EtcdUseSSL.GetAsBool(),
		Params.EtcdCfg.Endpoints.GetAsStrings(),
		Params.EtcdCfg.EtcdTLSCert.GetValue(),
		Params.EtcdCfg.EtcdTLSKey.GetValue(),
		Params.EtcdCfg.EtcdTLSCACert.GetValue(),
		Params.EtcdCfg.EtcdTLSMinVersion.GetValue())
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

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
	"encoding/json"
	"path"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func TestProxyManager(t *testing.T) {
	Params.Init()

	etcdCli, err := etcd.GetEtcdClient(
		Params.EtcdCfg.UseEmbedEtcd.GetAsBool(),
		Params.EtcdCfg.EtcdUseSSL.GetAsBool(),
		Params.EtcdCfg.Endpoints.GetAsStrings(),
		Params.EtcdCfg.EtcdTLSCert.GetValue(),
		Params.EtcdCfg.EtcdTLSKey.GetValue(),
		Params.EtcdCfg.EtcdTLSCACert.GetValue(),
		Params.EtcdCfg.EtcdTLSMinVersion.GetValue())
	assert.NoError(t, err)
	defer etcdCli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	sessKey := path.Join(Params.EtcdCfg.MetaRootPath.GetValue(), sessionutil.DefaultServiceRoot)
	etcdCli.Delete(ctx, sessKey, clientv3.WithPrefix())
	defer etcdCli.Delete(ctx, sessKey, clientv3.WithPrefix())
	s1 := sessionutil.Session{
		ServerID: 100,
	}
	b1, err := json.Marshal(&s1)
	assert.NoError(t, err)
	k1 := path.Join(sessKey, typeutil.ProxyRole+"-100")
	_, err = etcdCli.Put(ctx, k1, string(b1))
	assert.NoError(t, err)

	s0 := sessionutil.Session{
		ServerID: 99,
	}
	b0, err := json.Marshal(&s0)
	assert.NoError(t, err)
	k0 := path.Join(sessKey, typeutil.ProxyRole+"-99")
	_, err = etcdCli.Put(ctx, k0, string(b0))
	assert.NoError(t, err)

	f1 := func(sess []*sessionutil.Session) {
		assert.Equal(t, len(sess), 2)
		assert.Equal(t, int64(100), sess[0].ServerID)
		assert.Equal(t, int64(99), sess[1].ServerID)
		t.Log("get sessions", sess[0], sess[1])
	}
	pm := newProxyManager(ctx, etcdCli, f1)
	assert.NoError(t, err)
	fa := func(sess *sessionutil.Session) {
		assert.Equal(t, int64(101), sess.ServerID)
		t.Log("add session", sess)
	}
	fd := func(sess *sessionutil.Session) {
		assert.Equal(t, int64(100), sess.ServerID)
		t.Log("del session", sess)
	}
	pm.AddSessionFunc(fa)
	pm.DelSessionFunc(fd)

	err = pm.WatchProxy()
	assert.NoError(t, err)
	t.Log("======== start watch proxy ==========")

	s2 := sessionutil.Session{
		ServerID: 101,
	}
	b2, err := json.Marshal(&s2)
	assert.NoError(t, err)
	k2 := path.Join(sessKey, typeutil.ProxyRole+"-101")
	_, err = etcdCli.Put(ctx, k2, string(b2))
	assert.NoError(t, err)

	_, err = etcdCli.Delete(ctx, k1)
	assert.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	pm.Stop()
	time.Sleep(100 * time.Millisecond)
}

func TestProxyManager_ErrCompacted(t *testing.T) {
	Params.Init()

	etcdCli, err := etcd.GetEtcdClient(
		Params.EtcdCfg.UseEmbedEtcd.GetAsBool(),
		Params.EtcdCfg.EtcdUseSSL.GetAsBool(),
		Params.EtcdCfg.Endpoints.GetAsStrings(),
		Params.EtcdCfg.EtcdTLSCert.GetValue(),
		Params.EtcdCfg.EtcdTLSKey.GetValue(),
		Params.EtcdCfg.EtcdTLSCACert.GetValue(),
		Params.EtcdCfg.EtcdTLSMinVersion.GetValue())
	assert.NoError(t, err)
	defer etcdCli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	sessKey := path.Join(Params.EtcdCfg.MetaRootPath.GetValue(), sessionutil.DefaultServiceRoot)
	f1 := func(sess []*sessionutil.Session) {
		t.Log("get sessions num", len(sess))
	}
	pm := newProxyManager(ctx, etcdCli, f1)

	eventCh := pm.etcdCli.Watch(
		pm.ctx,
		path.Join(Params.EtcdCfg.MetaRootPath.GetValue(), sessionutil.DefaultServiceRoot, typeutil.ProxyRole),
		clientv3.WithPrefix(),
		clientv3.WithCreatedNotify(),
		clientv3.WithPrevKV(),
		clientv3.WithRev(1),
	)

	for i := 1; i < 10; i++ {
		k := path.Join(sessKey, typeutil.ProxyRole+strconv.FormatInt(int64(i), 10))
		v := "invalid session: " + strconv.FormatInt(int64(i), 10)
		_, err = etcdCli.Put(ctx, k, v)
		assert.NoError(t, err)
	}

	// The reason there the error is no handle is that if you run compact twice, an error will be reported;
	// error msg is "etcdserver: mvcc: required revision has been compacted"
	etcdCli.Compact(ctx, 10)

	assert.Panics(t, func() {
		pm.startWatchEtcd(pm.ctx, eventCh)
	})

	for i := 1; i < 10; i++ {
		k := path.Join(sessKey, typeutil.ProxyRole+strconv.FormatInt(int64(i), 10))
		_, err = etcdCli.Delete(ctx, k)
		assert.NoError(t, err)
	}
}

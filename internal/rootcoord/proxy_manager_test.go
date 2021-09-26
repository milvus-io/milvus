// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package rootcoord

import (
	"context"
	"encoding/json"
	"path"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestProxyManager(t *testing.T) {
	Params.Init()
	cli, err := clientv3.New(clientv3.Config{Endpoints: Params.EtcdEndpoints})
	assert.Nil(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	sessKey := path.Join(Params.MetaRootPath, sessionutil.DefaultServiceRoot)
	cli.Delete(ctx, sessKey, clientv3.WithPrefix())
	defer cli.Delete(ctx, sessKey, clientv3.WithPrefix())
	s1 := sessionutil.Session{
		ServerID: 100,
	}
	b1, err := json.Marshal(&s1)
	assert.Nil(t, err)
	k1 := path.Join(sessKey, typeutil.ProxyRole+"-100")
	_, err = cli.Put(ctx, k1, string(b1))
	assert.Nil(t, err)

	s0 := sessionutil.Session{
		ServerID: 99,
	}
	b0, err := json.Marshal(&s0)
	assert.Nil(t, err)
	k0 := path.Join(sessKey, typeutil.ProxyRole+"-99")
	_, err = cli.Put(ctx, k0, string(b0))
	assert.Nil(t, err)

	f1 := func(sess []*sessionutil.Session) {
		assert.Equal(t, len(sess), 2)
		assert.Equal(t, int64(100), sess[0].ServerID)
		assert.Equal(t, int64(99), sess[1].ServerID)
		t.Log("get sessions", sess[0], sess[1])
	}

	pm, err := newProxyManager(ctx, Params.EtcdEndpoints, f1)
	assert.Nil(t, err)
	fa := func(sess *sessionutil.Session) {
		assert.Equal(t, int64(101), sess.ServerID)
		t.Log("add session", sess)
	}
	fd := func(sess *sessionutil.Session) {
		assert.Equal(t, int64(100), sess.ServerID)
		t.Log("del session", sess)
	}
	pm.AddSession(fa)
	pm.DelSession(fd)

	err = pm.WatchProxy()
	assert.Nil(t, err)
	t.Log("======== start watch proxy ==========")

	s2 := sessionutil.Session{
		ServerID: 101,
	}
	b2, err := json.Marshal(&s2)
	assert.Nil(t, err)
	k2 := path.Join(sessKey, typeutil.ProxyRole+"-101")
	_, err = cli.Put(ctx, k2, string(b2))
	assert.Nil(t, err)

	_, err = cli.Delete(ctx, k1)
	assert.Nil(t, err)
	time.Sleep(100 * time.Millisecond)
	pm.Stop()
	time.Sleep(100 * time.Millisecond)
}

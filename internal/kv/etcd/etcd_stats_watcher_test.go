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

package etcdkv

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-distributed/internal/util/paramtable"
	"go.etcd.io/etcd/clientv3"
)

func TestEtcdStatsWatcher(t *testing.T) {
	var p paramtable.BaseTable
	p.Init()
	addr, err := p.Load("_EtcdAddress")
	assert.Nil(t, err)
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{addr}})
	assert.Nil(t, err)
	defer cli.Close()
	w := NewEtcdStatsWatcher(cli)
	startCh := make(chan struct{})
	receiveCh := make(chan struct{})

	w.helper.eventAfterStartWatch = func() {
		var e struct{}
		startCh <- e
	}
	w.helper.eventAfterReceive = func() {
		var e struct{}
		receiveCh <- e
	}
	go w.StartBackgroundLoop(context.TODO())

	<-startCh

	_, err = cli.Put(context.TODO(), string([]byte{65}), string([]byte{65, 65, 65}))
	assert.Nil(t, err)
	<-receiveCh
	size := w.GetSize()
	assert.EqualValues(t, 4, size)

}

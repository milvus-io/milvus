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

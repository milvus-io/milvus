package proxy_node

import (
	"context"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/clientv3"
	"testing"
	"time"
)

func TestTimestampOracle(t *testing.T) {
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{"127.0.0.1:2379"}})
	assert.Nil(t, err)

	defer cli.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tso := timestampOracle{
		client:       cli,
		ctx:          ctx,
		rootPath:     "/proxy/tso",
		saveInterval: 200,
	}
	tso.Restart(0)
	time.Sleep(time.Second)
	tso.loadTimestamp()
	tso.mux.Lock()
	assert.GreaterOrEqualf(t, tso.tso.physical, uint64(100), "physical error")

	t.Log("physical = ", tso.tso.physical)
	tso.mux.Unlock()
	ts, _ := tso.GetTimestamp(1)
	t.Log("Timestamp = ", ts[0])
}

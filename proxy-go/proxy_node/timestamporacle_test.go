package proxy_node

import (
	"context"
	"go.etcd.io/etcd/clientv3"
	"testing"
	"time"
)

func TestTimestampOracle(t *testing.T) {
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{"127.0.0.1:2379"}})
	if err != nil {
		t.Fatal(err)
	}
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
	if tso.tso.physical < 100 {
		t.Fatalf("physical error")
	}
	t.Log("physical = ", tso.tso.physical)
	tso.mux.Unlock()
	ts, _ := tso.GetTimestamp(1)
	t.Log("Timestamp = ", ts[0])
}

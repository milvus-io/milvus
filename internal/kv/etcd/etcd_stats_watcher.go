package etcdkv

import (
	"context"
	"sync"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/log"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

type EtcdStatsWatcher struct {
	mu        sync.RWMutex
	client    *clientv3.Client
	helper    etcdStatsHelper
	size      int
	startTime time.Time
}

type etcdStatsHelper struct {
	eventAfterReceive    func()
	eventAfterStartWatch func()
}

func defaultHelper() etcdStatsHelper {
	return etcdStatsHelper{
		eventAfterReceive:    func() {},
		eventAfterStartWatch: func() {},
	}
}

func NewEtcdStatsWatcher(client *clientv3.Client) *EtcdStatsWatcher {
	return &EtcdStatsWatcher{
		client: client,
		helper: defaultHelper(),
	}
}

func (w *EtcdStatsWatcher) StartBackgroundLoop(ctx context.Context) {
	ch := w.client.Watch(ctx, "", clientv3.WithPrefix(), clientv3.WithCreatedNotify())
	w.mu.Lock()
	w.startTime = time.Now()
	w.mu.Unlock()
	w.helper.eventAfterStartWatch()
	for {
		select {
		case <-ctx.Done():
			log.Debug("etcd stats watcher shutdown")
			return
		case e := <-ch:
			if e.Err() != nil {
				log.Error("etcd stats watcher receive error response", zap.Error(e.Err()))
				continue
			}
			if len(e.Events) == 0 {
				continue
			}
			t := 0
			for _, event := range e.Events {
				t += len(event.Kv.Key) + len(event.Kv.Value)
			}
			w.mu.Lock()
			w.size += t
			w.mu.Unlock()
			w.helper.eventAfterReceive()
		}
	}
}

func (w *EtcdStatsWatcher) GetSize() int {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.size
}

func (w *EtcdStatsWatcher) GetStartTime() time.Time {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.startTime
}

func (w *EtcdStatsWatcher) Reset() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.size = 0
	w.startTime = time.Now()
}

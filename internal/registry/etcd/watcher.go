package etcd

import (
	"context"
	"encoding/json"
	"path"
	"sync"

	"github.com/milvus-io/milvus/internal/registry"
	milvuscommon "github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"go.etcd.io/etcd/api/v3/mvccpb"
	v3rpc "go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// Rewatch defines the behavior outer session watch handles ErrCompacted
// it should process the current full list of session
// and returns err if meta error or anything else goes wrong
type Rewatch func(sessions map[string]registry.ServiceEntry) error
type watcher[T any] struct {
	s        *etcdServiceDiscovery
	rch      clientv3.WatchChan
	eventCh  chan registry.SessionEvent[T]
	closeCh  chan struct{}
	prefix   string
	rewatch  Rewatch
	validate func(registry.ServiceEntry) bool
	convert  func(context.Context, registry.ServiceEntry) (T, error)
	stopOnce sync.Once
}

func newWatcher[T any](s *etcdServiceDiscovery, prefix string, etcdCh clientv3.WatchChan, convert func(context.Context, registry.ServiceEntry) (T, error)) *watcher[T] {
	w := &watcher[T]{
		s:       s,
		eventCh: make(chan registry.SessionEvent[T], 100),
		rch:     etcdCh,
		prefix:  prefix,
		//		rewatch:  rewatch,
		validate: func(s registry.ServiceEntry) bool { return true },
		convert:  convert,
	}
	w.start()
	return w
}

// Watch returns the internal event channel, implementing `registry.ServiceWatcher`
func (w *watcher[T]) Watch() <-chan registry.SessionEvent[T] {
	return w.eventCh
}

func (w *watcher[T]) Stop() {
	w.stopOnce.Do(func() {
		close(w.closeCh)
	})
}

func (w *watcher[T]) start() {
	go func() {
		for {
			select {
			case <-w.closeCh:
				return
			case wresp, ok := <-w.rch:
				if !ok {
					log.Warn("session watch channel closed")
					return
				}
				w.handleWatchResponse(wresp)
			}
		}
	}()
}

func (w *watcher[T]) handleWatchResponse(wresp clientv3.WatchResponse) {
	if wresp.Err() != nil {
		err := w.handleWatchErr(wresp.Err())
		if err != nil {
			log.Error("failed to handle watch session response", zap.Error(err))
			panic(err)
		}
		return
	}
	for _, ev := range wresp.Events {
		session := &etcdSession{}
		var eventType registry.SessionEventType
		switch ev.Type {
		case mvccpb.PUT:
			log.Debug("watch services",
				zap.Any("add kv", ev.Kv))
			err := json.Unmarshal(ev.Kv.Value, session)
			if err != nil {
				log.Error("watch services", zap.Error(err))
				continue
			}
			if !w.validate(session) {
				continue
			}
			if session.stopping.Load() {
				eventType = registry.SessionUpdateEvent
			} else {
				eventType = registry.SessionAddEvent
			}
		case mvccpb.DELETE:
			log.Debug("watch services",
				zap.Any("delete kv", ev.PrevKv))
			err := json.Unmarshal(ev.PrevKv.Value, session)
			if err != nil {
				log.Error("watch services", zap.Error(err))
				continue
			}
			if !w.validate(session) {
				continue
			}
			eventType = registry.SessionDelEvent
		}
		log.Debug("WatchService", zap.Any("event type", eventType))
		client, err := w.convert(context.TODO(), session)
		if err != nil {
			log.Warn("failed to convert session entry to client", zap.Error(err))
			continue
		}
		w.eventCh <- registry.SessionEvent[T]{
			EventType: eventType,
			Entry:     session,
			Client:    client,
		}
	}
}

func (w *watcher[T]) handleWatchErr(err error) error {
	ctx := context.TODO()
	// if not ErrCompacted, just close the channel
	if err != v3rpc.ErrCompacted {
		//close event channel
		log.Warn("Watch service found error", zap.Error(err))
		close(w.eventCh)
		return err
	}

	_, revision, err := w.s.getServices(ctx, w.prefix)
	if err != nil {
		log.Warn("GetSession before rewatch failed", zap.String("prefix", w.prefix), zap.Error(err))
		close(w.eventCh)
		return err
	}
	// rewatch is nil, no logic to handle
	if w.rewatch == nil {
		log.Warn("Watch service with ErrCompacted but no rewatch logic provided")
	} else {
		//TODO
		/*
			err = w.rewatch(sessions)*/
	}
	if err != nil {
		log.Warn("WatchServices rewatch failed", zap.String("prefix", w.prefix), zap.Error(err))
		close(w.eventCh)
		return err
	}

	w.rch = w.s.client.Watch(ctx, path.Join(w.s.metaRoot, milvuscommon.DefaultServiceRoot, w.prefix), clientv3.WithPrefix(), clientv3.WithPrevKV(), clientv3.WithRev(revision))
	return nil
}

package sessionutil

import (
	"context"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"go.etcd.io/etcd/api/v3/mvccpb"
	v3rpc "go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"path"
	"sync"
)

type SessionWatcher struct {
	s         *Session
	rch       clientv3.WatchChan
	eventCh   chan *SessionEvent
	prefix    string
	rewatch   Rewatch
	validate  func(*Session) bool
	closeOnce sync.Once
}

func (w *SessionWatcher) closeEventCh() {
	w.closeOnce.Do(func() {
		close(w.eventCh)
	})
}

func (w *SessionWatcher) start() {
	go func() {
		defer w.closeEventCh()
		for {
			select {
			case <-w.s.ctx.Done():
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

func (sw *SessionWatcher) handleWatchResponse(wresp clientv3.WatchResponse) {
	log := log.Ctx(context.TODO())
	if wresp.Err() != nil {
		sw.handleWatchErr(wresp.Err())
		return
	}
	for _, ev := range wresp.Events {
		session := &Session{}
		var eventType SessionEventType
		switch ev.Type {
		case mvccpb.PUT:
			log.Debug("watch services",
				zap.Any("add kv", ev.Kv))
			err := json.Unmarshal(ev.Kv.Value, session)
			if err != nil {
				log.Error("watch services", zap.Error(err))
				continue
			}
			if !sw.validate(session) {
				continue
			}
			if session.Stopping {
				eventType = SessionUpdateEvent
			} else {
				eventType = SessionAddEvent
			}
		case mvccpb.DELETE:
			log.Debug("watch services",
				zap.Any("delete kv", ev.PrevKv))
			err := json.Unmarshal(ev.PrevKv.Value, session)
			if err != nil {
				log.Error("watch services", zap.Error(err))
				continue
			}
			if !sw.validate(session) {
				log.Warn("ignore session", zap.Any("delete kv", ev.PrevKv))
				continue
			}
			eventType = SessionDelEvent
		}
		sw.eventCh <- &SessionEvent{
			EventType: eventType,
			Session:   session,
		}
	}
}

func (sw *SessionWatcher) handleWatchErr(err error) {
	// if not ErrCompacted, just close the channel
	if err != v3rpc.ErrCompacted {
		// close event channel
		log.Warn("Watch service found error", zap.Error(err))
		sw.closeEventCh()
	}

	sessions, revision, err := sw.s.GetSessions(sw.prefix)
	if err != nil {
		log.Warn("GetSession before rewatch failed", zap.String("prefix", sw.prefix), zap.Error(err))
		sw.closeEventCh()
	}

	if sw.rewatch != nil {
		err = sw.rewatch(sessions)
	}
	if err != nil {
		log.Warn("WatchServices rewatch failed", zap.String("prefix", sw.prefix), zap.Error(err))
		sw.closeEventCh()
	}

	sw.rch = sw.s.etcdCli.Watch(sw.s.ctx, path.Join(sw.s.metaRoot, DefaultServiceRoot, sw.prefix), clientv3.WithPrefix(), clientv3.WithPrevKV(), clientv3.WithRev(revision))
}

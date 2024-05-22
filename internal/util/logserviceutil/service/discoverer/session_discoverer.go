package discoverer

import (
	"context"
	"encoding/json"

	"github.com/blang/semver/v4"
	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/service/attributes"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/util"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/log"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc/resolver"
)

// sessionDiscoverer is used to apply a session watch on etcd.
type sessionDiscoverer struct {
	etcdCli      *clientv3.Client
	prefix       string
	logger       *log.MLogger
	versionRange semver.Range
	revision     int64
	peerSessions map[string]*sessionutil.Session // map[Key]session, map the key path of session to session.
}

// NewVersionedState return the empty version state.
func (sw *sessionDiscoverer) NewVersionedState() VersionedState {
	return VersionedState{
		Version: util.NewVersionInt64(),
		State:   resolver.State{},
	}
}

// Discover watches the service discovery on these goroutine.
// It may be broken down if compaction happens on etcd server.
func (sw *sessionDiscoverer) Discover(ctx context.Context, cb func(VersionedState) error) error {
	// init the discoverer.
	if err := sw.initDiscover(ctx); err != nil {
		return err
	}

	// Always send the current state first.
	// Outside logic may lost the last state before retry Discover function.
	if err := cb(sw.parseState()); err != nil {
		return err
	}
	return sw.watch(ctx, cb)
}

// watch performs the watch on etcd.
func (sw *sessionDiscoverer) watch(ctx context.Context, cb func(VersionedState) error) error {
	// start a watcher at background.
	eventCh := sw.etcdCli.Watch(
		ctx,
		sw.prefix,
		clientv3.WithPrefix(),
		clientv3.WithRev(sw.revision+1),
	)

	for {
		// Watch the etcd events.
		select {
		case <-ctx.Done():
			return errors.Wrap(ctx.Err(), "cancel the discovery")
		case event, ok := <-eventCh:
			// Break the loop if the watch is failed.
			if !ok {
				return errors.New("etcd watch channel closed unexpectedly")
			}
			if err := sw.handleETCDEvent(event); err != nil {
				return err
			}
		}
		if err := cb(sw.parseState()); err != nil {
			return err
		}
	}
}

// handleETCDEvent handles the etcd event.
func (sw *sessionDiscoverer) handleETCDEvent(resp clientv3.WatchResponse) error {
	if resp.Err() != nil {
		return resp.Err()
	}

	for _, ev := range resp.Events {
		logger := sw.logger.With(zap.String("event", ev.Type.String()),
			zap.String("sessionKey", string(ev.Kv.Key)))
		switch ev.Type {
		case clientv3.EventTypePut:
			logger = logger.With(zap.String("sessionValue", string(ev.Kv.Value)))
			session, err := sw.parseSession(ev.Kv.Value)
			if err != nil {
				logger.Warn("failed to parse session", zap.Error(err))
				continue
			}
			logger.Info("new server modification")
			sw.peerSessions[string(ev.Kv.Key)] = session
		case clientv3.EventTypeDelete:
			logger.Info("old server removed")
			delete(sw.peerSessions, string(ev.Kv.Key))
		}
	}
	// Update last revision.
	sw.revision = resp.Header.Revision
	return nil
}

// initDiscover initializes the discoverer if needed.
func (sw *sessionDiscoverer) initDiscover(ctx context.Context) error {
	if sw.revision > 0 {
		return nil
	}

	resp, err := sw.etcdCli.Get(ctx, sw.prefix, clientv3.WithPrefix(), clientv3.WithSerializable())
	if err != nil {
		return err
	}
	for _, kv := range resp.Kvs {
		logger := sw.logger.With(zap.String("sessionKey", string(kv.Key)), zap.String("sessionValue", string(kv.Value)))
		session, err := sw.parseSession(kv.Value)
		if err != nil {
			logger.Warn("fail to parse session when initializing discoverer", zap.Error(err))
			continue
		}
		sw.peerSessions[string(kv.Key)] = session
	}
	sw.revision = resp.Header.Revision
	return nil
}

// parseSession parse the session from etcd value.
func (sw *sessionDiscoverer) parseSession(value []byte) (*sessionutil.Session, error) {
	session := new(sessionutil.Session)
	if err := json.Unmarshal(value, session); err != nil {
		return nil, err
	}
	return session, nil
}

// parseState parse the state from peerSessions.
// Always perform a copy here.
func (sw *sessionDiscoverer) parseState() VersionedState {
	addrs := make([]resolver.Address, 0, len(sw.peerSessions))
	for _, session := range sw.peerSessions {
		// filter low version.
		if !sw.versionRange(session.Version) {
			sw.logger.Info("skip low version node", zap.String("version", session.Version.String()), zap.Int64("serverID", session.ServerID))
			continue
		}
		// filter stopping nodes.
		if session.Stopping {
			sw.logger.Info("skip stopping node", zap.Int64("serverID", session.ServerID))
			continue
		}
		attr := new(attributes.Attributes)
		attr = attributes.WithSession(attr, session)
		addrs = append(addrs, resolver.Address{
			Addr:               session.Address,
			BalancerAttributes: attr,
		})
	}
	return VersionedState{
		Version: util.VersionInt64(sw.revision),
		State:   resolver.State{Addresses: addrs},
	}
}

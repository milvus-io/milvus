package discoverer

import (
	"context"

	"github.com/blang/semver/v4"
	"github.com/cockroachdb/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc/resolver"

	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/attributes"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// NewSessionDiscoverer returns a new Discoverer for the milvus session registration.
func NewSessionDiscoverer(etcdCli *clientv3.Client, prefix string, exclusive bool, minimumVersion string) Discoverer {
	return &sessionDiscoverer{
		etcdCli:      etcdCli,
		prefix:       prefix,
		exclusive:    exclusive,
		versionRange: semver.MustParseRange(">=" + minimumVersion),
		logger:       log.With(zap.String("prefix", prefix), zap.Bool("exclusive", exclusive), zap.String("expectedVersion", minimumVersion)),
		revision:     0,
		peerSessions: make(map[string]*sessionutil.SessionRaw),
	}
}

// sessionDiscoverer is used to apply a session watch on etcd.
type sessionDiscoverer struct {
	etcdCli      *clientv3.Client
	prefix       string
	exclusive    bool // if exclusive, only one session is allowed, not use the prefix, only use the role directly.
	logger       *log.MLogger
	versionRange semver.Range
	revision     int64
	peerSessions map[string]*sessionutil.SessionRaw // map[Key]SessionRaw, map the key path of session to session.
}

// NewVersionedState return the empty version state.
func (sw *sessionDiscoverer) NewVersionedState() VersionedState {
	return VersionedState{
		Version: typeutil.VersionInt64(-1),
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
	opts := []clientv3.OpOption{clientv3.WithRev(sw.revision + 1)}
	if !sw.exclusive {
		opts = append(opts, clientv3.WithPrefix())
	}
	// start a watcher at background.
	eventCh := sw.etcdCli.Watch(
		ctx,
		sw.prefix,
		opts...,
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
		sw.logger.Warn("etcd watch failed with error", zap.Error(resp.Err()))
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
	opts := []clientv3.OpOption{clientv3.WithSerializable()}
	if !sw.exclusive {
		opts = append(opts, clientv3.WithPrefix())
	}
	resp, err := sw.etcdCli.Get(ctx, sw.prefix, opts...)
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
		logger.Info("new server initialization", zap.Any("session", session))
		sw.peerSessions[string(kv.Key)] = session
	}
	sw.revision = resp.Header.Revision
	return nil
}

// parseSession parse the session from etcd value.
func (sw *sessionDiscoverer) parseSession(value []byte) (*sessionutil.SessionRaw, error) {
	session := new(sessionutil.SessionRaw)
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
		session := session
		v, err := semver.Parse(session.Version)
		if err != nil {
			sw.logger.Error("failed to parse version for session", zap.Int64("serverID", session.ServerID), zap.String("version", session.Version), zap.Error(err))
			continue
		}
		// filter low version.
		// !!! important, stopping nodes should not be removed here.
		if !sw.versionRange(v) {
			sw.logger.Info("skip low version node", zap.Int64("serverID", session.ServerID), zap.String("version", session.Version))
			continue
		}

		addrs = append(addrs, resolver.Address{
			Addr: session.Address,
			// resolverAttributes is important to use when resolving, server id to make resolver.Address with same adresss different.
			Attributes: attributes.WithServerID(new(attributes.Attributes), session.ServerID),
			// balancerAttributes can be seen by picker of grpc balancer.
			BalancerAttributes: attributes.WithSession(new(attributes.Attributes), session),
		})
	}

	// TODO: service config should be sent by resolver in future to achieve dynamic configuration for grpc.
	return VersionedState{
		Version: typeutil.VersionInt64(sw.revision),
		State:   resolver.State{Addresses: addrs},
	}
}

// Sessions returns the sessions in the state.
// Should only be called when using session discoverer.
func (s *VersionedState) Sessions() map[int64]*sessionutil.SessionRaw {
	sessions := make(map[int64]*sessionutil.SessionRaw)
	for _, v := range s.State.Addresses {
		session := attributes.GetSessionFromAttributes(v.BalancerAttributes)
		if session == nil {
			log.Error("no session found in resolver state, skip it", zap.String("address", v.Addr))
			continue
		}
		sessions[session.ServerID] = session
	}
	return sessions
}

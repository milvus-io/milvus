// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package paramtable

import (
	"context"
	"encoding/json"
	"path"
	"sync"
	"time"

	"github.com/blang/semver/v4"
	"github.com/cockroachdb/errors"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// checkInterval is the polling interval of the gate-processing loop. The
// cluster version itself is event-driven (session watch); the ticker only
// applies the per-gate SwitchDelay stability window.
const checkInterval = 100 * time.Millisecond

// maxScanBackoff caps the exponential backoff of the session-scan retry in
// watchLoop, so an unreachable etcd is not polled at the check rate forever.
const maxScanBackoff = 5 * time.Second

// sessionVersion is the minimal session info used for scanning, carrying only
// the registered node version.
type sessionVersion struct {
	Version string `json:"Version"`
}

// gate tracks one version-gated config item for this process run. The
// confirmator is one-shot: every gate is registered before Start, and once all
// gates are resolved the confirmator exits. No gate can be registered at
// runtime afterwards.
type gate struct {
	key      string
	switcher *VersionGateSwitcher
	version  semver.Version // parsed GateVersion

	resolved bool
	armedAt  time.Time // when the cluster first stayed above GateVersion (zero = not armed)
	wasAbove bool      // whether the cluster was above GateVersion at the last check

	// retryAt is the earliest time the due flip may be retried after a
	// transient failure (etcd unreachable, flip CAS error). The retry backs
	// off exponentially up to SwitchDelay so a broken config center is not
	// hammered at the check interval; zero means no backoff pending.
	retryAt time.Time
	backoff time.Duration
}

// confirmator is the one-shot cluster version confirmator. It is a
// paramtable-level capability: the MixCoord role starts it via
// StartVersionGateSwitcher (see recoverConfirmator), reusing the etcd client
// paramtable created for its config etcd source, and needs no external
// wiring. A single session watch maintains the minimum online version across
// all nodes; every registered gate is then driven by that minimum version:
// once the cluster stays above the gate's GateVersion for the whole
// SwitchDelay stability window, the gate flips its config item's value to
// TargetValue in the config center (etcd source). The flip is guarded so an
// explicit operator value is never overwritten. When every registered gate is
// resolved the confirmator exits; nothing keeps running afterwards. Use close
// to stop it.
type confirmator struct {
	cli           *clientv3.Client
	sessionPrefix string // prefix of the session keys (<metaRoot>/session)
	configRoot    string // root of the config center keys (<configRoot>/config/<key>)

	mu        sync.Mutex
	gates     []*gate
	minOnline semver.Version // minimum version among all sessions (zero = unknown)
	started   bool

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// newConfirmator creates a confirmator that watches the sessions of all roles
// under metaRoot. The etcd client is injected by the caller — the same client
// paramtable uses for its config etcd source — so the confirmator never opens
// a second connection and does not own the client (close does not close it).
// Use recoverConfirmator to create, register and start a confirmator.
func newConfirmator(etcdCli *clientv3.Client, metaRoot, configRoot string) *confirmator {
	return &confirmator{
		cli:           etcdCli,
		sessionPrefix: path.Join(metaRoot, "session"),
		configRoot:    configRoot,
	}
}

// recoverConfirmator creates and starts a cluster version confirmator in one
// call: it registers a gate for every version-gated config item, then starts
// the session watch and gate loop in the background. Gates are registered
// before start (one-shot); items without a VersionGateSwitcher are skipped.
// When no gate is left pending (all items skipped) it returns a nil
// confirmator; when every registered gate is already resolved (explicit config
// or flipped by a previous run) the confirmator exits on its own after the
// initial pass. The etcd client is injected by the caller and shared with the
// config etcd source; the confirmator does not own it. Call close on the
// returned confirmator to stop it.
func recoverConfirmator(etcdCli *clientv3.Client, metaRoot, configRoot string, items []*ParamItem) (*confirmator, error) {
	vg := newConfirmator(etcdCli, metaRoot, configRoot)
	for _, item := range items {
		if item == nil || item.VersionGateSwitcher == nil {
			continue
		}
		if err := vg.registerGate(item.Key, item.VersionGateSwitcher); err != nil {
			mlog.Warn(context.TODO(), "version gate: register gate failed, skip", mlog.String("key", item.Key), mlog.Err(err))
			continue
		}
	}
	if len(vg.gates) == 0 {
		vg.close()
		return nil, nil
	}
	// Start in the background: the initial resolution reads etcd and must not
	// block paramtable initialization. Once every gate is resolved the
	// confirmator stops itself.
	go func() {
		if err := vg.start(context.TODO()); err != nil {
			mlog.Warn(context.TODO(), "version gate: start confirmator failed", mlog.Err(err))
		}
	}()
	return vg, nil
}

// registerGate registers a version-gated config item. Registration is only
// allowed before start: the confirmator is one-shot and does not support gates
// registered at runtime.
func (c *confirmator) registerGate(key string, switcher *VersionGateSwitcher) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started {
		return merr.WrapErrServiceInternal("version gate: register gate after start is not supported")
	}
	if switcher == nil {
		return merr.WrapErrServiceInternal("version gate: nil switcher")
	}
	v, err := semver.Parse(switcher.GateVersion)
	if err != nil {
		return errors.Wrapf(err, "version gate: parse gate version %s", switcher.GateVersion)
	}
	c.gates = append(c.gates, &gate{key: key, switcher: switcher, version: v})
	return nil
}

// start resolves every registered gate. Gates whose config value is no longer
// the sentinel (flipped earlier or explicitly configured by the operator) are
// resolved immediately; for the remaining gates a single session watch and one
// gate-processing loop are started in the background. start returns after the
// initial pass. When all gates are resolved the confirmator stops itself.
func (c *confirmator) start(ctx context.Context) error {
	c.mu.Lock()
	if c.started {
		c.mu.Unlock()
		return merr.WrapErrServiceInternal("version gate: already started")
	}
	c.started = true
	c.mu.Unlock()

	startCtx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	pending := 0
	for _, g := range c.gates {
		c.mu.Lock()
		if c.gateResolvedLocked(g) {
			g.resolved = true
			mlog.Info(startCtx, "version gate: gate resolved at startup (explicit config or already flipped)",
				mlog.String("key", g.key))
		}
		c.mu.Unlock()
		if !g.resolved {
			pending++
		}
	}
	if pending == 0 {
		mlog.Info(startCtx, "version gate: all gates resolved at startup, confirmator exits",
			mlog.Int("gates", len(c.gates)))
		return nil
	}
	// One shared session watch maintaining the minimum online version...
	c.wg.Add(1)
	go c.watchLoop(startCtx)
	// ...and one loop driving every gate from that minimum version.
	c.wg.Add(1)
	go c.gateLoop(startCtx)
	return nil
}

// Close stops the confirmator and cancels the background session watch and
// gate-processing loop, waiting for them to exit. The etcd client is injected
// by the caller and shared with the config etcd source, so it is not closed
// here.
func (c *confirmator) close() {
	if c.cancel != nil {
		c.cancel()
	}
	c.wg.Wait()
}

// watchLoop watches the session prefix and maintains the minimum online
// version. Every watch event triggers a full rescan of the session prefix, so
// the maintained state is always consistent with etcd even across watch
// restarts (e.g. after a compaction): the watch is only the "something changed"
// trigger, correctness comes from the rescan. On watch failure the loop
// rebuilds the watch from a fresh revision.
func (c *confirmator) watchLoop(ctx context.Context) {
	defer c.wg.Done()
	scanBackoff := checkInterval
	for {
		rev, err := c.reloadSessions(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			mlog.Warn(ctx, "version gate: session scan failed, retry", mlog.Err(err))
			if !sleepCtx(ctx, scanBackoff) {
				return
			}
			scanBackoff *= 2
			if scanBackoff > maxScanBackoff {
				scanBackoff = maxScanBackoff
			}
			continue
		}
		scanBackoff = checkInterval
		wch := c.cli.Watch(ctx, c.sessionPrefix, clientv3.WithPrefix(), clientv3.WithRev(rev))
		for resp := range wch {
			if err := resp.Err(); err != nil {
				// e.g. compacted revision: rebuild from a fresh scan.
				mlog.Warn(ctx, "version gate: session watch error, rescan", mlog.Err(err))
				break
			}
			if len(resp.Events) == 0 {
				continue
			}
			if _, err := c.reloadSessions(ctx); err != nil {
				mlog.Warn(ctx, "version gate: session rescan failed", mlog.Err(err))
			}
		}
		if ctx.Err() != nil {
			return
		}
		// The watch channel ended unexpectedly; rebuild it from the outer loop.
	}
}

// sleepCtx sleeps for d unless ctx is done first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

// reloadSessions rescans the session prefix, recomputes the minimum online
// version and returns the next watch revision (current revision + 1) so no
// event committed between the scan and the watch is missed.
func (c *confirmator) reloadSessions(ctx context.Context) (int64, error) {
	min, rev, err := c.scanSessions(ctx)
	if err != nil {
		return 0, err
	}
	c.mu.Lock()
	c.minOnline = min
	c.mu.Unlock()
	return rev, nil
}

// scanSessions returns the minimum online version and the current etcd
// revision from a fresh scan of the session prefix. Sessions with an
// unparseable version are skipped; an empty session set yields the zero
// version.
func (c *confirmator) scanSessions(ctx context.Context) (semver.Version, int64, error) {
	resp, err := c.cli.Get(ctx, c.sessionPrefix, clientv3.WithPrefix())
	if err != nil {
		return semver.Version{}, 0, err
	}
	var min semver.Version
	for _, kv := range resp.Kvs {
		session := &sessionVersion{}
		if err := json.Unmarshal(kv.Value, session); err != nil {
			continue
		}
		v, err := semver.Parse(session.Version)
		if err != nil {
			continue
		}
		if isZero(min) || v.LT(min) {
			min = v
		}
	}
	return min, resp.Header.Revision + 1, nil
}

// gateLoop periodically applies the per-gate SwitchDelay stability window and
// flips the gates whose window has elapsed. It stops the confirmator once
// every gate is resolved.
func (c *confirmator) gateLoop(ctx context.Context) {
	defer c.wg.Done()
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if c.processGates(ctx) {
				// One-shot: all gates resolved, nothing keeps running.
				mlog.Info(ctx, "version gate: all gates resolved, confirmator exits")
				c.cancel()
				return
			}
		}
	}
}

// processGates drives every unresolved gate from the current minimum online
// version: it arms the gate (starts the SwitchDelay window) when the cluster
// is above the gate's GateVersion and disarms it otherwise. Gates whose
// stability window has elapsed are flipped. It returns true when every gate is
// resolved.
func (c *confirmator) processGates(ctx context.Context) bool {
	allDone := true
	now := time.Now()
	for _, g := range c.gates {
		c.mu.Lock()
		if g.resolved {
			c.mu.Unlock()
			continue
		}
		above := !isZero(c.minOnline) && c.minOnline.GE(g.version)
		switch {
		case above && !g.wasAbove:
			g.armedAt = now
			mlog.Info(ctx, "version gate: cluster above gate version, start stability window",
				mlog.String("key", g.key), mlog.String("gateVersion", g.switcher.GateVersion),
				mlog.String("minOnline", c.minOnline.String()))
		case !above && g.wasAbove:
			g.armedAt = time.Time{}
			mlog.Warn(ctx, "version gate: cluster below gate version, reset stability window",
				mlog.String("key", g.key), mlog.String("gateVersion", g.switcher.GateVersion),
				mlog.String("minOnline", c.minOnline.String()))
		}
		g.wasAbove = above
		due := above && !g.armedAt.IsZero() && now.Sub(g.armedAt) >= g.switcher.SwitchDelay
		retryAt := g.retryAt
		c.mu.Unlock()

		if due {
			if now.Before(retryAt) {
				// A previous flip attempt failed transiently; back off instead
				// of hammering the config center at the check interval.
				allDone = false
				continue
			}
			if !c.recheckAndFlip(ctx, g) {
				allDone = false
				continue
			}
			// recheckAndFlip returned true: the gate is resolved (flipped, or
			// superseded by an explicit config value). flip logs the actual
			// outcome; mark the gate resolved and move on.
			c.mu.Lock()
			g.resolved = true
			c.mu.Unlock()
			continue
		}
		allDone = false
	}
	return allDone
}

// recheckAndFlip re-evaluates the minimum online version against a fresh etcd
// read before the irreversible flip (the session watch is event-driven and its
// events may lag behind the flip check), then performs the flip. It returns
// true when the gate is resolved (flipped, or superseded by an explicit config
// value). Transient failures (etcd scan/flip errors) grow the gate's retry
// backoff exponentially up to the SwitchDelay cap.
func (c *confirmator) recheckAndFlip(ctx context.Context, g *gate) bool {
	min, _, err := c.scanSessions(ctx)
	if err != nil {
		c.backoffForRetryLocked(g)
		mlog.Warn(ctx, "version gate: re-check sessions failed, retry later",
			mlog.String("key", g.key), mlog.Err(err))
		return false
	}
	if isZero(min) || !min.GE(g.version) {
		c.mu.Lock()
		g.armedAt = time.Time{}
		g.wasAbove = false
		g.retryAt = time.Time{}
		g.backoff = 0
		c.mu.Unlock()
		mlog.Warn(ctx, "version gate: cluster below gate version at flip time, reset stability window",
			mlog.String("key", g.key), mlog.String("gateVersion", g.switcher.GateVersion),
			mlog.String("minOnline", min.String()))
		return false
	}
	if err := c.flip(ctx, g); err != nil {
		c.backoffForRetryLocked(g)
		mlog.Warn(ctx, "version gate: flip failed, will retry",
			mlog.String("key", g.key), mlog.Err(err))
		return false
	}
	// Flip succeeded (or the gate was superseded by an explicit value):
	// clear any accumulated backoff.
	c.mu.Lock()
	g.retryAt = time.Time{}
	g.backoff = 0
	c.mu.Unlock()
	return true
}

// backoffForRetryLocked grows the gate's flip-retry backoff exponentially up
// to the SwitchDelay cap and schedules the next retry at now+backoff. Caller
// must hold c.mu.
func (c *confirmator) backoffForRetryLocked(g *gate) {
	if g.backoff == 0 {
		g.backoff = checkInterval
	} else {
		g.backoff *= 2
	}
	if g.backoff > g.switcher.SwitchDelay {
		g.backoff = g.switcher.SwitchDelay
	}
	g.retryAt = time.Now().Add(g.backoff)
}

// flip writes the gate's TargetValue into the config center once. The write is
// guarded so an explicit operator value is never overwritten: the etcd-level
// CAS only writes when the key is absent or still holds the sentinel value, so
// a concurrently written explicit value wins. The process-local effective
// value (file/env sources) is re-read right before the write: an explicit
// non-sentinel value set while the confirmator is running (e.g. the false
// escape hatch during a rolling upgrade) resolves the gate without touching
// etcd, which would otherwise mask it forever.
func (c *confirmator) flip(ctx context.Context, g *gate) error {
	// The FileSource hot-reloads and the item is refreshable, so an operator
	// may have set an explicit value (e.g. the false escape hatch) after the
	// confirmator started; that value must win over the flip.
	if v, ok := currentConfigValue(g.key); ok && v != g.switcher.EnableAutoSwitchValue {
		mlog.Info(ctx, "version gate: local config value is explicit, skip flip",
			mlog.String("key", g.key), mlog.String("value", v))
		return nil
	}
	key := c.configKey(g.key)
	resp, err := c.cli.Get(ctx, key)
	if err != nil {
		return err
	}
	txn := c.cli.Txn(ctx)
	switch {
	case len(resp.Kvs) == 0:
		txn.If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0))
	case string(resp.Kvs[0].Value) == g.switcher.EnableAutoSwitchValue:
		txn.If(clientv3.Compare(clientv3.Value(key), "=", g.switcher.EnableAutoSwitchValue))
	default:
		// An explicit etcd value is present: nothing to flip.
		mlog.Info(ctx, "version gate: explicit etcd config value wins, skip flip",
			mlog.String("key", g.key), mlog.String("value", string(resp.Kvs[0].Value)))
		return nil
	}
	txn.Then(clientv3.OpPut(key, g.switcher.TargetValue))
	txnResp, err := txn.Commit()
	if err != nil {
		return err
	}
	if !txnResp.Succeeded {
		// Another writer won the CAS: if it moved the value away from the
		// sentinel the gate is resolved; otherwise retry later.
		cur, present, err := c.configValue(g.key)
		if err != nil {
			return err
		}
		if present && cur != g.switcher.EnableAutoSwitchValue {
			mlog.Info(ctx, "version gate: concurrent writer resolved the gate, skip flip",
				mlog.String("key", g.key), mlog.String("value", cur))
			return nil
		}
		return merr.WrapErrServiceInternal("version gate: config flip CAS failed, value unchanged")
	}
	// Make the flip visible in this process immediately instead of waiting for
	// the periodic config refresher.
	refreshLocalConfig()
	mlog.Info(ctx, "version gate: gate flipped",
		mlog.String("key", g.key), mlog.String("value", g.switcher.TargetValue))
	return nil
}

// gateResolvedLocked reports whether the gate needs no flipping: the effective
// config value is no longer the sentinel (explicit operator value, or the
// value was already flipped by a previous run). Caller must hold c.mu.
func (c *confirmator) gateResolvedLocked(g *gate) bool {
	if v, ok := currentConfigValue(g.key); ok {
		return v != g.switcher.EnableAutoSwitchValue
	}
	// The process config manager does not expose the value (e.g. the key only
	// has a default): fall back to the config-center value.
	v, present, err := c.configValue(g.key)
	if err != nil {
		return false
	}
	return present && v != g.switcher.EnableAutoSwitchValue
}

// isZero reports whether v is the zero semver version.
func isZero(v semver.Version) bool {
	return v.EQ(semver.Version{})
}

// configKey returns the config-center etcd key of a config item.
func (c *confirmator) configKey(key string) string {
	return path.Join(c.configRoot, "config", config.FormatKey(key))
}

// configValue reads the config-center etcd key of a config item.
func (c *confirmator) configValue(key string) (string, bool, error) {
	resp, err := c.cli.Get(context.Background(), c.configKey(key))
	if err != nil {
		return "", false, err
	}
	if len(resp.Kvs) == 0 {
		return "", false, nil
	}
	return string(resp.Kvs[0].Value), true, nil
}

// currentConfigValue returns the effective value of the config item as
// resolved by the process config manager. It reports ok=false when the manager
// is unavailable or the key is not registered; callers must not treat that as
// "sentinel" — the etcd-level CAS in flip remains the authoritative guard
// against overwriting explicit values.
func currentConfigValue(key string) (string, bool) {
	bt := GetBaseTable()
	if bt == nil {
		return "", false
	}
	_, v, err := bt.Manager().GetConfig(key)
	if err != nil {
		return "", false
	}
	return v, true
}

// refreshLocalConfig triggers a linearizable refresh of the local etcd config
// source so a flip performed by this process becomes visible immediately. It
// is best-effort: when the etcd config source is unavailable the periodic
// refresher picks the change up later.
func refreshLocalConfig() {
	bt := GetBaseTable()
	if bt == nil {
		return
	}
	etcdSource, ok := bt.Manager().GetEtcdSource()
	if !ok {
		return
	}
	if err := etcdSource.RefreshConfigurationsLinearizable(); err != nil {
		mlog.Warn(context.TODO(), "version gate: refresh local config failed", mlog.Err(err))
	}
}

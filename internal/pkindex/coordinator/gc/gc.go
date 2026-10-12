// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package gc deletes the primary key index objects that no manifest needs:
// manifest bodies outside the retention and SSTs that no retained body
// references.
package gc

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/pkindex/coordinator/manifestmanager"
	"github.com/milvus-io/milvus/internal/pkindex/core/manifest"
	"github.com/milvus-io/milvus/internal/pkindex/pkerr"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/pkindexpb"
)

// ErrInvalidConfig is returned by New for a Config that it cannot run with.
var ErrInvalidConfig = errors.New("invalid primary key index gc config")

// MinGrace is the lowest Config.Grace that New accepts.
const MinGrace = 10 * time.Minute

// Config is the schedule and the retention of the GC. Datacoord reads each
// duration from a dataCoord.pkindex.gc.* parameter whose unit is second.
type Config struct {
	// Interval is the time between two rounds. It must be positive.
	Interval time.Duration
	// Grace is the minimum age of an SST that no retained body references, and
	// of a body whose version is above the current version, before it is
	// deleted. It must be at least MinGrace. It must be longer than the longest
	// time from an object write to its commit: from a manifest body write to
	// its pointer swap, from an SST upload to its flush commit, and from a
	// compaction output upload to its compact commit.
	Grace time.Duration
	// RetainVersions is the number of versions, up to and including the
	// current version, whose bodies are kept.
	RetainVersions int
	// RetainDuration is the minimum age of a body at or below the current
	// version and outside the RetainVersions newest versions before it is
	// deleted.
	RetainDuration time.Duration
}

// InFlightOutputs returns the SST IDs that running compactions of a vchannel
// may still commit. It must list an output from before its upload until its
// commit takes effect or its task is abandoned for good. A nil function means
// that no compaction runs.
type InFlightOutputs func(ctx context.Context, collectionID int64, vchannel string) ([]int64, error)

// GC deletes, for each vchannel that has a manifest pointer, the manifest
// bodies outside the retention and the SSTs that no retained body references.
// It reads the current version from the pointer store, never from a cache.
type GC struct {
	pointers manifestmanager.PointerStore
	store    manifest.ObjectStore
	layout   manifest.Layout
	cfg      Config
	inFlight InFlightOutputs
	now      func() time.Time

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// New returns a GC over the pointers in pointers and the objects in store, at
// the keys of layout. It returns ErrInvalidConfig when cfg.Interval is not
// positive, cfg.Grace is below MinGrace, or a retention value is negative.
func New(pointers manifestmanager.PointerStore, store manifest.ObjectStore, layout manifest.Layout,
	cfg Config, inFlight InFlightOutputs,
) (*GC, error) {
	if cfg.Interval <= 0 || cfg.Grace < MinGrace || cfg.RetainVersions < 0 || cfg.RetainDuration < 0 {
		return nil, errors.Wrapf(ErrInvalidConfig, "%+v", cfg)
	}
	return &GC{pointers: pointers, store: store, layout: layout, cfg: cfg, inFlight: inFlight, now: time.Now}, nil
}

// Start runs one round every cfg.Interval until ctx is done or Close is
// called. Call it at most once.
func (g *GC) Start(ctx context.Context) {
	ctx, g.cancel = context.WithCancel(ctx)
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		ticker := time.NewTicker(g.cfg.Interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := g.RunOnce(ctx); err != nil {
					mlog.Warn(ctx, "pkindex gc round failed", mlog.Err(err))
				}
			}
		}
	}()
}

// Close stops the rounds that Start runs and waits for the current round to
// end. It is a no-op when Start was not called.
func (g *GC) Close() {
	if g.cancel != nil {
		g.cancel()
	}
	g.wg.Wait()
}

// RunOnce runs one round over all vchannels. It returns an error only when it
// cannot list the vchannels; an error on one vchannel is logged and the round
// goes on to the next. The round stops, without an error, when ctx is done.
func (g *GC) RunOnce(ctx context.Context) error {
	keys, _, err := g.pointers.List(ctx, manifest.PointerPrefix)
	if err != nil {
		return markUnavailable(err, "list manifest pointers")
	}
	for _, key := range keys {
		if ctx.Err() != nil {
			return nil
		}
		collectionID, vchannel, ok := manifest.ParsePointerKey(key)
		if !ok {
			continue
		}
		if err := g.runVChannel(ctx, collectionID, vchannel); err != nil {
			mlog.Warn(ctx, "pkindex gc of vchannel failed",
				mlog.FieldCollectionID(collectionID), mlog.FieldVChannel(vchannel), mlog.Err(err))
		}
	}
	return nil
}

// runVChannel collects one vchannel. It reads the in-flight compaction
// outputs, then the pointer and the current body, then the other manifest
// bodies, then the SSTs. It deletes nothing when the current body key is not
// directly under the layout's manifest dir or the current sst_base_path is not
// the layout's. It deletes the bodies outside the retention, and, when every
// retained body could be read, the SSTs older than Grace that are neither
// referenced by a retained body nor an in-flight output.
func (g *GC) runVChannel(ctx context.Context, collectionID int64, vchannel string) error {
	live := map[int64]struct{}{}
	if g.inFlight != nil {
		outputs, err := g.inFlight(ctx, collectionID, vchannel)
		if err != nil {
			return markUnavailable(err, "read in-flight compaction outputs of %s", vchannel)
		}
		for _, id := range outputs {
			live[id] = struct{}{}
		}
	}
	cur, curKey, err := g.current(ctx, collectionID, vchannel)
	if err != nil || cur == nil {
		return err
	}
	dir := g.layout.ManifestDir(collectionID, vchannel)
	if _, ok := manifest.ParseManifestBodyKey(dir, curKey); !ok || cur.GetSstBasePath() != g.layout.SSTBasePath(collectionID, vchannel) {
		mlog.Warn(ctx, "pkindex gc skips vchannel whose manifest does not match the layout",
			mlog.FieldCollectionID(collectionID), mlog.FieldVChannel(vchannel),
			mlog.String("bodyKey", curKey), mlog.String("sstBasePath", cur.GetSstBasePath()))
		return nil
	}
	now := g.now()

	minKept := cur.GetVersion() - int64(g.cfg.RetainVersions) + 1
	var kept, drop []string
	err = g.store.Walk(ctx, dir+"/", func(key string, mod time.Time) bool {
		version, ok := manifest.ParseManifestBodyKey(dir, key)
		if !ok || key == curKey {
			return true
		}
		age := now.Sub(mod)
		var keep bool
		if version > cur.GetVersion() {
			keep = age <= g.cfg.Grace
		} else {
			keep = version >= minKept || age <= g.cfg.RetainDuration
		}
		if keep {
			kept = append(kept, key)
		} else {
			drop = append(drop, key)
		}
		return true
	})
	if err != nil {
		return markUnavailable(err, "walk manifest bodies of %s", vchannel)
	}
	bodies := len(drop)

	addSSTs(live, cur)
	if err := g.addRetainedSSTs(ctx, live, kept); err != nil {
		// the live set is incomplete: delete no SST of this vchannel
		mlog.Warn(ctx, "pkindex gc skips the sst sweep of vchannel",
			mlog.FieldCollectionID(collectionID), mlog.FieldVChannel(vchannel), mlog.Err(err))
	} else {
		base := cur.GetSstBasePath()
		err = g.store.Walk(ctx, base+"/", func(key string, mod time.Time) bool {
			id, ok := manifest.ParseSSTKey(base, key)
			if !ok {
				return true
			}
			if _, ok := live[id]; !ok && now.Sub(mod) > g.cfg.Grace {
				drop = append(drop, key)
			}
			return true
		})
		if err != nil {
			return markUnavailable(err, "walk ssts of %s", vchannel)
		}
	}

	if len(drop) == 0 {
		return nil
	}
	if err := g.store.MultiRemove(ctx, drop); err != nil {
		return markUnavailable(err, "remove %d objects of %s", len(drop), vchannel)
	}
	mlog.Info(ctx, "pkindex gc removed objects",
		mlog.FieldCollectionID(collectionID), mlog.FieldVChannel(vchannel),
		mlog.Int64("currentVersion", cur.GetVersion()),
		mlog.Int("bodies", bodies), mlog.Int("ssts", len(drop)-bodies))
	return nil
}

// current reads the pointer of a vchannel and the body it names. It returns
// nil when the vchannel has no pointer.
func (g *GC) current(ctx context.Context, collectionID int64, vchannel string) (*pkindexpb.Manifest, string, error) {
	key := manifest.PointerKey(collectionID, vchannel)
	value, found, err := g.pointers.Load(ctx, key)
	if err != nil {
		return nil, "", markUnavailable(err, "load manifest pointer %s", key)
	}
	if !found {
		return nil, "", nil
	}
	var p pkindexpb.ManifestPointer
	if err := proto.Unmarshal([]byte(value), &p); err != nil {
		return nil, "", errors.Mark(errors.Wrapf(err, "decode manifest pointer %s", key), pkerr.ErrCorrupted)
	}
	m, err := g.readBody(ctx, p.GetBodyKey())
	if err != nil {
		return nil, "", err
	}
	if m.GetVersion() != p.GetVersion() {
		return nil, "", errors.Mark(errors.Newf("manifest body %s has version %d, pointer has %d",
			p.GetBodyKey(), m.GetVersion(), p.GetVersion()), pkerr.ErrCorrupted)
	}
	return m, p.GetBodyKey(), nil
}

// addRetainedSSTs adds to live the SSTs that the bodies at keys reference.
func (g *GC) addRetainedSSTs(ctx context.Context, live map[int64]struct{}, keys []string) error {
	for _, key := range keys {
		m, err := g.readBody(ctx, key)
		if err != nil {
			return err
		}
		addSSTs(live, m)
	}
	return nil
}

func (g *GC) readBody(ctx context.Context, key string) (*pkindexpb.Manifest, error) {
	b, err := g.store.Read(ctx, key)
	if err != nil {
		return nil, markUnavailable(err, "read manifest body %s", key)
	}
	var m pkindexpb.Manifest
	if err := proto.Unmarshal(b, &m); err != nil {
		return nil, errors.Mark(errors.Wrapf(err, "decode manifest body %s", key), pkerr.ErrCorrupted)
	}
	return &m, nil
}

func addSSTs(live map[int64]struct{}, m *pkindexpb.Manifest) {
	for _, s := range m.GetSsts() {
		live[s.GetId()] = struct{}{}
	}
}

// markUnavailable adds context to a failure of a remote store and marks it as
// pkerr.ErrUnavailable. A mark that err already has, such as pkerr.ErrCorrupted
// for a missing object, stays.
func markUnavailable(err error, format string, args ...any) error {
	return errors.Mark(errors.Wrapf(err, format, args...), pkerr.ErrUnavailable)
}

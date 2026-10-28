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

// Package pkindex is the primary key index of a streaming node, at the
// pchannel level and above. A Manager holds a PChannelIndexManager for each
// pchannel. A PChannelIndexManager keeps the member table of its vchannels,
// takes the writes and the TimeTicks of the observe of the WAL recovery,
// drives the handovers and keeps the LastAcked bound. Each vchannel is an
// index.Index (package index), which owns the engine of the vchannel and the
// write path decider on top of it (package internal/pkindex/dedup),
// and hands its tables over to object storage through a Committer.
package pkindex

import (
	"context"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/pkindex/engine"
	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex/index"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// defaultBackoffBase is the first wait after a failed manifest lookup or handover.
const defaultBackoffBase = time.Second

// Params are the tunables of the primary key index of a streaming node.
type Params struct {
	// Params of the Index: MemTableSize and ReuploadAfter.
	index.Params
	// ActiveBytes starts a handover when the active generation holds this many bytes.
	ActiveBytes uint64
	// TailBytes starts a handover when the WAL recovery tail reaches this size.
	TailBytes uint64
	// SoftWatermark is the WAL recovery tail size at which the WAL slows appends.
	SoftWatermark uint64
	// BackoffBase is the first wait after a failure.
	BackoffBase time.Duration
	// MaxBackoff is the longest wait after a failure.
	MaxBackoff time.Duration
	// StallTimeout is how long, by observed time, the index may hold the WAL
	// checkpoint back before the driver hands over the oldest dirty Index.
	StallTimeout time.Duration
}

// ParamsFromConfig reads the params from paramtable. ReuploadAfter is half
// of the grace window of the garbage collection of the manifest service: the
// garbage collection removes an uploaded table that no manifest references
// after the grace window, so a retry that waits longer uploads again first.
func ParamsFromConfig() Params {
	cfg := &paramtable.Get().StreamingCfg
	grace := paramtable.Get().DataCoordCfg.PKIndexGCGrace.GetAsDuration(time.Second)
	return Params{
		Params: index.Params{
			MemTableSize:  uint64(cfg.PKIndexMemTableSize.GetAsSize()),
			ReuploadAfter: grace / 2,
		},
		ActiveBytes:   uint64(cfg.PKIndexHandoverActiveBytes.GetAsSize()),
		TailBytes:     uint64(cfg.PKIndexHandoverTailBytes.GetAsSize()),
		SoftWatermark: uint64(cfg.WALRecoveryTailSoftWatermark.GetAsSize()),
		BackoffBase:   defaultBackoffBase,
		MaxBackoff:    cfg.PKIndexHandoverMaxBackoff.GetAsDurationByParse(),
		StallTimeout:  paramtable.Get().DataNodeCfg.SyncPeriod.GetAsDuration(time.Second),
	}
}

// WALTailSnapshot is what the WAL recovery storage knows about its tail when
// it observes a persisted TimeTick. The observe passes it to
// ObserveTimeTick, so the index never calls the recovery storage back.
type WALTailSnapshot struct {
	// Bytes is the size of the WAL recovery tail.
	Bytes uint64
	// CompletedTimeTick is the time tick of the completed point of the ack
	// tracker: the WAL recovery checkpoint before the LastAcked of the
	// modules bounds it. The index holds the checkpoint back when its
	// LastAcked is below it.
	CompletedTimeTick uint64
}

// ManagerConfig configures a Manager.
type ManagerConfig struct {
	// Root is the local root directory of the primary key indexes.
	Root string
	// Shared are the node-level engine caches. The caller owns them.
	Shared *engine.SharedResources
	// AllocID allocates the IDs of new tables.
	AllocID func(ctx context.Context) (int64, error)
	// Store moves tables between local disk and object storage.
	Store TableStore
	// StorageRootPath is the root path of the object storage
	// (ChunkManager.RootPath()). The SST base path of a vchannel is under it.
	StorageRootPath string
	// Committer reads and changes the manifests. It must not be nil.
	Committer Committer
	// Sched runs the handovers of all pchannels of the node.
	Sched nodescheduler.Scheduler
	// Params returns the current params. The primary key index calls it on every
	// use, because some params are refreshable.
	Params func() Params
}

// Manager owns the primary key indexes of one streaming node. It opens one
// PChannelIndexManager per pchannel.
//
// The Manager does not own ManagerConfig.Shared. The caller releases it after
// Close returns.
type Manager struct {
	cfg      ManagerConfig
	lifetime *typeutil.Lifetime

	mu       sync.Mutex
	channels map[string]*PChannelIndexManager // by pchannel name
}

// NewManager creates a Manager. It panics when cfg.Committer is nil: an
// index that can not commit is never recovered, so a reopen of the WAL
// would lose the writes before its checkpoint.
func NewManager(cfg ManagerConfig) *Manager {
	if cfg.Committer == nil {
		panic("the primary key index manager needs a Committer")
	}
	return &Manager{
		cfg:      cfg,
		lifetime: typeutil.NewLifetime(),
		channels: make(map[string]*PChannelIndexManager),
	}
}

// PChannelConfig is what the WAL gives the primary key index of a pchannel.
type PChannelConfig struct {
	PChannel types.PChannelInfo
	// Seed is the initial lastObserved: the time tick of the global
	// checkpoint of the pchannel, where the WAL recovery starts to observe.
	Seed uint64
	// OnFatal marks the WAL unavailable. It must not block.
	OnFatal func(error)
}

// New creates the primary key index of a pchannel and registers it, so that
// Close closes it with the node. It does not start the handover driver.
//
// The owner, the WAL recovery, creates the indexes of the existing
// collections with CreateCollection, recovers them, and then calls Start.
// It closes the PChannelIndexManager when the WAL closes.
//
// After Close, New returns a closed PChannelIndexManager, see
// PChannelIndexManager.Closed: CreateCollection on it fails.
func (m *Manager) New(cfg PChannelConfig) *PChannelIndexManager {
	c := newPChannelIndexManager(m, cfg)
	if !m.lifetime.Add(typeutil.LifetimeStateWorking) {
		c.Close()
		return c
	}
	defer m.lifetime.Done()
	m.mu.Lock()
	m.channels[cfg.PChannel.Name] = c
	m.mu.Unlock()
	return c
}

// Root returns the local root directory of the indexes of the node.
func (m *Manager) Root() string { return m.cfg.Root }

// Committer returns the Committer of the node, nil when it has none.
func (m *Manager) Committer() Committer { return m.cfg.Committer }

// Scheduler returns the node scheduler that runs the tasks of the indexes.
func (m *Manager) Scheduler() nodescheduler.Scheduler { return m.cfg.Sched }

// Params reads the params now. The config field is a function that the
// Manager calls on every use.
func (m *Manager) Params() Params { return m.cfg.Params() }

// forget removes a closed PChannelIndexManager from the Manager.
func (m *Manager) forget(c *PChannelIndexManager) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.channels[c.info.Name] == c {
		delete(m.channels, c.info.Name)
	}
}

// Close closes every open PChannelIndexManager. It does not release ManagerConfig.Shared.
func (m *Manager) Close() {
	m.lifetime.SetState(typeutil.LifetimeStateStopped)
	m.lifetime.Wait()

	m.mu.Lock()
	channels := make([]*PChannelIndexManager, 0, len(m.channels))
	for _, c := range m.channels {
		channels = append(channels, c)
	}
	m.mu.Unlock()
	for _, c := range channels {
		c.Close()
	}
}

// VChannelRef names the manifest of an index on a vchannel.
type VChannelRef struct {
	CollectionID int64
	IndexID      int64
	VChannel     string
}

// RecoveryManifest is the answer of GetRecoveryManifests for one vchannel.
type RecoveryManifest struct {
	// Found reports whether the vchannel has a manifest.
	Found bool
	// Version and BodyKey point to the manifest when Found is true. BodyKey
	// is the object key of the manifest body.
	Version int64
	BodyKey string
	// Err is the failure of this item. The other fields are not set when it
	// is not nil.
	Err error
}

// ManifestReader finds the manifests of the vchannels of a pchannel. The
// PChannelIndexManager defines the part of the manifest service that it uses.
//
// The calls return an error that matches index.ErrRejected when a retry
// cannot fix the error.
type ManifestReader interface {
	// GetRecoveryManifests returns one answer for each ref, in the order of
	// refs. An item that fails has its own Err. The caller sends at most
	// MaxRecoveryManifests refs in one call.
	GetRecoveryManifests(ctx context.Context, refs []VChannelRef) ([]RecoveryManifest, error)
	// ReadManifest reads the manifest body at bodyKey, the BodyKey of a
	// RecoveryManifest. A body that does not exist or does not decode returns
	// an error marked pkroot.ErrCorrupted.
	ReadManifest(ctx context.Context, bodyKey string) (index.ManifestView, error)
}

// MaxRecoveryManifests is the largest number of refs in one
// GetRecoveryManifests call.
const MaxRecoveryManifests = 10000

// Committer is what the primary key index of a streaming node needs from the
// manifest service: the lookups of the PChannelIndexManager and the flush
// commits of the Index.
type Committer interface {
	ManifestReader
	index.Committer
}

// TableStore moves SST files between local disk and object storage. See
// index.TableStore.
type TableStore = index.TableStore

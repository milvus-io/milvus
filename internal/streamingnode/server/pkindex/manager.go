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
// observes the TimeTick messages, claims the pchannel, drives the handovers
// and keeps the LastAcked bound. Each vchannel is an index.Index (package
// index), which owns the engine of the vchannel and the write path authority
// on top of it (package internal/pkindex/authority), and hands its tables
// over to object storage through a Committer.
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

// defaultBackoffBase is the first wait after a failed claim or handover.
const defaultBackoffBase = time.Second

// Params are the tunables of the primary key index of a streaming node.
type Params struct {
	// Params of the Index: MemTableSize, LockStripes and ReuploadAfter.
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
}

// ParamsFromConfig reads the params from paramtable. It leaves ReuploadAfter
// zero: the caller that injects the Committer sets it from the grace window
// of the garbage collection of the manifest service.
func ParamsFromConfig() Params {
	cfg := &paramtable.Get().StreamingCfg
	return Params{
		Params: index.Params{
			MemTableSize: uint64(cfg.PKIndexMemTableSize.GetAsSize()),
			LockStripes:  cfg.PKIndexLockStripes.GetAsInt(),
		},
		ActiveBytes:   uint64(cfg.PKIndexHandoverActiveBytes.GetAsSize()),
		TailBytes:     uint64(cfg.PKIndexHandoverTailBytes.GetAsSize()),
		SoftWatermark: uint64(cfg.WALRecoveryTailSoftWatermark.GetAsSize()),
		BackoffBase:   defaultBackoffBase,
		MaxBackoff:    cfg.PKIndexHandoverMaxBackoff.GetAsDurationByParse(),
	}
}

// WALTail is what the WAL recovery reports about the WAL tail of a pchannel.
type WALTail struct {
	// Bytes is the size of the WAL recovery tail.
	Bytes uint64
	// RecoveryTimeTick is the time tick of the WAL recovery checkpoint. The
	// index holds the truncation back when its LastAcked is below it.
	RecoveryTimeTick uint64
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
	// Committer claims the pchannels and reads and changes the manifests.
	// When it is nil, a PChannelIndexManager does not claim and no Index hands over. The
	// indexes still serve the writes.
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
	channels map[*PChannelIndexManager]struct{}
}

// NewManager creates a Manager.
func NewManager(cfg ManagerConfig) *Manager {
	return &Manager{
		cfg:      cfg,
		lifetime: typeutil.NewLifetime(),
		channels: make(map[*PChannelIndexManager]struct{}),
	}
}

// OpenParam is what the WAL gives the primary key index of a pchannel.
type OpenParam struct {
	PChannel types.PChannelInfo
	// Seed is the initial lastAppliedTimeTick: the time tick of the last
	// TimeTick message when the WAL opens.
	Seed uint64
	// Tail returns the WAL recovery tail, zeros when it is unknown. Zeros
	// never start a handover. Nil is allowed.
	Tail func() WALTail
	// OnFatal marks the WAL unavailable. It must not block.
	OnFatal func(error)
}

// Open opens the primary key index of a pchannel.
//
// The caller creates the indexes of the existing collections with
// CreateCollection and then calls StartClaim.
//
// After Close, Open returns a closed PChannelIndexManager: CreateCollection
// on it fails.
func (m *Manager) Open(_ context.Context, p OpenParam) *PChannelIndexManager {
	return m.open(p, true)
}

// open opens a PChannelIndexManager. Tests pass startDriver false and call
// step by hand.
func (m *Manager) open(p OpenParam, startDriver bool) *PChannelIndexManager {
	c := newPChannelIndexManager(m, p.PChannel, p.Seed, p.Tail, p.OnFatal)
	if !m.lifetime.Add(typeutil.LifetimeStateWorking) {
		c.Close()
		return c
	}
	defer m.lifetime.Done()
	m.mu.Lock()
	m.channels[c] = struct{}{}
	m.mu.Unlock()
	if startDriver {
		c.startDriver()
	}
	return c
}

// forget removes a closed PChannelIndexManager from the Manager.
func (m *Manager) forget(c *PChannelIndexManager) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.channels, c)
}

// Close closes every open PChannelIndexManager. It does not release ManagerConfig.Shared.
func (m *Manager) Close() {
	m.lifetime.SetState(typeutil.LifetimeStateStopped)
	m.lifetime.Wait()

	m.mu.Lock()
	channels := make([]*PChannelIndexManager, 0, len(m.channels))
	for c := range m.channels {
		channels = append(channels, c)
	}
	m.mu.Unlock()
	for _, c := range channels {
		c.Close()
	}
}

// VChannelRef names the manifest of a vchannel.
type VChannelRef struct {
	CollectionID int64
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

// Claimer claims a pchannel and finds the manifests of its vchannels. The
// PChannelIndexManager defines the part of the manifest service that it uses.
//
// The calls return an error that matches index.ErrFenced when a newer term
// owns the pchannel, and index.ErrRejected when a retry cannot fix the error.
type Claimer interface {
	// ClaimPChannel claims the pchannel for term. A claim with the claimed
	// term again succeeds.
	ClaimPChannel(ctx context.Context, pchannel string, term int64) error
	// GetRecoveryManifests returns one answer for each ref, in the order of
	// refs. An item that fails has its own Err. The caller sends at most
	// MaxRecoveryManifests refs in one call.
	GetRecoveryManifests(ctx context.Context, refs []VChannelRef) ([]RecoveryManifest, error)
}

// MaxRecoveryManifests is the largest number of refs in one
// GetRecoveryManifests call.
const MaxRecoveryManifests = 10000

// Committer is what the primary key index of a streaming node needs from the
// manifest service: the claim and the lookups of the PChannelIndexManager, and the flush
// commits of the Index.
type Committer interface {
	Claimer
	index.Committer
}

// TableStore moves SST files between local disk and object storage. See
// index.TableStore.
type TableStore = index.TableStore

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

package index

import (
	"context"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/pkindex/engine"
	"github.com/milvus-io/milvus/internal/pkindex/snapshot"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// defaultBackoffBase is the first wait after a failed claim or handover.
const defaultBackoffBase = time.Second

// Params are the tunables of the index layer.
type Params struct {
	// MemTableSize is the memtable size of the engine of one vchannel.
	MemTableSize uint64
	// LockStripes is the number of lock stripes of the decider of one vchannel.
	LockStripes int
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
	// ReuploadAfter is the age after which a retried handover uploads its
	// tables again, so that the garbage collection of the manifest service
	// keeps them. The caller that injects the Committer sets it from the
	// grace window of that garbage collection.
	ReuploadAfter time.Duration
}

// ParamsFromConfig reads the params from paramtable. It leaves ReuploadAfter
// zero: the caller that injects the Committer sets it.
func ParamsFromConfig() Params {
	cfg := &paramtable.Get().StreamingCfg
	return Params{
		MemTableSize:  uint64(cfg.PKIndexMemTableSize.GetAsSize()),
		LockStripes:   cfg.PKIndexLockStripes.GetAsInt(),
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
	Store snapshot.TableStore
	// StorageRootPath is the root path of the object storage
	// (ChunkManager.RootPath()). The SST base path of a vchannel is under it.
	StorageRootPath string
	// Committer claims the pchannels and reads and changes the manifests.
	// When it is nil, a Channel does not claim and no Index hands over. The
	// indexes still serve the writes.
	Committer snapshot.Committer
	// Sched runs the handovers of all pchannels of the node.
	Sched nodescheduler.Scheduler
	// Params returns the current params. The index layer calls it on every
	// use, because some params are refreshable.
	Params func() Params
}

// Manager owns the primary key indexes of one streaming node. It opens one
// Channel per pchannel.
//
// The Manager does not own ManagerConfig.Shared. The caller releases it after
// Close returns.
type Manager struct {
	cfg      ManagerConfig
	lifetime *typeutil.Lifetime

	mu       sync.Mutex
	channels map[*Channel]struct{}
}

// NewManager creates a Manager.
func NewManager(cfg ManagerConfig) *Manager {
	return &Manager{
		cfg:      cfg,
		lifetime: typeutil.NewLifetime(),
		channels: make(map[*Channel]struct{}),
	}
}

// OpenChannel opens the index layer of a pchannel.
//
// seed is the initial lastAppliedTimeTick: the time tick of the last TimeTick
// message when the WAL opens. tail returns the WAL recovery tail, zeros when
// it is unknown. Zeros never start a handover. onFatal marks the WAL
// unavailable. It must not block.
//
// The caller adds the indexes of the existing collections and then calls
// Channel.StartClaim.
//
// After Close, OpenChannel returns a closed Channel: Add on it fails.
func (m *Manager) OpenChannel(info types.PChannelInfo, seed uint64, tail func() WALTail, onFatal func(error)) *Channel {
	return m.openChannel(info, seed, tail, onFatal, true)
}

// openChannel opens a Channel. Tests pass startDriver false and call step by hand.
func (m *Manager) openChannel(info types.PChannelInfo, seed uint64, tail func() WALTail, onFatal func(error), startDriver bool) *Channel {
	c := newChannel(m, info, seed, tail, onFatal)
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

// forget removes a closed Channel from the Manager.
func (m *Manager) forget(c *Channel) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.channels, c)
}

// Close closes every open Channel. It does not release ManagerConfig.Shared.
func (m *Manager) Close() {
	m.lifetime.SetState(typeutil.LifetimeStateStopped)
	m.lifetime.Wait()

	m.mu.Lock()
	channels := make([]*Channel, 0, len(m.channels))
	for c := range m.channels {
		channels = append(channels, c)
	}
	m.mu.Unlock()
	for _, c := range channels {
		c.Close()
	}
}

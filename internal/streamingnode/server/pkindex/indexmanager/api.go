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

// Package indexmanager owns the primary key indexes of a streaming node.
package indexmanager

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/pkindex/engine"
	"github.com/milvus-io/milvus/internal/pkindex/manifest"
	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex/index"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

// Manager owns every open pchannel and closes them with the node.
type Manager interface {
	New(cfg PChannelConfig) PChannel
	Close()
}

// PChannel owns its vchannel indexes, handovers and WAL checkpoint bound.
type PChannel interface {
	RecoverIndexes(ctx context.Context, specs []IndexSpec) ([]index.Index, error)
	Start()
	Close()
	OpenIndex(ctx context.Context, spec IndexSpec) (index.Index, bool, error)
	RemoveIndex(vchannel string, indexID int64) func(context.Context) error
	IndexOf(vchannel string) (index.Index, error)
	Apply(ctx context.Context, vchannel string, w index.Write) error
	TruncateUntil(ctx context.Context, vchannel string, tt uint64) error
	ObserveTimeTick(tt uint64, tail WALTailSnapshot)
	ObserveFlush(vchannel string, tt uint64)
	ObserveFlushAll(tt uint64)
	LastObserved() uint64
	LastAcked() uint64
}

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
	// Engine holds the node-level engine caches. The caller owns them.
	Engine *engine.SharedResources
	// AllocID allocates the IDs of new tables.
	AllocID func(ctx context.Context) (int64, error)
	// Store moves tables between local disk and object storage.
	Store index.TableStore
	// StorageRootPath is the root path of the object storage
	// (ChunkManager.RootPath()). The SST base path of a vchannel is under it.
	StorageRootPath string
	// Committer changes and finds the manifests. It must not be nil.
	Committer manifest.Committer
	// Reader reads the manifest bodies that the lookups of Committer point to.
	Reader manifest.ObjectReader
	// Sched runs the handovers of all pchannels of the node.
	Sched nodescheduler.Scheduler
	// ManifestReadConcurrency bounds the parallel manifest body reads of a
	// recovery. A value at or below 0 means 64.
	ManifestReadConcurrency int
	// Params returns the current params. The primary key index calls it on every
	// use, because some params are refreshable.
	Params func() Params
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

// IndexSpec names the primary key index of a vchannel.
type IndexSpec struct {
	VChannel     string
	CollectionID int64
	IndexID      int64
	// CheckpointTimeTick is the time tick of the CreateIndex that enabled the
	// index on the vchannel.
	CheckpointTimeTick uint64
}

// ErrOtherIndexOpen reports an OpenIndex of a vchannel that has an open
// Index of another index ID.
var ErrOtherIndexOpen = errors.New("another primary key index of the vchannel is open")

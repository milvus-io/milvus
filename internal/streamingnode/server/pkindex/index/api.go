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
	"time"

	"github.com/milvus-io/milvus/internal/pkindex/dedup"
	"github.com/milvus-io/milvus/internal/pkindex/engine"
	"github.com/milvus-io/milvus/internal/pkindex/manifest"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

// TableStore moves SST files between local disk and object storage.
type TableStore interface {
	// UploadFile writes the local file at path to key.
	UploadFile(ctx context.Context, key, path string) error

	// DownloadFile writes the object at key to the local file at path. It
	// writes a temporary file in the same directory and renames it, so path
	// either does not exist or holds the whole object.
	//
	// A missing object returns an error marked pkindex.ErrCorrupted. A
	// manifest references the table, so a table that is gone is data
	// corruption and not an IO failure that a retry can clear.
	DownloadFile(ctx context.Context, key, path string) error
}

// Put is one key that a message inserts, with the segment of the row.
type Put struct {
	PK        dedup.PK
	SegmentID int64
}

// Write is the index change of one WAL message: the keys it deletes and the
// keys it puts, at the time tick the message takes effect. The recovery of
// the pchannel builds it from a message, so this package reads no message.
// An autocommit write takes effect at its own time tick, a transaction at
// the time tick of its commit, with every body in one Write: its puts can go
// to more than one segment.
type Write struct {
	// Puts are the keys the message inserts, each with its segment.
	Puts []Put
	// Deletes are the keys the message deletes. They are written before Puts,
	// so a key in both ends up present: a companion delete must not hide the
	// insert of the same message.
	Deletes  []dedup.PK
	TimeTick uint64
}

// Params are the parameters of an Index. The Channel refreshes them.
type Params struct {
	// MemTableSize is the memtable size of the engine of one vchannel.
	MemTableSize uint64
	// ReuploadAfter is the age of an upload after which a retried commit
	// uploads the tables again.
	ReuploadAfter time.Duration
}

// HandoverState is what the handover driver of the pchannel reads about an
// Index in one call: whether it is dirty, whether a prepared handover waits
// for its commit, the size of its active generation and its checkpoint.
type HandoverState struct {
	// Dirty reports writes since the last rotate, frozen generations, or a
	// pending handover.
	Dirty bool
	// Pending reports a prepared handover that waits for its commit.
	Pending bool
	// ActiveBytes is the size of the active generation of the engine.
	ActiveBytes uint64
	// LastCommittedTimeTick is the checkpoint of the last committed handover.
	LastCommittedTimeTick uint64
}

// OpenConfig is what the primary key index manager of a pchannel gives the
// Open of an Index. The callbacks are how the Index reaches the pchannel:
// it does not know the Channel.
type OpenConfig struct {
	// VChannel is the vchannel of the Index.
	VChannel string
	// CollectionID is the collection of the vchannel.
	CollectionID int64
	// IndexID is the ID of the primary key index. It names the local
	// directory and the object keys of the Index.
	IndexID int64
	// CheckpointTimeTick is the time tick of the CreateIndex that enabled the
	// index on the vchannel. Apply skips the writes before it.
	CheckpointTimeTick uint64
	// Dir is the local directory of the Index, see Dir.
	Dir string
	// SSTBasePath is the object key prefix of the tables of the Index.
	SSTBasePath string
	// Term is the term of the pchannel, sent with every flush.
	Term int64
	// Logger carries the fields of the pchannel.
	Logger *mlog.Logger

	// Engine are the node-level engine caches.
	Engine  *engine.SharedResources
	AllocID func(ctx context.Context) (int64, error)
	Store   TableStore
	// Committer changes and finds the manifests.
	Committer manifest.Committer
	Params    func() Params

	// LastObserved returns the lastObserved of the pchannel: the time tick
	// through which the observe of the WAL recovery wrote every change into
	// the engines. A handover covers it.
	LastObserved func() uint64
	// OnRefresh runs when the dirty state or LastCommittedTimeTick of the
	// Index may have changed: after a handover step and after the committed
	// set is installed. The pchannel keeps its LastAcked heap with it. The
	// first write after a rotate is reported by the return value of Apply
	// instead.
	OnRefresh func(Index)
}

// Index is the primary key index of one vchannel on a streaming node. It
// owns the engine, the write path decider on top of it, and the handover
// state of the vchannel. It reaches its pchannel only through the callbacks
// of its OpenConfig.
type Index interface {
	// VChannel returns the vchannel of the Index.
	VChannel() string
	// CollectionID returns the collection of the Index.
	CollectionID() int64
	// IndexID returns the ID of the primary key index of the Index.
	IndexID() int64
	// Logger returns the logger of the Index, with its vchannel and collection.
	Logger() *mlog.Logger

	// Ready reports whether the committed table set of the vchannel is
	// installed in the Index. Until then the writes of the vchannel wait for
	// it (see dedup.Config.Ready) and the Index does not hand over.
	Ready() bool
	// SetCommitted records the manifest of the vchannel at the open, or,
	// with a nil view, marks the Index ready at once when the vchannel has no
	// manifest. The writes up to the checkpoint of the view are in its
	// tables, so the replay skips them, and Install installs the tables. It
	// runs once, at the open, before any replay and before any write of the
	// Index.
	SetCommitted(v *manifest.View)
	// Install brings the committed table set of the recorded manifest into
	// the engine and marks the Index ready. An Index without a recorded
	// manifest only becomes ready. A failure returns and the caller retries,
	// so every step is idempotent.
	Install(ctx context.Context) error
	// Close stops the Index: it waits for a running handover, then closes
	// the engine. It keeps the local files, except after an Apply or a
	// TruncateUntil saw a corruption error: then it removes the local
	// directory, so the next open starts from the manifest.
	Close(ctx context.Context)

	// Decider returns the write path decider of the vchannel. The WAL
	// interceptor decides every write through it.
	Decider() dedup.Decider
	// Apply writes w to the engine, in time tick order, one message at a
	// time. It reports whether the Index went from clean to dirty.
	Apply(ctx context.Context, w Write) (dirtied bool, err error)
	// TruncateUntil empties the Index at tt, the time tick of a
	// TruncateCollection of its vchannel. It reports whether the Index went
	// from clean to dirty.
	TruncateUntil(ctx context.Context, tt uint64) (dirtied bool, err error)

	// Handover runs one handover step. It returns whether a commit succeeded.
	Handover(ctx context.Context) (committed bool, err error)
	// HandoverState returns the state the handover driver of the pchannel
	// reads in one call.
	HandoverState() HandoverState
}

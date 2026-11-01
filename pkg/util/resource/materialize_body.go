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

package resource

import (
	"runtime"
	"sync"
	"sync/atomic"
	"weak"

	"google.golang.org/grpc/mem"

	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

// MaterializeBody is the body of a MaterializeResults: its entries, each held
// as one or more consecutive read-only pieces (design doc 20261019 §6).
//
// The release codec receives a MaterializeResults without copying its body:
// each entry is a list of views of the buffers gRPC received the message in,
// which can split an entry anywhere, and the body holds one reference to each
// such buffer (mem.Buffer.Ref) so gRPC's Free after Unmarshal does not
// recycle it. The generated struct's Body [][]byte cannot hold an entry that
// spans buffers without a copy, so the codec attaches the body to the message
// instead (SetMaterializeBody) and leaves Body empty; TakeMaterializeBody is
// the one accessor, and it also wraps a plain Body (a message built in
// process, such as LocalWorker's).
//
// Ownership: whoever took the body releases it, once (Release is
// idempotent). A body still attached to a message that is dropped is never
// released: its buffers are garbage collected instead of returning to the
// pool, which is safe. Readers must not use an entry after Release.
// A MaterializeBody is not safe for concurrent mutation; concurrent reads of
// its entries are.
type MaterializeBody struct {
	entries  []bodyEntry
	held     []mem.Buffer // one reference each, freed by Release
	released atomic.Bool
}

type bodyEntry struct {
	pieces [][]byte
	// srcs[i] is where pieces[i] lies, for marshaling it as a view; nil when
	// every piece is a Go slice.
	srcs []pieceSrc
}

// pieceSrc is a piece's buffer and its offset in buf.ReadOnlyData(); buf is
// nil for a Go slice.
type pieceSrc struct {
	buf mem.Buffer
	off int
}

// newMaterializeBody wraps Go-owned entries; nothing is held, so Release is a
// no-op. The entries are not copied.
func newMaterializeBody(entries [][]byte) *MaterializeBody {
	b := &MaterializeBody{entries: make([]bodyEntry, len(entries))}
	for i, e := range entries {
		b.entries[i] = bodyEntry{pieces: [][]byte{e}}
	}
	return b
}

// Len is the number of entries.
func (b *MaterializeBody) Len() int {
	if b == nil {
		return 0
	}
	return len(b.entries)
}

// Entry returns entry i's pieces, in order; their concatenation is the entry.
// The caller must not modify them, nor use them after Release.
func (b *MaterializeBody) Entry(i int) [][]byte { return b.entries[i].pieces }

// EntrySize is entry i's length in bytes.
func (b *MaterializeBody) EntrySize(i int) int { return b.entries[i].size() }

func (e *bodyEntry) size() int {
	n := 0
	for _, p := range e.pieces {
		n += len(p)
	}
	return n
}

// Append moves o's entries, after b's, and o's references into b; o is left
// empty. Neither body is copied.
func (b *MaterializeBody) Append(o *MaterializeBody) {
	if o == nil {
		return
	}
	b.entries = append(b.entries, o.entries...)
	b.held = append(b.held, o.held...)
	o.entries, o.held = nil, nil
}

// Release frees the body's references, once. A nil body is a no-op.
func (b *MaterializeBody) Release() {
	if b == nil || !b.released.CompareAndSwap(false, true) {
		return
	}
	for _, buf := range b.held {
		buf.Free()
	}
	b.held, b.entries = nil, nil
}

// attachedBodies maps a message to the body attached to it. Keys are weak:
// the map does not keep a message alive, and a cleanup drops the entry of a
// message collected while its body is still attached (the body itself is then
// garbage, see MaterializeBody). Taking or releasing the body stops that
// cleanup, so a message whose body was taken costs nothing when collected.
var attachedBodies = struct {
	sync.Mutex
	m map[weak.Pointer[internalpb.MaterializeResults]]attachedBody
	n atomic.Int64 // len(m), read without the lock
}{m: make(map[weak.Pointer[internalpb.MaterializeResults]]attachedBody)}

// attachedBody is a body attached to a message, with the cleanup that drops
// the entry if the message is collected first.
type attachedBody struct {
	body    *MaterializeBody
	cleanup runtime.Cleanup
}

// SetMaterializeBody makes body msg's body: msg.Body is cleared and the
// message owns body until someone takes it. A body already attached is
// released. An empty body is released and nothing is attached.
func SetMaterializeBody(msg *internalpb.MaterializeResults, body *MaterializeBody) {
	msg.Body = nil
	ReleaseMaterializeBody(msg)
	if body.Len() == 0 {
		body.Release()
		return
	}
	wp := weak.Make(msg)
	cleanup := runtime.AddCleanup(msg, dropAttachedBody, wp)
	attachedBodies.Lock()
	attachedBodies.m[wp] = attachedBody{body: body, cleanup: cleanup}
	attachedBodies.n.Store(int64(len(attachedBodies.m)))
	attachedBodies.Unlock()
}

func dropAttachedBody(wp weak.Pointer[internalpb.MaterializeResults]) {
	attachedBodies.Lock()
	delete(attachedBodies.m, wp)
	attachedBodies.n.Store(int64(len(attachedBodies.m)))
	attachedBodies.Unlock()
}

// detachMaterializeBody removes msg's attached body from the side table and
// stops its cleanup; nil when none is attached.
func detachMaterializeBody(msg *internalpb.MaterializeResults) *MaterializeBody {
	if attachedBodies.n.Load() == 0 {
		return nil
	}
	wp := weak.Make(msg)
	attachedBodies.Lock()
	a, ok := attachedBodies.m[wp]
	if ok {
		delete(attachedBodies.m, wp)
		attachedBodies.n.Store(int64(len(attachedBodies.m)))
	}
	attachedBodies.Unlock()
	if !ok {
		return nil
	}
	a.cleanup.Stop()
	return a.body
}

// TakeMaterializeBody detaches and returns msg's body; the caller releases
// it. A message without an attached body yields a body over msg.Body that
// holds nothing (msg.Body is left as is). A nil message yields an empty body.
func TakeMaterializeBody(msg *internalpb.MaterializeResults) *MaterializeBody {
	if msg == nil {
		return &MaterializeBody{}
	}
	if b := detachMaterializeBody(msg); b != nil {
		return b
	}
	return newMaterializeBody(msg.GetBody())
}

// ReleaseMaterializeBody releases the body attached to msg, if any.
func ReleaseMaterializeBody(msg *internalpb.MaterializeResults) {
	if msg == nil {
		return
	}
	detachMaterializeBody(msg).Release()
}

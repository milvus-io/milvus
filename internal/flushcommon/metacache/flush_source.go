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

package metacache

// FlushSourceMode indicates which subsystem is responsible for supplying the
// binlog payload of a segment when the WAL flush task executes.
//
// This is process-local runtime state, in the same family as bufferRows /
// syncingRows / syncingTasks. It is NEVER persisted to etcd or DataCoord. The
// mode is decided the first time a segment receives data and remains sticky
// for the segment's lifetime inside metacache. When the segment is removed
// from metacache (via RemoveSegments after flush/drop), the mode is naturally
// garbage-collected together with the SegmentInfo.
//
// On restart, sealed/flushed segments do not need this field (no growing
// phase remains). Growing segments re-derive the value through the regular
// write-buffer source decision path during WAL replay.
//
// Consumers use it to route writebuffer sync tasks and to refine StreamingNode
// runtime flush-pressure accounting after the per-segment source is known.
type FlushSourceMode int32

const (
	// FlushSourceUnknown means the source has not yet been decided. New
	// segments default to this value until the first insert arrives.
	FlushSourceUnknown FlushSourceMode = 0
	// FlushSourceWriteBuffer means the WriteBuffer's local insert payload is
	// the source of truth for sync tasks of this segment.
	FlushSourceWriteBuffer FlushSourceMode = 1
	// FlushSourceGrowing means the data lives in an external growing source
	// and the WriteBuffer only tracks progress.
	FlushSourceGrowing FlushSourceMode = 2
)

func (m FlushSourceMode) String() string {
	switch m {
	case FlushSourceWriteBuffer:
		return "WriteBuffer"
	case FlushSourceGrowing:
		return "Growing"
	default:
		return "Unknown"
	}
}

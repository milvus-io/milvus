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

package idempotencyview

// RecoverySnapshot retains Summary's chronological chunk boundaries. Each
// section is ordered by commit TimeTick; chunks have nonoverlapping ranges.
// VChannels is the compatibility view, sharing the same immutable Records.
type RecoverySnapshot struct {
	VChannels map[string]*Snapshot
	Chunks    [][]*Snapshot
}

// Range merges only the sections of the current chunk. It needs O(V_chunk)
// cursors, without collecting or sorting all recovered records again.
func (s *RecoverySnapshot) Range(yield func(string, *Record) bool) {
	var cursors recoveryCursors
	for _, sections := range s.Chunks {
		for _, section := range sections {
			if len(section.Records) != 0 {
				cursors = append(cursors, recoveryCursor{section: section, tick: section.Records[0].SourceTimeTick})
			}
		}
		for i := len(cursors)/2 - 1; i >= 0; i-- {
			cursors.down(i)
		}
		for len(cursors) != 0 {
			cursor := &cursors[0]
			if len(cursors) == 1 {
				for _, record := range cursor.section.Records[cursor.offset:] {
					if !yield(cursor.section.VChannel, record) {
						return
					}
				}
				cursors[0] = recoveryCursor{}
				cursors = cursors[:0]
				break
			}
			if !yield(cursor.section.VChannel, cursor.section.Records[cursor.offset]) {
				return
			}
			cursor.offset++
			if cursor.offset == len(cursor.section.Records) {
				last := len(cursors) - 1
				cursors[0] = cursors[last]
				cursors[last] = recoveryCursor{}
				cursors = cursors[:last]
			} else {
				cursor.tick = cursor.section.Records[cursor.offset].SourceTimeTick
			}
			cursors.down(0)
		}
	}
}

type recoveryCursor struct {
	section *Snapshot
	offset  int
	tick    uint64 // Cache the comparison key instead of chasing section/record pointers.
}

type recoveryCursors []recoveryCursor

// Only the root changes during merging. A typed sift-down caches the cursor
// and moves each child once, avoiding per-record heap.Interface calls and swaps.
func (h recoveryCursors) down(root int) {
	cursor := h[root]
	for {
		child := 2*root + 1
		if child >= len(h) {
			break
		}
		if right := child + 1; right < len(h) && h[right].tick < h[child].tick {
			child = right
		}
		if cursor.tick <= h[child].tick {
			break
		}
		h[root] = h[child]
		root = child
	}
	h[root] = cursor
}

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

package manifest

import (
	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/pkindexpb"
)

// Flush is the edit that adds newly uploaded SSTs and moves the checkpoint
// forward. CollectionID, VChannel, SSTBasePath and PKDataType describe the
// first version when the vchannel has no manifest.
type Flush struct {
	CollectionID       int64
	VChannel           string
	SSTBasePath        string
	PKDataType         schemapb.DataType
	PChannelTerm       int64
	AddSSTs            []*pkindexpb.SSTRef // newest first
	CheckpointTimeTick uint64
}

// Compaction is the edit that replaces a contiguous run of SSTs with merged SSTs.
type Compaction struct {
	Inputs  []int64
	Outputs []*pkindexpb.SSTRef // newest first
}

// ApplyPChannelClaim applies a claim of a pchannel for term to cur, which is
// nil when the pchannel has no claim record. It returns a new record and true
// when cur is nil or term is above the recorded term, and cur and false when
// term equals the recorded term. It returns ErrFenced when term is below the
// recorded term. It does not change cur.
func ApplyPChannelClaim(cur *pkindexpb.PChannelClaim, pchannel string, term int64) (*pkindexpb.PChannelClaim, bool, error) {
	if cur != nil && term < cur.GetPchannelTerm() {
		return nil, false, errors.Wrapf(ErrFenced, "pchannel %s: claim term %d, request term %d", pchannel, cur.GetPchannelTerm(), term)
	}
	if cur != nil && term == cur.GetPchannelTerm() {
		return cur, false, nil
	}
	return &pkindexpb.PChannelClaim{PchannelTerm: term}, true, nil
}

// CheckFlushTerm checks the term of a flush on pchannel against the claim
// record cur, which is nil when the pchannel has no claim record. It returns
// ErrFenced when the flush term is below the recorded term, and ErrNotClaimed
// when cur is nil or the flush term is above the recorded term.
func CheckFlushTerm(cur *pkindexpb.PChannelClaim, pchannel string, term int64) error {
	switch {
	case cur == nil:
		return errors.Wrapf(ErrNotClaimed, "pchannel %s: no claim record, flush term %d", pchannel, term)
	case term < cur.GetPchannelTerm():
		return errors.Wrapf(ErrFenced, "pchannel %s: claim term %d, flush term %d", pchannel, cur.GetPchannelTerm(), term)
	case term > cur.GetPchannelTerm():
		return errors.Wrapf(ErrNotClaimed, "pchannel %s: claim term %d, flush term %d", pchannel, cur.GetPchannelTerm(), term)
	}
	return nil
}

// ApplyFlush applies a flush to cur, which is nil when the vchannel has no
// manifest; then the flush applies to an empty manifest of version 0 that
// takes the collection, vchannel, SST base path and primary key data type
// from the flush. It returns a new version and true when the checkpoint
// moves forward: the new SSTs go in front of the SST list, the checkpoint is
// raised, and the flush term becomes the last flush term. A flush with no
// SSTs only moves the checkpoint. When the checkpoint does not move forward
// and the flush term equals the last flush term, the flush is an applied
// retry and ApplyFlush returns cur and false; it does not look at the SSTs of
// the flush. It returns ErrSchemaMismatch when the primary key data type
// differs from cur, ErrCheckpointRegressed when the checkpoint does not move
// forward and the flush term differs from the last flush term, and
// ErrDuplicateSST when the checkpoint moves forward and an SST is already in
// cur or is listed twice in the flush. It does not check the flush term
// against the claim record. It does not change cur.
func ApplyFlush(cur *pkindexpb.Manifest, f Flush) (*pkindexpb.Manifest, bool, error) {
	base := cur
	if base == nil {
		base = &pkindexpb.Manifest{
			CollectionId: f.CollectionID,
			Vchannel:     f.VChannel,
			PkDataType:   f.PKDataType,
			SstBasePath:  f.SSTBasePath,
		}
	}
	if f.PKDataType != base.GetPkDataType() {
		return nil, false, errors.Wrapf(ErrSchemaMismatch, "vchannel %s: manifest %s, flush %s", base.GetVchannel(), base.GetPkDataType(), f.PKDataType)
	}
	if f.CheckpointTimeTick <= base.GetCheckpointTimeTick() {
		if f.PChannelTerm != base.GetLastFlushTerm() {
			return nil, false, errors.Wrapf(ErrCheckpointRegressed, "vchannel %s: manifest %d at term %d, flush %d at term %d",
				base.GetVchannel(), base.GetCheckpointTimeTick(), base.GetLastFlushTerm(), f.CheckpointTimeTick, f.PChannelTerm)
		}
		return cur, false, nil
	}
	if err := checkNewSSTs(base.GetVchannel(), idSet(base.GetSsts()), f.AddSSTs); err != nil {
		return nil, false, err
	}
	next := nextVersion(base)
	next.LastFlushTerm = f.PChannelTerm
	next.CheckpointTimeTick = f.CheckpointTimeTick
	next.Ssts = append(cloneSSTs(f.AddSSTs), next.GetSsts()...)
	return next, true, nil
}

// ApplyCompaction applies a compaction to cur. When the inputs are a non-empty
// contiguous run of the SST list of cur, in the same order, it returns a new
// version and true, in which the outputs replace the inputs at the position of
// the inputs; the checkpoint and the last flush term do not change. When the inputs are
// not empty, no input is in the SST list of cur and all outputs are in it, the
// compaction is an applied retry and ApplyCompaction returns cur and false. It
// returns ErrNotClaimed when cur is nil, ErrStaleEdit in all other cases where
// the inputs are not such a run, and ErrDuplicateSST when an output ID is
// already in the SST list of cur or is listed twice in the outputs. It does
// not change cur.
func ApplyCompaction(cur *pkindexpb.Manifest, c Compaction) (*pkindexpb.Manifest, bool, error) {
	if cur == nil {
		return nil, false, ErrNotClaimed
	}
	list := cur.GetSsts()
	if isAppliedCompaction(idSet(list), c) {
		return cur, false, nil
	}
	start := -1
	for i, s := range list {
		if len(c.Inputs) > 0 && s.GetId() == c.Inputs[0] {
			start = i
			break
		}
	}
	if len(c.Inputs) == 0 || start < 0 || start+len(c.Inputs) > len(list) {
		return nil, false, errors.Wrapf(ErrStaleEdit, "vchannel %s: inputs %v", cur.GetVchannel(), c.Inputs)
	}
	for i, id := range c.Inputs {
		if list[start+i].GetId() != id {
			return nil, false, errors.Wrapf(ErrStaleEdit, "vchannel %s: inputs %v", cur.GetVchannel(), c.Inputs)
		}
	}
	if err := checkNewSSTs(cur.GetVchannel(), idSet(list), c.Outputs); err != nil {
		return nil, false, err
	}
	next := nextVersion(cur)
	merged := make([]*pkindexpb.SSTRef, 0, len(list)-len(c.Inputs)+len(c.Outputs))
	merged = append(merged, next.GetSsts()[:start]...)
	merged = append(merged, cloneSSTs(c.Outputs)...)
	merged = append(merged, next.GetSsts()[start+len(c.Inputs):]...)
	next.Ssts = merged
	return next, true, nil
}

// isAppliedCompaction reports whether the inputs of c are not empty, no input is
// in present, and all outputs of c are in present.
func isAppliedCompaction(present map[int64]struct{}, c Compaction) bool {
	if len(c.Inputs) == 0 {
		return false
	}
	for _, id := range c.Inputs {
		if _, ok := present[id]; ok {
			return false
		}
	}
	for _, s := range c.Outputs {
		if _, ok := present[s.GetId()]; !ok {
			return false
		}
	}
	return true
}

// checkNewSSTs returns ErrDuplicateSST when an SST of add is in present or is
// listed twice in add.
func checkNewSSTs(vchannel string, present map[int64]struct{}, add []*pkindexpb.SSTRef) error {
	seen := make(map[int64]struct{}, len(add))
	for _, s := range add {
		id := s.GetId()
		if _, ok := present[id]; ok {
			return errors.Wrapf(ErrDuplicateSST, "vchannel %s: sst %d", vchannel, id)
		}
		if _, ok := seen[id]; ok {
			return errors.Wrapf(ErrDuplicateSST, "vchannel %s: sst %d is listed twice", vchannel, id)
		}
		seen[id] = struct{}{}
	}
	return nil
}

func nextVersion(cur *pkindexpb.Manifest) *pkindexpb.Manifest {
	next := proto.Clone(cur).(*pkindexpb.Manifest)
	next.Version = cur.GetVersion() + 1
	return next
}

func cloneSSTs(in []*pkindexpb.SSTRef) []*pkindexpb.SSTRef {
	out := make([]*pkindexpb.SSTRef, 0, len(in))
	for _, s := range in {
		out = append(out, proto.Clone(s).(*pkindexpb.SSTRef))
	}
	return out
}

func idSet(list []*pkindexpb.SSTRef) map[int64]struct{} {
	set := make(map[int64]struct{}, len(list))
	for _, s := range list {
		set[s.GetId()] = struct{}{}
	}
	return set
}

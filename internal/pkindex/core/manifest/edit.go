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

// Claim is the edit that records the owner of a vchannel.
type Claim struct {
	CollectionID int64
	VChannel     string
	PChannelTerm int64
	PKDataType   schemapb.DataType
	// SSTBasePath is used only when the claim creates the first version.
	SSTBasePath string
}

// Flush is the edit that adds newly uploaded SSTs and moves the checkpoint forward.
type Flush struct {
	PChannelTerm       int64
	AddSSTs            []*pkindexpb.SSTRef // newest first
	CheckpointTimeTick uint64
}

// Compact is the edit that replaces a contiguous run of SSTs with merged SSTs.
type Compact struct {
	Inputs  []int64
	Outputs []*pkindexpb.SSTRef // newest first
}

// ApplyClaim applies a claim to cur, which is nil when the vchannel has no
// manifest. It returns the first version when cur is nil, cur and false when
// the claim term equals the manifest term, and a new version with the raised
// term and true when the claim term is higher. It returns ErrFenced when the
// claim term is below the manifest term, and then ErrSchemaMismatch when the
// primary key data type differs from cur. It does not change cur.
func ApplyClaim(cur *pkindexpb.Manifest, c Claim) (*pkindexpb.Manifest, bool, error) {
	if cur == nil {
		return &pkindexpb.Manifest{
			CollectionId: c.CollectionID,
			Vchannel:     c.VChannel,
			Version:      1,
			PchannelTerm: c.PChannelTerm,
			PkDataType:   c.PKDataType,
			SstBasePath:  c.SSTBasePath,
		}, true, nil
	}
	if c.PChannelTerm < cur.GetPchannelTerm() {
		return nil, false, errors.Wrapf(ErrFenced, "vchannel %s: manifest term %d, claim term %d", cur.GetVchannel(), cur.GetPchannelTerm(), c.PChannelTerm)
	}
	if c.PKDataType != cur.GetPkDataType() {
		return nil, false, errors.Wrapf(ErrSchemaMismatch, "vchannel %s: manifest %s, claim %s", cur.GetVchannel(), cur.GetPkDataType(), c.PKDataType)
	}
	if c.PChannelTerm == cur.GetPchannelTerm() {
		return cur, false, nil
	}
	next := nextVersion(cur)
	next.PchannelTerm = c.PChannelTerm
	return next, true, nil
}

// ApplyFlush applies a flush to cur. It returns a new version and true when
// the checkpoint moves forward: the new SSTs go in front of the SST list, the
// checkpoint is raised, and a higher term is recorded. A flush with no SSTs
// only moves the checkpoint. When the checkpoint does not move forward and the
// term equals the manifest term, the flush is an applied retry and ApplyFlush
// returns cur and false; it does not look at the SSTs of the flush. It returns
// ErrNotClaimed when cur is nil, ErrFenced when the flush term is below the
// manifest term, ErrCheckpointRegressed when the checkpoint does not move
// forward and the term is above the manifest term, and ErrDuplicateSST when
// the checkpoint moves forward and an SST is already in cur or is listed twice
// in the flush. It does not change cur.
func ApplyFlush(cur *pkindexpb.Manifest, f Flush) (*pkindexpb.Manifest, bool, error) {
	if cur == nil {
		return nil, false, ErrNotClaimed
	}
	if f.PChannelTerm < cur.GetPchannelTerm() {
		return nil, false, errors.Wrapf(ErrFenced, "vchannel %s: manifest term %d, flush term %d", cur.GetVchannel(), cur.GetPchannelTerm(), f.PChannelTerm)
	}
	if f.CheckpointTimeTick <= cur.GetCheckpointTimeTick() {
		if f.PChannelTerm > cur.GetPchannelTerm() {
			return nil, false, errors.Wrapf(ErrCheckpointRegressed, "vchannel %s: manifest %d, flush %d", cur.GetVchannel(), cur.GetCheckpointTimeTick(), f.CheckpointTimeTick)
		}
		return cur, false, nil
	}
	if err := checkNewSSTs(cur.GetVchannel(), idSet(cur.GetSsts()), f.AddSSTs); err != nil {
		return nil, false, err
	}
	next := nextVersion(cur)
	next.PchannelTerm = f.PChannelTerm
	next.CheckpointTimeTick = f.CheckpointTimeTick
	next.Ssts = append(cloneSSTs(f.AddSSTs), next.GetSsts()...)
	return next, true, nil
}

// ApplyCompact applies a compaction to cur. When the inputs are a non-empty
// contiguous run of the SST list of cur, in the same order, it returns a new
// version and true, in which the outputs replace the inputs at the position of
// the inputs; the checkpoint and the term do not change. When the inputs are
// not empty, no input is in the SST list of cur and all outputs are in it, the
// compaction is an applied retry and ApplyCompact returns cur and false. It
// returns ErrNotClaimed when cur is nil, ErrStaleEdit in all other cases where
// the inputs are not such a run, and ErrDuplicateSST when an output ID is
// already in the SST list of cur or is listed twice in the outputs. It does
// not change cur.
func ApplyCompact(cur *pkindexpb.Manifest, c Compact) (*pkindexpb.Manifest, bool, error) {
	if cur == nil {
		return nil, false, ErrNotClaimed
	}
	list := cur.GetSsts()
	if isAppliedCompact(idSet(list), c) {
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

// isAppliedCompact reports whether the inputs of c are not empty, no input is
// in present, and all outputs of c are in present.
func isAppliedCompact(present map[int64]struct{}, c Compact) bool {
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

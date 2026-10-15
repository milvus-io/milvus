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

//go:build test

package pkindex

import (
	"strconv"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/pkindex/sst"
)

// FakeManifest is a manifest of one vchannel as a fake manifest service
// keeps it. Tests use it to fake a Committer.
type FakeManifest struct {
	View ManifestView
	// LastFlushTerm is the term of the last flush that changed the manifest.
	LastFlushTerm int64
	PKDataType    schemapb.DataType
}

// clone returns a copy of m that shares no slice with m.
func (m *FakeManifest) clone() *FakeManifest {
	c := *m
	c.View.Tables = append([]TableRef(nil), m.View.Tables...)
	return &c
}

// FakeClaimPChannel applies a claim of a pchannel for term to the claimed
// term cur (0 when the pchannel was never claimed), with the rules of the
// manifest service. It returns the claimed term after the claim:
//
//   - a lower term: ErrFenced,
//   - the same term: cur,
//   - a higher term or no claim: term.
func FakeClaimPChannel(pchannel string, cur, term int64) (int64, error) {
	if term < cur {
		return cur, errors.Wrapf(ErrFenced, "pchannel %s: claimed term %d, claim term %d", pchannel, cur, term)
	}
	return term, nil
}

// FakeRecoveryManifest returns the answer for the manifest cur (nil when
// the vchannel has none).
func FakeRecoveryManifest(cur *FakeManifest) RecoveryManifest {
	if cur == nil {
		return RecoveryManifest{}
	}
	return RecoveryManifest{Found: true, Version: cur.View.Version, BodyKey: "manifest/" + strconv.FormatInt(cur.View.Version, 10) + ".pb"}
}

// FakeFlush applies a flush to cur (nil when the vchannel has no manifest),
// with the rules of the manifest service. claimed is the claimed term of the
// pchannel, and basePath the SST base path of a new manifest. It returns the
// manifest after the flush and whether the flush changed it:
//
//   - a term below claimed: ErrFenced,
//   - a term above claimed: ErrRejected (not claimed),
//   - no manifest: a new one, so the flush gives version 1,
//   - another primary key type: ErrRejected,
//   - a checkpoint that does not move forward: cur when the term is the term
//     of the last flush (an applied retry), else ErrRejected,
//   - a table that is in cur or listed twice: ErrRejected,
//   - else the next version with the new tables in front.
func FakeFlush(cur *FakeManifest, claimed int64, args FlushArgs, basePath string) (*FakeManifest, bool, error) {
	if args.PChannelTerm < claimed {
		return nil, false, errors.Wrapf(ErrFenced, "vchannel %s: claimed term %d, flush term %d", args.VChannel, claimed, args.PChannelTerm)
	}
	if args.PChannelTerm > claimed {
		return nil, false, errors.Wrapf(ErrRejected, "vchannel %s: term %d is not claimed", args.VChannel, args.PChannelTerm)
	}
	if cur == nil {
		cur = &FakeManifest{View: ManifestView{SSTBasePath: basePath}, PKDataType: args.PKDataType}
	}
	if args.PKDataType != cur.PKDataType {
		return nil, false, errors.Wrapf(ErrRejected, "vchannel %s: primary key type %s, flush %s", args.VChannel, cur.PKDataType, args.PKDataType)
	}
	if args.CheckpointTimeTick <= cur.View.CheckpointTimeTick {
		if args.PChannelTerm != cur.LastFlushTerm {
			return nil, false, errors.Wrapf(ErrRejected, "vchannel %s: checkpoint regressed from %d to %d", args.VChannel, cur.View.CheckpointTimeTick, args.CheckpointTimeTick)
		}
		return cur.clone(), false, nil
	}
	seen := make(map[sst.ID]struct{}, len(cur.View.Tables)+len(args.AddTables))
	for _, t := range cur.View.Tables {
		seen[t.ID] = struct{}{}
	}
	for _, t := range args.AddTables {
		if _, ok := seen[t.ID]; ok {
			return nil, false, errors.Wrapf(ErrRejected, "vchannel %s: duplicate table %d", args.VChannel, t.ID)
		}
		seen[t.ID] = struct{}{}
	}
	next := cur.clone()
	next.View.Version++
	next.LastFlushTerm = args.PChannelTerm
	next.View.CheckpointTimeTick = args.CheckpointTimeTick
	next.View.Tables = append(append([]TableRef(nil), args.AddTables...), cur.View.Tables...)
	return next, true, nil
}

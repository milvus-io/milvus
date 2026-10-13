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

package snapshot

import (
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/pkindex/sst"
)

// FakeManifest is a manifest of one vchannel as a fake manifest service
// keeps it. Tests use it to fake a Committer.
type FakeManifest struct {
	View       ManifestView
	Term       int64
	PKDataType schemapb.DataType
}

// clone returns a copy of m that shares no slice with m.
func (m *FakeManifest) clone() *FakeManifest {
	c := *m
	c.View.Tables = append([]TableRef(nil), m.View.Tables...)
	return &c
}

// FakeClaim applies a claim to cur, with the rules of the manifest service.
// It returns the manifest after the claim and whether the claim changed it:
//
//   - no manifest: a new one at version 1 with basePath,
//   - a lower term: ErrFenced,
//   - another primary key type: ErrRejected,
//   - the same term: cur, unchanged,
//   - a higher term: the next version with the new term.
func FakeClaim(cur *FakeManifest, args ClaimArgs, basePath string) (*FakeManifest, bool, error) {
	if cur == nil {
		return &FakeManifest{
			View:       ManifestView{Version: 1, SSTBasePath: basePath},
			Term:       args.PChannelTerm,
			PKDataType: args.PKDataType,
		}, true, nil
	}
	if args.PChannelTerm < cur.Term {
		return nil, false, errors.Wrapf(ErrFenced, "vchannel %s: manifest term %d, claim term %d", args.VChannel, cur.Term, args.PChannelTerm)
	}
	if args.PKDataType != cur.PKDataType {
		return nil, false, errors.Wrapf(ErrRejected, "vchannel %s: primary key type %s, claim %s", args.VChannel, cur.PKDataType, args.PKDataType)
	}
	if args.PChannelTerm == cur.Term {
		return cur.clone(), false, nil
	}
	next := cur.clone()
	next.View.Version++
	next.Term = args.PChannelTerm
	return next, true, nil
}

// FakeFlush applies a flush to cur, with the rules of the manifest service.
// It returns the manifest after the flush and whether the flush changed it:
//
//   - no manifest: ErrRejected (not claimed),
//   - a lower term: ErrFenced,
//   - a checkpoint that does not move forward: cur for the same term (an
//     applied retry), ErrRejected for a higher term,
//   - a table that is in cur or listed twice: ErrRejected,
//   - else the next version with the new tables in front.
func FakeFlush(cur *FakeManifest, args FlushArgs) (*FakeManifest, bool, error) {
	if cur == nil {
		return nil, false, errors.Wrapf(ErrRejected, "vchannel %s is not claimed", args.VChannel)
	}
	if args.PChannelTerm < cur.Term {
		return nil, false, errors.Wrapf(ErrFenced, "vchannel %s: manifest term %d, flush term %d", args.VChannel, cur.Term, args.PChannelTerm)
	}
	if args.CheckpointTimeTick <= cur.View.CheckpointTimeTick {
		if args.PChannelTerm > cur.Term {
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
	next.Term = args.PChannelTerm
	next.View.CheckpointTimeTick = args.CheckpointTimeTick
	next.View.Tables = append(append([]TableRef(nil), args.AddTables...), cur.View.Tables...)
	return next, true, nil
}

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

package index

import (
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/pkindex/sst"
)

// FakeManifest is a manifest of one vchannel as a fake manifest service
// keeps it. Tests use it to fake a Committer.
type FakeManifest struct {
	View ManifestView
	// LastFlushTerm is the term of the last flush that changed the manifest.
	LastFlushTerm int64
	// IndexID is the index ID of the first flush.
	IndexID int64
}

// clone returns a copy of m that shares no slice with m.
func (m *FakeManifest) clone() *FakeManifest {
	c := *m
	c.View.Tables = append([]TableRef(nil), m.View.Tables...)
	return &c
}

// FakeFlush applies a flush to cur (nil when the vchannel has no manifest),
// with the rules of the manifest service. assigned is the term of the
// pchannel meta of the streaming coordinator, and basePath the SST base path of a new manifest. It returns the
// manifest after the flush and whether the flush changed it:
//
//   - a term below assigned: ErrFenced,
//   - a term above assigned: ErrRejected (not the assigned term),
//   - no manifest: a new one, so the flush gives version 1,
//   - another index ID: ErrRejected,
//   - a checkpoint that does not move forward: cur when the term is the term
//     of the last flush (an applied retry), else ErrRejected,
//   - a table that is in cur or listed twice: ErrRejected,
//   - else the next version with the new tables in front.
func FakeFlush(cur *FakeManifest, assigned int64, args FlushArgs, basePath string) (*FakeManifest, bool, error) {
	if args.PChannelTerm < assigned {
		return nil, false, errors.Wrapf(ErrFenced, "vchannel %s: assigned term %d, flush term %d", args.VChannel, assigned, args.PChannelTerm)
	}
	if args.PChannelTerm > assigned {
		return nil, false, errors.Wrapf(ErrRejected, "vchannel %s: term %d is not the assigned term", args.VChannel, args.PChannelTerm)
	}
	if cur == nil {
		cur = &FakeManifest{View: ManifestView{SSTBasePath: basePath}, IndexID: args.IndexID}
	}
	if args.IndexID != cur.IndexID {
		return nil, false, errors.Wrapf(ErrRejected, "vchannel %s: index %d, flush %d", args.VChannel, cur.IndexID, args.IndexID)
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

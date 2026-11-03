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

// Package manifest is the persisted state of a primary key index in object
// storage: where its SSTs and manifest bodies are (path.go), what a manifest
// of a vchannel records (View), and the calls of the manifest service that
// change and find it (Committer). The streaming node, the coordinator and the
// data node share it.
package manifest

import (
	"github.com/cockroachdb/pebble/sstable"

	"github.com/milvus-io/milvus/internal/pkindex/sst"
	"github.com/milvus-io/milvus/pkg/v3/proto/pkindexpb"
)

// TableRef describes a committed table in a manifest.
type TableRef = sst.Info

// View is the part of a vchannel manifest that the streaming node uses.
type View struct {
	// Version goes up by one with each change of the manifest. The first
	// flush of a vchannel creates the manifest at version 1.
	Version int64
	// SSTBasePath is the object key prefix of the tables. See SSTKey.
	SSTBasePath string
	// CheckpointTimeTick is the time tick that the tables cover.
	CheckpointTimeTick uint64
	// Tables is the complete committed table set.
	Tables []TableRef
}

// FlushArgs adds tables to the manifest of the index IndexID on a vchannel
// and moves its checkpoint to CheckpointTimeTick. The first flush of the
// index on the vchannel creates its manifest.
type FlushArgs struct {
	CollectionID       int64
	IndexID            int64
	VChannel           string
	PChannelTerm       int64
	AddTables          []TableRef
	CheckpointTimeTick uint64
	// ResetTimeTick is the time tick of a truncate of the collection that the
	// flush commits, 0 when none. When set, AddTables replace the committed
	// table set instead of going in front of it.
	ResetTimeTick uint64
}

// VChannelRef names the manifest of an index on a vchannel.
type VChannelRef struct {
	CollectionID int64
	IndexID      int64
	VChannel     string
}

// RecoveryManifest is the answer of GetRecoveryManifests for one vchannel.
type RecoveryManifest struct {
	// Found reports whether the vchannel has a manifest.
	Found bool
	// Version and BodyKey point to the manifest when Found is true. BodyKey
	// is the object key of the manifest body.
	Version int64
	BodyKey string
	// Err is the failure of this item. The other fields are not set when it
	// is not nil.
	Err error
}

// MaxRecoveryManifests is the largest number of refs in one
// GetRecoveryManifests call.
const MaxRecoveryManifests = 10000

// ViewOf returns the View of a manifest body.
func ViewOf(m *pkindexpb.Manifest) View {
	tables := make([]TableRef, len(m.GetSsts()))
	for i, ref := range m.GetSsts() {
		tables[i] = sst.Info{
			ID:          sst.ID(ref.GetId()),
			MinKey:      ref.GetMinKey(),
			MaxKey:      ref.GetMaxKey(),
			NumEntries:  ref.GetNumEntries(),
			Size:        ref.GetSize(),
			TableFormat: sstable.TableFormat(ref.GetTableFormat()),
		}
	}
	return View{
		Version:            m.GetVersion(),
		SSTBasePath:        m.GetSstBasePath(),
		CheckpointTimeTick: m.GetCheckpointTimeTick(),
		Tables:             tables,
	}
}

// SSTRefsOf returns the SST references of a manifest body for tables.
func SSTRefsOf(tables []TableRef) []*pkindexpb.SSTRef {
	refs := make([]*pkindexpb.SSTRef, len(tables))
	for i, t := range tables {
		refs[i] = &pkindexpb.SSTRef{
			Id:          int64(t.ID),
			TableFormat: uint32(t.TableFormat),
			Size:        t.Size,
			MinKey:      t.MinKey,
			MaxKey:      t.MaxKey,
			NumEntries:  t.NumEntries,
		}
	}
	return refs
}

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

package snapshot

import (
	"context"
	"strconv"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/pkindex/sst"
)

// The errors of a Committer that a retry cannot fix. Every other error of a
// Committer is retryable.
var (
	// ErrFenced reports that a newer owner claimed the pchannel with a
	// higher term. The WAL instance of the old term must stop.
	ErrFenced = errors.New("primary key index manifest is fenced by a newer term")
	// ErrRejected reports that the manifest service rejected the request
	// for good: a bug of the caller, an invalid request, or a corrupted
	// stored manifest. The same request fails the same way again.
	ErrRejected = errors.New("primary key index manifest rejected the request")
)

// TableRef describes a committed table in a manifest.
type TableRef = sst.Info

// ManifestView is the part of a vchannel manifest that the handover uses.
type ManifestView struct {
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

// VChannelRef names the manifest of a vchannel.
type VChannelRef struct {
	CollectionID int64
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

// FlushArgs adds tables to the manifest of a vchannel and moves its
// checkpoint to CheckpointTimeTick. The first flush of a vchannel creates
// its manifest with PKDataType.
type FlushArgs struct {
	CollectionID       int64
	VChannel           string
	PChannelTerm       int64
	PKDataType         schemapb.DataType
	AddTables          []TableRef
	CheckpointTimeTick uint64
}

// Committer claims a pchannel, finds the manifests of its vchannels and
// changes them.
//
// The calls return an error that matches ErrFenced when a newer term owns
// the pchannel, and ErrRejected when a retry cannot fix the error.
type Committer interface {
	// ClaimPChannel claims the pchannel for term. A claim with the claimed
	// term again succeeds.
	ClaimPChannel(ctx context.Context, pchannel string, term int64) error
	// GetRecoveryManifests returns one answer for each ref, in the order of
	// refs. An item that fails has its own Err. The caller sends at most
	// MaxRecoveryManifests refs in one call.
	GetRecoveryManifests(ctx context.Context, refs []VChannelRef) ([]RecoveryManifest, error)
	// CommitFlush applies a flush and returns the complete manifest after
	// it. A retry with the same arguments after a lost response returns the
	// manifest that the first call committed.
	CommitFlush(ctx context.Context, args FlushArgs) (ManifestView, error)
}

// MaxRecoveryManifests is the largest number of refs in one
// GetRecoveryManifests call.
const MaxRecoveryManifests = 10000

// SSTKey returns the object key of table id under basePath:
// <basePath>/<id>.sst. The manifest service uses the same format.
func SSTKey(basePath string, id sst.ID) string {
	return basePath + "/" + strconv.FormatInt(int64(id), 10) + sst.Extension
}

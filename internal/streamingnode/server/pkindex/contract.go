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

// Package pkindex is the primary key index of a streaming node. A Manager
// holds a Channel for each pchannel, and a Channel holds an Index for each
// vchannel. The Index owns the engine of its vchannel and the write path
// authority on top of it (package internal/pkindex/authority), and hands its
// tables over to object storage through a Committer.
package pkindex

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex/index"
)

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

// Claimer claims a pchannel and finds the manifests of its vchannels. The
// Channel defines the part of the manifest service that it uses.
//
// The calls return an error that matches index.ErrFenced when a newer term
// owns the pchannel, and index.ErrRejected when a retry cannot fix the error.
type Claimer interface {
	// ClaimPChannel claims the pchannel for term. A claim with the claimed
	// term again succeeds.
	ClaimPChannel(ctx context.Context, pchannel string, term int64) error
	// GetRecoveryManifests returns one answer for each ref, in the order of
	// refs. An item that fails has its own Err. The caller sends at most
	// MaxRecoveryManifests refs in one call.
	GetRecoveryManifests(ctx context.Context, refs []VChannelRef) ([]RecoveryManifest, error)
}

// MaxRecoveryManifests is the largest number of refs in one
// GetRecoveryManifests call.
const MaxRecoveryManifests = 10000

// Committer is what the primary key index of a streaming node needs from the
// manifest service: the claim and the lookups of the Channel, and the flush
// commits of the Index.
type Committer interface {
	Claimer
	index.Committer
}

// TableStore moves SST files between local disk and object storage. See
// index.TableStore.
type TableStore = index.TableStore

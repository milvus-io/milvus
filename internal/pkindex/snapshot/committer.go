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
	// ErrFenced reports that a newer owner claimed the vchannel with a
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
	// Version goes up by one with each change of the manifest. The claim
	// that creates the manifest gives version 1.
	Version int64
	// SSTBasePath is the object key prefix of the tables. See SSTKey.
	SSTBasePath string
	// CheckpointTimeTick is the time tick that the tables cover.
	CheckpointTimeTick uint64
	// Tables is the complete committed table set.
	Tables []TableRef
}

// ClaimArgs is a claim of the manifest of a vchannel for a term.
type ClaimArgs struct {
	CollectionID int64
	VChannel     string
	PChannelTerm int64
	PKDataType   schemapb.DataType
}

// FlushArgs adds tables to the manifest of a vchannel and moves its
// checkpoint to CheckpointTimeTick.
type FlushArgs struct {
	CollectionID       int64
	VChannel           string
	PChannelTerm       int64
	AddTables          []TableRef
	CheckpointTimeTick uint64
}

// Committer claims and changes the manifest of a vchannel. It returns the
// complete manifest after the call.
//
// It returns an error that matches ErrFenced when a newer term owns the
// vchannel, and ErrRejected when a retry cannot fix the error. A retry of a
// CommitFlush with the same arguments after a lost response returns the
// manifest that the first call committed.
type Committer interface {
	Claim(ctx context.Context, args ClaimArgs) (ManifestView, error)
	CommitFlush(ctx context.Context, args FlushArgs) (ManifestView, error)
}

// SSTKey returns the object key of table id under basePath:
// <basePath>/<id>.sst. The manifest service uses the same format.
func SSTKey(basePath string, id sst.ID) string {
	return basePath + "/" + strconv.FormatInt(int64(id), 10) + sst.Extension
}

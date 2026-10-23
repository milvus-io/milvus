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
package index

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	pkroot "github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/internal/pkindex/sst"
)

// The errors of a Committer that a retry cannot fix. Every other error of a
// Committer is retryable.
var (
	// ErrFenced reports that the pchannel is assigned to a higher term. The
	// WAL instance of the old term must stop.
	ErrFenced = errors.New("primary key index manifest is fenced by a newer term")
	// ErrRejected reports that the manifest service rejected the request
	// for good: a bug of the caller, an invalid request, or a corrupted
	// stored manifest. The same request fails the same way again.
	ErrRejected = errors.New("primary key index manifest rejected the request")
)

// IsFatal reports whether err ends the WAL instance: the term is fenced by a
// newer owner, the manifest service rejected a request that a retry cannot
// fix, or stored data is corrupted.
func IsFatal(err error) bool {
	return errors.Is(err, ErrFenced) || errors.Is(err, ErrRejected) || errors.Is(err, pkroot.ErrCorrupted)
}

// TableRef describes a committed table in a manifest.
type TableRef = sst.Info

// ManifestView is the part of a vchannel manifest that the handover uses.
type ManifestView struct {
	// Version goes up by one with each change of the manifest. The first
	// flush of a vchannel creates the manifest at version 1.
	Version int64
	// SSTBasePath is the object key prefix of the tables. See pkindex.SSTKey.
	SSTBasePath string
	// CheckpointTimeTick is the time tick that the tables cover.
	CheckpointTimeTick uint64
	// Tables is the complete committed table set.
	Tables []TableRef
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

// Committer changes the manifest of a vchannel. The handover defines the
// part of the manifest service that it uses.
//
// The call returns an error that matches ErrFenced when the pchannel is
// assigned to a higher term, and ErrRejected when a retry cannot fix the error.
type Committer interface {
	// CommitFlush applies a flush and returns the complete manifest after
	// it. A retry with the same arguments after a lost response returns the
	// manifest that the first call committed.
	CommitFlush(ctx context.Context, args FlushArgs) (ManifestView, error)
}

// TableStore moves SST files between local disk and object storage.
type TableStore interface {
	// UploadFile writes the local file at path to key.
	UploadFile(ctx context.Context, key, path string) error

	// DownloadFile writes the object at key to the local file at path. It
	// writes a temporary file in the same directory and renames it, so path
	// either does not exist or holds the whole object.
	//
	// A missing object returns an error marked pkindex.ErrCorrupted. A
	// manifest references the table, so a table that is gone is data
	// corruption and not an IO failure that a retry can clear.
	DownloadFile(ctx context.Context, key, path string) error
}

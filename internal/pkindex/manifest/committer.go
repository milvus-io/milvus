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
	"context"

	"github.com/cockroachdb/errors"

	pkroot "github.com/milvus-io/milvus/internal/pkindex"
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

// Committer is what the streaming node needs from the manifest service: the
// flush commits of a handover and the manifest lookups of a recovery.
//
// The calls return an error that matches ErrFenced when the pchannel is
// assigned to a higher term, and ErrRejected when a retry cannot fix the error.
type Committer interface {
	// CommitFlush applies a flush and returns the complete manifest after
	// it. A retry with the same arguments after a lost response returns the
	// manifest that the first call committed.
	CommitFlush(ctx context.Context, args FlushArgs) (View, error)

	// GetRecoveryManifests returns one answer for each ref, in the order of
	// refs. An item that fails has its own Err. The caller sends at most
	// MaxRecoveryManifests refs in one call.
	GetRecoveryManifests(ctx context.Context, refs []VChannelRef) ([]RecoveryManifest, error)
}

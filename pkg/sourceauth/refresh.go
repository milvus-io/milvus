// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sourceauth

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const refreshPreconditionKey = "milvus-source-refresh-precondition"

func sourceFingerprint(source, spec string) string {
	// This is an optimistic-concurrency check, not an authorization signature.
	// Preserve the full coordinator spec, including its previous binding.
	encoded, _ := json.Marshal([2]string{source, spec})
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}

// RefreshRPCContext replaces, never appends to, the internal precondition.
// An authenticated omitted refresh must not overwrite a newer source using a
// stale Proxy schema cache. The receiving coordinator checks under its DDL
// lock, before allocating a job or writing WAL. No caller credential is added.
func RefreshRPCContext(ctx context.Context, source, spec string, omitted bool) context.Context {
	md, _ := metadata.FromOutgoingContext(ctx)
	md = md.Copy()
	md.Delete(refreshPreconditionKey)
	if omitted && HasAuthorizer(ctx) {
		md.Set(refreshPreconditionKey, sourceFingerprint(source, spec))
	}
	return metadata.NewOutgoingContext(ctx, md)
}

// ValidateRefreshAdmission is called with current coordinator state under the
// collection DDL lock. Public clients must still pass Proxy PrepareRefresh;
// this function does not authenticate an internal RPC or binding provenance.
func ValidateRefreshAdmission(ctx context.Context, currentSource, currentSpec, source, spec string) error {
	md, _ := metadata.FromIncomingContext(ctx)
	if expected := md.Get(refreshPreconditionKey); len(expected) != 0 {
		if len(expected) != 1 || expected[0] != sourceFingerprint(currentSource, currentSpec) {
			return merr.WrapErrServiceUnavailableMsg("external source changed while authorizing refresh; retry with current collection metadata")
		}
	}
	previous, err := Read(currentSource, currentSpec)
	if err != nil {
		return err
	}
	next, err := Read(source, spec)
	if err != nil {
		return err
	}
	if previous != nil && next == nil {
		return merr.WrapErrPrivilegeNotPermitted("a bound source requires a newly authorized refresh")
	}
	return nil
}

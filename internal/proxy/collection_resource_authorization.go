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

package proxy

import (
	"context"
	"strconv"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// collectionResource is a collection named by a persisted ID (a job's or a
// segment's owner), resolved to the database and name it has now. Grants are
// keyed by name, so authorization must use these, never a caller-selected
// database or a name recorded when the job started.
type collectionResource struct {
	dbName          string
	collectionName  string
	updateTimestamp uint64
	revisionKnown   bool
}

// resolveCollectionResource returns (nil, nil) when the collection or its
// database no longer exists. A shared metadata cache can outlive drop/recreate
// or rename, so ask the coordinator. kind names the resource in errors.
func (node *Proxy) resolveCollectionResource(ctx context.Context, collectionID int64, kind string) (*collectionResource, error) {
	if collectionID <= 0 {
		// Old coordinators omit added identity fields. Never fall back to
		// authorizing a name in the caller's database during a rolling upgrade.
		return nil, merr.WrapErrServiceUnavailable(kind + " identity is unavailable")
	}
	resp, err := node.mixCoord.DescribeCollection(describeCollectionMetadataContext(ctx), &milvuspb.DescribeCollectionRequest{
		Base:         commonpbutil.NewMsgBase(commonpbutil.WithMsgType(commonpb.MsgType_DescribeCollection)),
		CollectionID: collectionID,
	})
	if err := merr.CheckRPCCall(resp, err); err != nil {
		if errors.IsAny(err, merr.ErrCollectionNotFound, merr.ErrDatabaseNotFound) {
			return nil, nil
		}
		return nil, merr.Wrap(err, "resolve "+kind)
	}
	if resp.GetCollectionID() != collectionID || resp.GetDbName() == "" || resp.GetSchema().GetName() == "" {
		return nil, merr.WrapErrServiceUnavailable(kind + " identity is unavailable")
	}
	return &collectionResource{
		dbName: resp.GetDbName(), collectionName: resp.GetSchema().GetName(),
		updateTimestamp: resp.GetUpdateTimestamp(),
		revisionKnown:   resp.GetUpdateTimestampStr() == strconv.FormatUint(resp.GetUpdateTimestamp(), 10),
	}, nil
}

// resourceSubject is the caller to authorize. With authorization disabled
// every caller bypasses the check.
func resourceSubject(ctx context.Context) (rbacSubject, error) {
	if authorizationDisabled() {
		return rbacSubject{bypass: true}, nil
	}
	return getCurrentUserRBACSubject(ctx)
}

func (r *collectionResource) permits(subject rbacSubject, privilege string) (bool, error) {
	if subject.bypass {
		return true, nil
	}
	// The name is already canonical. Resolving it as an alias again through
	// a potentially stale cache could change the resource being authorized.
	for _, role := range subject.roleNames {
		allowed, err := isRolePermitted(role, r.dbName, commonpb.ObjectType_Collection.String(), r.collectionName, privilege)
		if err != nil {
			return false, err
		}
		if allowed {
			return true, nil
		}
	}
	return false, nil
}

// permitsCluster reports whether subject holds a cluster-level privilege,
// evaluated the way PrivilegeInterceptor evaluates cluster-level annotations.
func (s rbacSubject) permitsCluster(privilege string) (bool, error) {
	if s.bypass {
		return true, nil
	}
	for _, role := range s.roleNames {
		allowed, err := isRolePermitted(role, util.AnyWord, commonpb.ObjectType_Global.String(), util.AnyWord, privilege)
		if err != nil {
			return false, err
		}
		if allowed {
			return true, nil
		}
	}
	return false, nil
}

// authorizeCollectionResource reports whether subject holds privilege on the
// collection that resource was resolved from. A false result with a nil error
// is a denial; the caller decides how to present it.
func (node *Proxy) authorizeCollectionResource(ctx context.Context, collectionID int64, resource *collectionResource, subject rbacSubject, privilege string, kind string) (bool, error) {
	allowed, err := resource.permits(subject, privilege)
	if err != nil || !allowed || subject.bypass {
		return allowed, err
	}
	if !resource.revisionKnown {
		// Current coordinators explicitly encode even a legacy stored zero as
		// "0". Preserve those unchanged collections while rejecting responses
		// that omit the revision projection entirely.
		return false, merr.WrapErrServiceUnavailable(kind + " revision is unavailable")
	}
	// Grants are keyed by name. A delayed identity lookup can overlap a drop,
	// same-name recreation and new grant. Recheck after reading the policy so
	// that the new collection's grant cannot authorize the old collection's
	// resource. Include the metadata revision: moving away and back can
	// restore the same DB/name while an intermediate collection's name-keyed
	// grant was observed.
	current, err := node.resolveCollectionResource(ctx, collectionID, kind)
	if err != nil {
		return false, err
	}
	if current == nil {
		return false, nil
	}
	if *current != *resource {
		return false, merr.WrapErrServiceUnavailable(kind + " changed during authorization")
	}
	return true, nil
}

// authorizeCollectionID resolves collectionID and reports whether subject holds
// privilege on it. A collection that no longer exists is a denial.
func (node *Proxy) authorizeCollectionID(ctx context.Context, collectionID int64, subject rbacSubject, privilege string, kind string) (*collectionResource, bool, error) {
	resource, err := node.resolveCollectionResource(ctx, collectionID, kind)
	if err != nil || resource == nil {
		return nil, false, err
	}
	allowed, err := node.authorizeCollectionResource(ctx, collectionID, resource, subject, privilege, kind)
	if err != nil || !allowed {
		return nil, false, err
	}
	return resource, true, nil
}

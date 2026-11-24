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

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// segmentInfoAuthorizer requires, for each segment, GetStatistics on the
// collection the segment actually belongs to, the privilege
// GetPersistentSegmentInfo requires for the same metadata. Segments are named
// by ID only, so the caller-supplied database and collection say nothing
// about whose segment is read.
type segmentInfoAuthorizer struct {
	node      *Proxy
	subject   rbacSubject
	decisions map[int64]bool
}

func (node *Proxy) newSegmentInfoAuthorizer(ctx context.Context) (*segmentInfoAuthorizer, error) {
	subject, err := resourceSubject(ctx)
	if err != nil {
		return nil, err
	}
	return &segmentInfoAuthorizer{node: node, subject: subject, decisions: make(map[int64]bool)}, nil
}

func (a *segmentInfoAuthorizer) permits(ctx context.Context, info *datapb.SegmentInfo) (bool, error) {
	if a.subject.bypass {
		return true, nil
	}
	collectionID := info.GetCollectionID()
	if allowed, ok := a.decisions[collectionID]; ok {
		return allowed, nil
	}
	_, allowed, err := a.node.authorizeCollectionID(ctx, collectionID, a.subject,
		commonpb.ObjectPrivilege_PrivilegeGetStatistics.String(), "segment")
	if err != nil {
		return false, err
	}
	a.decisions[collectionID] = allowed
	return allowed, nil
}

// check returns the error for the first requested segment the caller may not
// read, or nil. A segment the caller may not read is reported exactly as
// DataCoord reports a missing one, so that existence cannot be probed.
//
// DataCoord stops at the first missing segment, so its error also says that
// every segment requested before it exists. When fetchErr is such an error,
// the earlier segments are read one by one: the first one the caller may not
// read is reported instead, as DataCoord would report it had it not existed.
func (a *segmentInfoAuthorizer) check(ctx context.Context, segmentIDs []int64, infos []*datapb.SegmentInfo, fetchErr error) error {
	if a.subject.bypass {
		return fetchErr
	}
	if fetchErr == nil {
		for _, info := range infos {
			allowed, err := a.permits(ctx, info)
			if err != nil {
				return err
			}
			if !allowed {
				return merr.WrapErrSegmentNotFound(info.GetID())
			}
		}
		return nil
	}
	if !errors.Is(fetchErr, merr.ErrSegmentNotFound) {
		return fetchErr
	}
	for _, id := range segmentIDs {
		resp, err := a.node.mixCoord.GetSegmentInfo(ctx, &datapb.GetSegmentInfoRequest{
			SegmentIDs:       []int64{id},
			IncludeUnHealthy: true,
		})
		if err := merr.CheckRPCCall(resp, err); err != nil {
			return err
		}
		for _, info := range resp.GetInfos() {
			allowed, err := a.permits(ctx, info)
			if err != nil {
				return err
			}
			if !allowed {
				return merr.WrapErrSegmentNotFound(id)
			}
		}
	}
	// Every segment exists now; report what DataCoord reported.
	return fetchErr
}

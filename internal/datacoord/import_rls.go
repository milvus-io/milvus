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

package datacoord

import (
	"context"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/rls"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/internal/util/rlsutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const importRLSMetadataTimeout = 30 * time.Second

func (s *Server) getImportRLSMetadata(
	ctx context.Context,
	req *rootcoordpb.GetRLSMetadataRequest,
	description string,
) (*rootcoordpb.GetRLSMetadataResponse, error) {
	rpcCtx, cancel := context.WithTimeout(ctx, importRLSMetadataTimeout)
	defer cancel()
	resp, err := s.mixCoord.GetRLSMetadata(rpcCtx, req)
	if err = merr.CheckRPCCall(resp, err); err == nil {
		return resp, nil
	}
	return nil, rls.WrapMetadataRefreshError(err, "failed to get %s for import", description)
}

// resolveImportRLSPredicate reads one ordered metadata snapshot and returns a
// predicate that can be persisted with the import job. The import ACK callback
// holds the same exclusive canonical-collection resource key as every RLS
// mutation, so no policy or principal update can run between the two metadata
// reads under the broadcaster's collection-lock contract. Metadata read
// failures are retriable; the returned policy error is deterministic for this
// snapshot.
func (s *Server) resolveImportRLSPredicate(ctx context.Context, in *internalpb.ImportRequestInternal) ([]byte, error) {
	properties := in.GetSchema().GetProperties()
	enabled, err := common.IsRLSEnabled(properties...)
	if err != nil {
		return nil, merr.WrapErrDataIntegrity(err, "invalid persisted RLS collection properties")
	}
	force, err := common.IsRLSForce(properties...)
	if err != nil {
		return nil, merr.WrapErrDataIntegrity(err, "invalid persisted RLS collection properties")
	}
	if !enabled {
		return nil, nil
	}
	if !paramtable.Get().ProxyCfg.RLSImportEnforcementEnabled.GetAsBool() {
		return nil, merr.WrapErrImportSysFailed(
			"RLS import enforcement is unavailable until the cluster upgrade completes")
	}
	if in.GetSkipRls() {
		if force {
			return nil, merr.WrapErrPrivilegeNotPermitted(
				"import operation denied by RLS: skip_rls is not allowed when rls.force is enabled on collection %s",
				in.GetSchema().GetName())
		}
		return nil, nil
	}
	if importutilv2.IsL0Import(in.GetOptions()) {
		return nil, merr.WrapErrOperationNotSupportedMsg("RLS-protected L0 import is not supported")
	}
	principalName, _, err := rls.ResolveRuntimePrincipal(true, in.GetRlsPrincipal(), "import")
	if err != nil {
		return nil, err
	}
	if s.mixCoord == nil {
		return nil, merr.WrapErrServiceUnavailable("mixcoord is unavailable")
	}

	policyResp, err := s.getImportRLSMetadata(ctx, &rootcoordpb.GetRLSMetadataRequest{
		Base:         commonpbutil.NewMsgBase(commonpbutil.WithSourceID(paramtable.GetNodeID())),
		CollectionId: in.GetCollectionID(),
		Kind:         rootcoordpb.RLSMetadataKind_RLS_METADATA_KIND_POLICIES,
	}, "RLS policies")
	if err != nil {
		return nil, err
	}
	if policyResp.GetCollectionId() != in.GetCollectionID() {
		return nil, merr.WrapErrDataIntegrityMsg(
			"RLS policy metadata collection id mismatch: expected %d, received %d",
			in.GetCollectionID(), policyResp.GetCollectionId())
	}
	policyMap, err := rls.RowPoliciesFromInfo(in.GetCollectionID(), policyResp.GetPolicies())
	if err != nil {
		return nil, err
	}
	policies := make([]*rlsutil.RowPolicy, 0, len(policyMap))
	for _, policy := range policyMap {
		policies = append(policies, policy)
	}

	schema, err := typeutil.CreateSchemaHelper(in.GetSchema())
	if err != nil {
		return nil, merr.WrapErrDataIntegrity(err, "create schema helper for RLS import check")
	}
	loadPrincipalTags := func() (map[string]rlsutil.TagValue, error) {
		principalResp, err := s.getImportRLSMetadata(ctx, &rootcoordpb.GetRLSMetadataRequest{
			Base:          commonpbutil.NewMsgBase(commonpbutil.WithSourceID(paramtable.GetNodeID())),
			CollectionId:  in.GetCollectionID(),
			Kind:          rootcoordpb.RLSMetadataKind_RLS_METADATA_KIND_PRINCIPALS,
			PrincipalName: principalName,
		}, "RLS principal tags")
		if err != nil {
			return nil, err
		}
		if principalResp.GetCollectionId() != in.GetCollectionID() {
			return nil, merr.WrapErrDataIntegrityMsg(
				"RLS principal metadata collection id mismatch: expected %d, received %d",
				in.GetCollectionID(), principalResp.GetCollectionId())
		}

		tags := map[string]rlsutil.TagValue{}
		principals := principalResp.GetPrincipals()
		if len(principals) > 1 {
			return nil, merr.WrapErrDataIntegrityMsg("duplicated RLS principal metadata for %q", principalName)
		}
		for _, principal := range principals {
			if principal == nil || principal.GetPrincipalName() != principalName || principal.GetCollectionId() != in.GetCollectionID() {
				return nil, merr.WrapErrDataIntegrityMsg("invalid RLS principal metadata returned for %q", principalName)
			}
			decodedTags, err := rlsutil.TagsFromJSON(principal.GetTags())
			if err != nil {
				return nil, merr.WrapErrDataIntegrity(err, "decode RLS principal %q tags", principalName)
			}
			tags = decodedTags
		}
		return tags, nil
	}
	expr, err := rls.BuildCheckPredicate(
		policies,
		principalName,
		loadPrincipalTags,
		rlsutil.PolicyActionInsert,
		schema,
		paramtable.Get().ProxyCfg.RLSMaxCombinedExpressionLength.GetAsInt(),
	)
	if err != nil {
		return nil, err
	}
	if expr == nil {
		return nil, nil
	}
	serialized, err := proto.Marshal(expr)
	if err != nil {
		return nil, merr.WrapErrDataIntegrity(err, "marshal RLS import check predicate")
	}
	return serialized, nil
}

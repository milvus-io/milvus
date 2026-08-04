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

package rootcoord

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/util/rlsutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/ce"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const unallocatedRLSPolicyID int64 = -1

func (c *Core) broadcastCreateRLSPolicy(ctx context.Context, req *rlsutil.CreateRowPolicyRequest) error {
	broadcaster, err := c.startBroadcastWithAliasOrCollectionLock(ctx, req.GetDbName(), req.GetCollectionName())
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	// Use an unallocated ID while validating uniqueness and the complete policy.
	// Only a valid new policy should consume an ID, and the check must happen
	// while holding the collection guard so a concurrent mutation cannot change
	// the result.
	policy, err := c.meta.PrepareCreateRLSPolicy(ctx, req, unallocatedRLSPolicyID)
	if err != nil {
		return err
	}
	if policy.PolicyID == unallocatedRLSPolicyID {
		policy.PolicyID, err = c.idAllocator.AllocOne()
		if err != nil {
			return err
		}
	}
	return broadcastAlterRLSPolicy(ctx, broadcaster, policy, newRLSCacheExpirations(
		req.GetDbName(), req.GetCollectionName(), policy.CollectionID, commonpb.MsgType_CreateRowPolicy))
}

func (c *Core) broadcastUpdateRLSPolicy(ctx context.Context, req *rlsutil.UpdateRowPolicyRequest) error {
	broadcaster, err := c.startBroadcastWithAliasOrCollectionLock(ctx, req.GetDbName(), req.GetCollectionName())
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	policy, err := c.meta.PrepareUpdateRLSPolicy(ctx, req)
	if err != nil {
		return err
	}
	return broadcastAlterRLSPolicy(ctx, broadcaster, policy, newRLSCacheExpirations(
		req.GetDbName(), req.GetCollectionName(), policy.CollectionID, commonpb.MsgType_UpdateRowPolicy))
}

func broadcastAlterRLSPolicy(ctx context.Context, broadcaster broadcaster.BroadcastAPI, policy *model.RLSPolicy, cacheExpirations *message.CacheExpirations) error {
	msg := message.NewAlterRLSMetadataMessageBuilderV2().
		WithHeader(&message.AlterRLSMetadataMessageHeader{
			DbId:             policy.DBID,
			CollectionId:     policy.CollectionID,
			CacheExpirations: cacheExpirations,
		}).
		WithBody(&message.AlterRLSMetadataMessageBody{
			Metadata: &messagespb.AlterRLSMetadataMessageBody_Policy{
				Policy: marshalRLSPolicyMessage(policy),
			},
		}).
		WithBroadcast([]string{streaming.WAL().ControlChannel()}).
		MustBuildBroadcast()
	_, err := broadcaster.Broadcast(ctx, msg)
	return err
}

func (c *Core) broadcastDropRLSPolicy(ctx context.Context, req *rlsutil.DropRowPolicyRequest) error {
	broadcaster, err := c.startBroadcastWithAliasOrCollectionLock(ctx, req.GetDbName(), req.GetCollectionName())
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	policy, err := c.meta.PrepareDropRLSPolicy(ctx, req)
	if err != nil {
		return err
	}
	msg := message.NewDropRLSMetadataMessageBuilderV2().
		WithHeader(&message.DropRLSMetadataMessageHeader{
			DbId:             policy.DBID,
			CollectionId:     policy.CollectionID,
			CacheExpirations: newRLSCacheExpirations(req.GetDbName(), req.GetCollectionName(), policy.CollectionID, commonpb.MsgType_DropRowPolicy),
		}).
		WithBody(&message.DropRLSMetadataMessageBody{
			Metadata: &messagespb.DropRLSMetadataMessageBody_PolicyName{
				PolicyName: policy.PolicyName,
			},
		}).
		WithBroadcast([]string{streaming.WAL().ControlChannel()}).
		MustBuildBroadcast()
	_, err = broadcaster.Broadcast(ctx, msg)
	return err
}

func (c *Core) broadcastSetRLSPrincipalTags(ctx context.Context, req *rlsutil.SetRLSPrincipalTagsRequest) error {
	broadcaster, err := c.startBroadcastWithAliasOrCollectionLock(ctx, req.GetDbName(), req.GetCollectionName())
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	principal, err := c.meta.PrepareSetRLSPrincipalTags(ctx, req)
	if err != nil {
		return err
	}
	return broadcastAlterRLSPrincipal(ctx, broadcaster, principal, newRLSCacheExpirations(
		req.GetDbName(), req.GetCollectionName(), principal.CollectionID, commonpb.MsgType_SetRLSPrincipalTags))
}

func broadcastAlterRLSPrincipal(ctx context.Context, broadcaster broadcaster.BroadcastAPI, principal *model.RLSPrincipal, cacheExpirations *message.CacheExpirations) error {
	principalMessage, err := marshalRLSPrincipalMessage(principal)
	if err != nil {
		return err
	}
	msg := message.NewAlterRLSMetadataMessageBuilderV2().
		WithHeader(&message.AlterRLSMetadataMessageHeader{
			DbId:             principal.DBID,
			CollectionId:     principal.CollectionID,
			CacheExpirations: cacheExpirations,
		}).
		WithBody(&message.AlterRLSMetadataMessageBody{
			Metadata: &messagespb.AlterRLSMetadataMessageBody_Principal{
				Principal: principalMessage,
			},
		}).
		WithBroadcast([]string{streaming.WAL().ControlChannel()}).
		MustBuildBroadcast()
	_, err = broadcaster.Broadcast(ctx, msg)
	return err
}

func (c *Core) broadcastDeleteRLSPrincipalTags(ctx context.Context, req *rlsutil.DeleteRLSPrincipalTagsRequest) error {
	broadcaster, err := c.startBroadcastWithAliasOrCollectionLock(ctx, req.GetDbName(), req.GetCollectionName())
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	principal, drop, err := c.meta.PrepareDeleteRLSPrincipalTags(ctx, req)
	if err != nil {
		return err
	}
	if !drop {
		return broadcastAlterRLSPrincipal(ctx, broadcaster, principal, newRLSCacheExpirations(
			req.GetDbName(), req.GetCollectionName(), principal.CollectionID, commonpb.MsgType_DeleteRLSPrincipalTags))
	}
	msg := message.NewDropRLSMetadataMessageBuilderV2().
		WithHeader(&message.DropRLSMetadataMessageHeader{
			DbId:             principal.DBID,
			CollectionId:     principal.CollectionID,
			CacheExpirations: newRLSCacheExpirations(req.GetDbName(), req.GetCollectionName(), principal.CollectionID, commonpb.MsgType_DeleteRLSPrincipalTags),
		}).
		WithBody(&message.DropRLSMetadataMessageBody{
			Metadata: &messagespb.DropRLSMetadataMessageBody_PrincipalName{
				PrincipalName: principal.PrincipalName,
			},
		}).
		WithBroadcast([]string{streaming.WAL().ControlChannel()}).
		MustBuildBroadcast()
	_, err = broadcaster.Broadcast(ctx, msg)
	return err
}

func newRLSCacheExpirations(dbName string, collectionName string, collectionID int64, msgType commonpb.MsgType) *message.CacheExpirations {
	return ce.NewBuilder().WithLegacyProxyCollectionMetaCache(
		ce.OptLPCMDBName(dbName),
		ce.OptLPCMCollectionName(collectionName),
		ce.OptLPCMCollectionID(collectionID),
		ce.OptLPCMMsgType(msgType),
	).Build()
}

func marshalRLSPolicyMessage(policy *model.RLSPolicy) *messagespb.RLSPolicyMetadata {
	return &messagespb.RLSPolicyMetadata{
		PolicyId:    policy.PolicyID,
		PolicyName:  policy.PolicyName,
		PolicyType:  milvuspb.RowPolicyType(policy.PolicyType),
		Actions:     policyActionsToMilvusProto(policy.Actions),
		UsingExpr:   policy.UsingExpr,
		CheckExpr:   policy.CheckExpr,
		Description: policy.Description,
	}
}

func marshalRLSPrincipalMessage(principal *model.RLSPrincipal) (*messagespb.RLSPrincipalMetadata, error) {
	tags, err := rlsutil.TagsToJSON(principal.Tags)
	if err != nil {
		return nil, merr.WrapErrDataIntegrity(err, "encode RLS principal metadata")
	}
	return &messagespb.RLSPrincipalMetadata{
		PrincipalName: principal.PrincipalName,
		Tags:          tags,
	}, nil
}

func unmarshalRLSPolicyMessage(header *message.AlterRLSMetadataMessageHeader, policy *messagespb.RLSPolicyMetadata) *model.RLSPolicy {
	return &model.RLSPolicy{
		DBID:         header.GetDbId(),
		CollectionID: header.GetCollectionId(),
		PolicyID:     policy.GetPolicyId(),
		PolicyName:   policy.GetPolicyName(),
		PolicyType:   rlsutil.PolicyType(policy.GetPolicyType()),
		Actions:      policyActionsFromMilvusProto(policy.GetActions()),
		UsingExpr:    policy.GetUsingExpr(),
		CheckExpr:    policy.GetCheckExpr(),
		Description:  policy.GetDescription(),
	}
}

func policyActionsToMilvusProto(actions []rlsutil.PolicyAction) []milvuspb.RowPolicyAction {
	converted := make([]milvuspb.RowPolicyAction, len(actions))
	for i, action := range actions {
		converted[i] = milvuspb.RowPolicyAction(action)
	}
	return converted
}

func policyActionsFromMilvusProto(actions []milvuspb.RowPolicyAction) []rlsutil.PolicyAction {
	converted := make([]rlsutil.PolicyAction, len(actions))
	for i, action := range actions {
		converted[i] = rlsutil.PolicyAction(action)
	}
	return converted
}

func unmarshalRLSPrincipalMessage(header *message.AlterRLSMetadataMessageHeader, principal *messagespb.RLSPrincipalMetadata) (*model.RLSPrincipal, error) {
	tags, err := rlsutil.TagsFromJSON(principal.GetTags())
	if err != nil {
		return nil, merr.WrapErrDataIntegrity(err, "decode RLS principal metadata")
	}
	return &model.RLSPrincipal{
		DBID:          header.GetDbId(),
		CollectionID:  header.GetCollectionId(),
		PrincipalName: principal.GetPrincipalName(),
		Tags:          tags,
	}, nil
}

func (c *DDLCallback) alterRLSMetadataV2AckCallback(ctx context.Context, result message.BroadcastResultAlterRLSMetadataMessageV2) error {
	msg := result.Message
	header := msg.Header()
	var err error
	switch metadata := msg.MustBody().GetMetadata().(type) {
	case *messagespb.AlterRLSMetadataMessageBody_Policy:
		if metadata.Policy == nil {
			return merr.WrapErrServiceInternalMsg("alter RLS metadata message has nil policy")
		}
		err = c.meta.ApplyAlterRLSPolicy(ctx, unmarshalRLSPolicyMessage(header, metadata.Policy))
	case *messagespb.AlterRLSMetadataMessageBody_Principal:
		if metadata.Principal == nil {
			return merr.WrapErrServiceInternalMsg("alter RLS metadata message has nil principal")
		}
		principal, err := unmarshalRLSPrincipalMessage(header, metadata.Principal)
		if err != nil {
			return err
		}
		err = c.meta.ApplyAlterRLSPrincipal(ctx, principal)
	default:
		return merr.WrapErrServiceInternalMsg("alter RLS metadata message has no metadata")
	}
	if err != nil {
		return err
	}
	return c.ExpireCaches(ctx, header)
}

func (c *DDLCallback) dropRLSMetadataV2AckCallback(ctx context.Context, result message.BroadcastResultDropRLSMetadataMessageV2) error {
	msg := result.Message
	header := msg.Header()
	var err error
	switch metadata := msg.MustBody().GetMetadata().(type) {
	case *messagespb.DropRLSMetadataMessageBody_PolicyName:
		err = c.meta.ApplyDropRLSPolicy(ctx, header.GetCollectionId(), metadata.PolicyName)
	case *messagespb.DropRLSMetadataMessageBody_PrincipalName:
		err = c.meta.ApplyDropRLSPrincipal(ctx, header.GetCollectionId(), metadata.PrincipalName)
	default:
		return merr.WrapErrServiceInternalMsg("drop RLS metadata message has no metadata")
	}
	if err != nil {
		return err
	}
	return c.ExpireCaches(ctx, header)
}

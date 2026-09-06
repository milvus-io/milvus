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
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/metastore/model"
	mockrootcoord "github.com/milvus-io/milvus/internal/rootcoord/mocks"
	"github.com/milvus-io/milvus/internal/util/rlsutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestRLSMetadataAckCallbacks(t *testing.T) {
	ctx := context.Background()
	meta := mockrootcoord.NewIMetaTable(t)
	callback := &DDLCallback{Core: newTestCore(withMeta(meta))}

	policy := &model.RLSPolicy{
		DBID:         10,
		CollectionID: 20,
		PolicyID:     30,
		PolicyName:   "tenant_policy",
		PolicyType:   rlsutil.PolicyTypePermissive,
		Actions:      []rlsutil.PolicyAction{rlsutil.PolicyActionQuery},
		UsingExpr:    "tenant == $current_principal",
		Description:  "tenant isolation",
	}
	meta.EXPECT().ApplyAlterRLSPolicy(mock.Anything, mock.MatchedBy(func(actual *model.RLSPolicy) bool {
		return actual.DBID == policy.DBID &&
			actual.CollectionID == policy.CollectionID &&
			actual.PolicyID == policy.PolicyID &&
			actual.PolicyName == policy.PolicyName &&
			actual.UsingExpr == policy.UsingExpr
	})).Return(nil).Once()
	alterPolicy := message.NewAlterRLSMetadataMessageBuilderV2().
		WithHeader(&message.AlterRLSMetadataMessageHeader{DbId: policy.DBID, CollectionId: policy.CollectionID}).
		WithBody(&message.AlterRLSMetadataMessageBody{
			Metadata: &messagespb.AlterRLSMetadataMessageBody_Policy{Policy: marshalRLSPolicyMessage(policy)},
		}).
		WithBroadcast([]string{"control"}).
		MustBuildBroadcast()
	require.NoError(t, callback.alterRLSMetadataV2AckCallback(ctx, message.BroadcastResultAlterRLSMetadataMessageV2{
		Message: message.MustAsBroadcastAlterRLSMetadataMessageV2(alterPolicy),
	}))

	principal := &model.RLSPrincipal{
		DBID:          10,
		CollectionID:  20,
		PrincipalName: "alice",
		Tags: map[string]rlsutil.TagValue{
			"tenant": rlsutil.NewStringTagValue("acme"),
			"level":  rlsutil.NewInt64TagValue(3),
			"score":  rlsutil.NewDoubleTagValue(0.75),
		},
	}
	meta.EXPECT().ApplyAlterRLSPrincipal(mock.Anything, mock.MatchedBy(func(actual *model.RLSPrincipal) bool {
		return actual.DBID == principal.DBID &&
			actual.CollectionID == principal.CollectionID &&
			actual.PrincipalName == principal.PrincipalName &&
			actual.Tags["tenant"] == rlsutil.NewStringTagValue("acme") &&
			actual.Tags["level"] == rlsutil.NewInt64TagValue(3) &&
			actual.Tags["score"] == rlsutil.NewDoubleTagValue(0.75)
	})).Return(nil).Once()
	principalMessage, err := marshalRLSPrincipalMessage(principal)
	require.NoError(t, err)
	alterPrincipal := message.NewAlterRLSMetadataMessageBuilderV2().
		WithHeader(&message.AlterRLSMetadataMessageHeader{DbId: principal.DBID, CollectionId: principal.CollectionID}).
		WithBody(&message.AlterRLSMetadataMessageBody{
			Metadata: &messagespb.AlterRLSMetadataMessageBody_Principal{Principal: principalMessage},
		}).
		WithBroadcast([]string{"control"}).
		MustBuildBroadcast()
	require.NoError(t, callback.alterRLSMetadataV2AckCallback(ctx, message.BroadcastResultAlterRLSMetadataMessageV2{
		Message: message.MustAsBroadcastAlterRLSMetadataMessageV2(alterPrincipal),
	}))

	meta.EXPECT().ApplyDropRLSPolicy(mock.Anything, int64(20), "tenant_policy").Return(nil).Once()
	dropPolicy := message.NewDropRLSMetadataMessageBuilderV2().
		WithHeader(&message.DropRLSMetadataMessageHeader{DbId: 10, CollectionId: 20}).
		WithBody(&message.DropRLSMetadataMessageBody{
			Metadata: &messagespb.DropRLSMetadataMessageBody_PolicyName{PolicyName: "tenant_policy"},
		}).
		WithBroadcast([]string{"control"}).
		MustBuildBroadcast()
	require.NoError(t, callback.dropRLSMetadataV2AckCallback(ctx, message.BroadcastResultDropRLSMetadataMessageV2{
		Message: message.MustAsBroadcastDropRLSMetadataMessageV2(dropPolicy),
	}))

	meta.EXPECT().ApplyDropRLSPrincipal(mock.Anything, int64(20), "alice").Return(nil).Once()
	dropPrincipal := message.NewDropRLSMetadataMessageBuilderV2().
		WithHeader(&message.DropRLSMetadataMessageHeader{DbId: 10, CollectionId: 20}).
		WithBody(&message.DropRLSMetadataMessageBody{
			Metadata: &messagespb.DropRLSMetadataMessageBody_PrincipalName{PrincipalName: "alice"},
		}).
		WithBroadcast([]string{"control"}).
		MustBuildBroadcast()
	require.NoError(t, callback.dropRLSMetadataV2AckCallback(ctx, message.BroadcastResultDropRLSMetadataMessageV2{
		Message: message.MustAsBroadcastDropRLSMetadataMessageV2(dropPrincipal),
	}))
}

func TestRLSMetadataAckCallbacksRejectMissingPayload(t *testing.T) {
	callback := &DDLCallback{Core: newTestCore(withMeta(mockrootcoord.NewIMetaTable(t)))}

	alter := message.NewAlterRLSMetadataMessageBuilderV2().
		WithHeader(&message.AlterRLSMetadataMessageHeader{DbId: 10, CollectionId: 20}).
		WithBody(&message.AlterRLSMetadataMessageBody{}).
		WithBroadcast([]string{"control"}).
		MustBuildBroadcast()
	err := callback.alterRLSMetadataV2AckCallback(context.Background(), message.BroadcastResultAlterRLSMetadataMessageV2{
		Message: message.MustAsBroadcastAlterRLSMetadataMessageV2(alter),
	})
	require.ErrorIs(t, err, merr.ErrServiceInternal)

	drop := message.NewDropRLSMetadataMessageBuilderV2().
		WithHeader(&message.DropRLSMetadataMessageHeader{DbId: 10, CollectionId: 20}).
		WithBody(&message.DropRLSMetadataMessageBody{}).
		WithBroadcast([]string{"control"}).
		MustBuildBroadcast()
	err = callback.dropRLSMetadataV2AckCallback(context.Background(), message.BroadcastResultDropRLSMetadataMessageV2{
		Message: message.MustAsBroadcastDropRLSMetadataMessageV2(drop),
	})
	require.ErrorIs(t, err, merr.ErrServiceInternal)
}

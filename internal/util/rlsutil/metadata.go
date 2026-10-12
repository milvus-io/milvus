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

package rlsutil

import (
	"context"
	"slices"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// WrapMetadataRefreshError gives metadata RPC failures the same retry
// classification for every RLS consumer.
func WrapMetadataRefreshError(err error, format string, args ...any) error {
	if errors.IsAny(err, merr.ErrIoFailed, merr.ErrNodeNotFound, merr.ErrNodeNotMatch) {
		return merr.WrapErrServiceUnavailableErr(err, format, args...)
	}
	if merr.IsMilvusError(err) {
		return merr.Wrapf(err, format, args...)
	}
	code := status.Code(err)
	if errors.IsAny(err, context.Canceled, context.DeadlineExceeded) ||
		code == codes.Canceled || code == codes.DeadlineExceeded || code == codes.Unavailable {
		return merr.WrapErrServiceUnavailableErr(err, format, args...)
	}
	return merr.WrapErrServiceInternalErr(err, format, args...)
}

// RowPoliciesFromInfo validates and converts coordinator policy metadata.
func RowPoliciesFromInfo(collectionID int64, policies []*rootcoordpb.RLSPolicyInfo) (map[string]*RowPolicy, error) {
	converted := make(map[string]*RowPolicy, len(policies))
	for i, policy := range policies {
		if policy == nil {
			return nil, merr.WrapErrDataIntegrityMsg("RLS policy metadata at index %d is nil", i)
		}
		convertedPolicy := &RowPolicy{
			PolicyName:  policy.GetPolicyName(),
			PolicyType:  policy.GetPolicyType(),
			Actions:     slices.Clone(policy.GetActions()),
			UsingExpr:   policy.GetUsingExpr(),
			CheckExpr:   policy.GetCheckExpr(),
			Description: policy.GetDescription(),
			PolicyId:    policy.GetPolicyId(),
		}
		if policy.GetCollectionId() != collectionID {
			return nil, merr.WrapErrDataIntegrityMsg(
				"RLS policy %q collection id mismatch: expected %d, received %d",
				convertedPolicy.GetPolicyName(), collectionID, policy.GetCollectionId())
		}
		if convertedPolicy.PolicyId <= 0 {
			return nil, merr.WrapErrDataIntegrityMsg("RLS policy %q has invalid id %d", convertedPolicy.GetPolicyName(), convertedPolicy.PolicyId)
		}
		if err := ValidateStoredPolicy(
			convertedPolicy.GetPolicyName(), convertedPolicy.GetPolicyType(), convertedPolicy.GetActions(),
			convertedPolicy.GetUsingExpr(), convertedPolicy.GetCheckExpr(),
		); err != nil {
			return nil, merr.WrapErrDataIntegrity(err, "invalid RLS policy %q metadata", convertedPolicy.GetPolicyName())
		}
		if _, ok := converted[convertedPolicy.GetPolicyName()]; ok {
			return nil, merr.WrapErrDataIntegrityMsg("duplicated RLS policy name %q", convertedPolicy.GetPolicyName())
		}
		converted[convertedPolicy.GetPolicyName()] = convertedPolicy
	}
	return converted, nil
}

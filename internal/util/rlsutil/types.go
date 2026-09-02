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
	"encoding/json"
	"io"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type TagValueKind int32

const (
	TagValueKindUnknown TagValueKind = iota
	TagValueKindString
	TagValueKindInt64
	TagValueKindDouble
)

type TagValue struct {
	Kind        TagValueKind
	StringValue string
	Int64Value  int64
	DoubleValue float64
}

func NewStringTagValue(value string) TagValue {
	return TagValue{Kind: TagValueKindString, StringValue: value}
}

func NewInt64TagValue(value int64) TagValue {
	return TagValue{Kind: TagValueKindInt64, Int64Value: value}
}

func NewDoubleTagValue(value float64) TagValue {
	return TagValue{Kind: TagValueKindDouble, DoubleValue: value}
}

func TagsFromJSON(payload string) (map[string]TagValue, error) {
	decoder := json.NewDecoder(strings.NewReader(payload))
	decoder.UseNumber()
	var raw map[string]any
	if err := decoder.Decode(&raw); err != nil {
		return nil, merr.WrapErrParameterInvalidMsg("RLS principal tags must be a valid JSON object: %s", err)
	}
	if raw == nil {
		return nil, merr.WrapErrParameterInvalidMsg("RLS principal tags must be a JSON object")
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return nil, err
	}
	tags := make(map[string]TagValue, len(raw))
	for key, value := range raw {
		switch typed := value.(type) {
		case string:
			tags[key] = NewStringTagValue(typed)
		case json.Number:
			if strings.ContainsAny(typed.String(), ".eE") {
				value, err := strconv.ParseFloat(typed.String(), 64)
				if err != nil {
					return nil, merr.WrapErrParameterInvalidMsg("RLS principal tag %q has an invalid double value", key)
				}
				tags[key] = NewDoubleTagValue(value)
			} else {
				value, err := strconv.ParseInt(typed.String(), 10, 64)
				if err == nil {
					tags[key] = NewInt64TagValue(value)
					continue
				}
				doubleValue, doubleErr := strconv.ParseFloat(typed.String(), 64)
				if doubleErr != nil {
					return nil, merr.WrapErrParameterInvalidMsg("RLS principal tag %q has an invalid numeric value", key)
				}
				tags[key] = NewDoubleTagValue(doubleValue)
			}
		default:
			return nil, merr.WrapErrParameterInvalidMsg("RLS principal tag %q must be a string, int64, or double", key)
		}
	}
	return tags, nil
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return merr.WrapErrParameterInvalidMsg("RLS principal tags must contain exactly one JSON object")
		}
		return merr.WrapErrParameterInvalidMsg("RLS principal tags contain invalid trailing data: %s", err)
	}
	return nil
}

func TagsToJSON(tags map[string]TagValue) (string, error) {
	values := make(map[string]any, len(tags))
	for key, value := range tags {
		switch value.Kind {
		case TagValueKindString:
			values[key] = value.StringValue
		case TagValueKindInt64:
			values[key] = value.Int64Value
		case TagValueKindDouble:
			values[key] = value.DoubleValue
		default:
			return "", merr.WrapErrServiceInternalMsg("RLS principal tag %q has unsupported internal value type", key)
		}
	}
	payload, err := json.Marshal(values)
	if err != nil {
		return "", merr.WrapErrDataIntegrity(err, "encode RLS principal tags")
	}
	return string(payload), nil
}

func CloneTags(tags map[string]TagValue) map[string]TagValue {
	if tags == nil {
		return nil
	}
	cloned := make(map[string]TagValue, len(tags))
	for key, value := range tags {
		cloned[key] = value
	}
	return cloned
}

type PolicyType int32

const (
	PolicyTypeUnknown     PolicyType = 0
	PolicyTypePermissive  PolicyType = 1
	PolicyTypeRestrictive PolicyType = 2
)

func (policyType PolicyType) String() string {
	switch policyType {
	case PolicyTypePermissive:
		return "RowPolicyTypePermissive"
	case PolicyTypeRestrictive:
		return "RowPolicyTypeRestrictive"
	default:
		return "RowPolicyTypeUnknown"
	}
}

type PolicyAction int32

const (
	PolicyActionQuery          PolicyAction = 0
	PolicyActionSearch         PolicyAction = 1
	PolicyActionInsert         PolicyAction = 2
	PolicyActionDelete         PolicyAction = 3
	PolicyActionUpsert         PolicyAction = 4
	PolicyActionQueryIterator  PolicyAction = 5
	PolicyActionSearchIterator PolicyAction = 6
	PolicyActionHybridSearch   PolicyAction = 7
)

func (action PolicyAction) String() string {
	switch action {
	case PolicyActionQuery:
		return "RowPolicyActionQuery"
	case PolicyActionQueryIterator:
		return "RowPolicyActionQueryIterator"
	case PolicyActionSearch:
		return "RowPolicyActionSearch"
	case PolicyActionSearchIterator:
		return "RowPolicyActionSearchIterator"
	case PolicyActionHybridSearch:
		return "RowPolicyActionHybridSearch"
	case PolicyActionDelete:
		return "RowPolicyActionDelete"
	case PolicyActionInsert:
		return "RowPolicyActionInsert"
	case PolicyActionUpsert:
		return "RowPolicyActionUpsert"
	default:
		return "RowPolicyActionUnknown"
	}
}

type CreateRowPolicyRequest struct {
	DbName         string
	CollectionName string
	PolicyName     string
	PolicyType     PolicyType
	Actions        []PolicyAction
	UsingExpr      string
	CheckExpr      string
	Description    string
}

func (request *CreateRowPolicyRequest) GetDbName() string {
	if request == nil {
		return ""
	}
	return request.DbName
}

func (request *CreateRowPolicyRequest) GetCollectionName() string {
	if request == nil {
		return ""
	}
	return request.CollectionName
}

func (request *CreateRowPolicyRequest) GetPolicyName() string {
	if request == nil {
		return ""
	}
	return request.PolicyName
}

func (request *CreateRowPolicyRequest) GetPolicyType() PolicyType {
	if request == nil {
		return PolicyTypeUnknown
	}
	return request.PolicyType
}

func (request *CreateRowPolicyRequest) GetActions() []PolicyAction {
	if request == nil {
		return nil
	}
	return request.Actions
}

func (request *CreateRowPolicyRequest) GetUsingExpr() string {
	if request == nil {
		return ""
	}
	return request.UsingExpr
}

func (request *CreateRowPolicyRequest) GetCheckExpr() string {
	if request == nil {
		return ""
	}
	return request.CheckExpr
}

func (request *CreateRowPolicyRequest) GetDescription() string {
	if request == nil {
		return ""
	}
	return request.Description
}

type UpdateRowPolicyRequest = CreateRowPolicyRequest

type DropRowPolicyRequest struct {
	DbName         string
	CollectionName string
	PolicyName     string
}

func (request *DropRowPolicyRequest) GetDbName() string {
	if request == nil {
		return ""
	}
	return request.DbName
}

func (request *DropRowPolicyRequest) GetCollectionName() string {
	if request == nil {
		return ""
	}
	return request.CollectionName
}

func (request *DropRowPolicyRequest) GetPolicyName() string {
	if request == nil {
		return ""
	}
	return request.PolicyName
}

type ListRowPoliciesRequest struct {
	DbName         string
	CollectionName string
}

func (request *ListRowPoliciesRequest) GetDbName() string {
	if request == nil {
		return ""
	}
	return request.DbName
}

func (request *ListRowPoliciesRequest) GetCollectionName() string {
	if request == nil {
		return ""
	}
	return request.CollectionName
}

type RowPolicy struct {
	PolicyName  string
	PolicyType  PolicyType
	Actions     []PolicyAction
	UsingExpr   string
	CheckExpr   string
	Description string
	PolicyId    int64
}

func (policy *RowPolicy) GetPolicyName() string {
	if policy == nil {
		return ""
	}
	return policy.PolicyName
}

func (policy *RowPolicy) GetPolicyType() PolicyType {
	if policy == nil {
		return PolicyTypeUnknown
	}
	return policy.PolicyType
}

func (policy *RowPolicy) GetActions() []PolicyAction {
	if policy == nil {
		return nil
	}
	return policy.Actions
}

func (policy *RowPolicy) GetUsingExpr() string {
	if policy == nil {
		return ""
	}
	return policy.UsingExpr
}

func (policy *RowPolicy) GetCheckExpr() string {
	if policy == nil {
		return ""
	}
	return policy.CheckExpr
}

type ListRowPoliciesResponse struct {
	Status         *commonpb.Status
	Policies       []*RowPolicy
	DbName         string
	CollectionName string
}

type SetRLSPrincipalTagsRequest struct {
	DbName         string
	CollectionName string
	PrincipalName  string
	Tags           map[string]TagValue
}

func (request *SetRLSPrincipalTagsRequest) GetDbName() string {
	if request == nil {
		return ""
	}
	return request.DbName
}

func (request *SetRLSPrincipalTagsRequest) GetCollectionName() string {
	if request == nil {
		return ""
	}
	return request.CollectionName
}

func (request *SetRLSPrincipalTagsRequest) GetPrincipalName() string {
	if request == nil {
		return ""
	}
	return request.PrincipalName
}

func (request *SetRLSPrincipalTagsRequest) GetTags() map[string]TagValue {
	if request == nil {
		return nil
	}
	return request.Tags
}

type GetRLSPrincipalTagsRequest struct {
	DbName         string
	CollectionName string
	PrincipalName  string
}

func (request *GetRLSPrincipalTagsRequest) GetDbName() string {
	if request == nil {
		return ""
	}
	return request.DbName
}

func (request *GetRLSPrincipalTagsRequest) GetCollectionName() string {
	if request == nil {
		return ""
	}
	return request.CollectionName
}

func (request *GetRLSPrincipalTagsRequest) GetPrincipalName() string {
	if request == nil {
		return ""
	}
	return request.PrincipalName
}

type GetRLSPrincipalTagsResponse struct {
	Status         *commonpb.Status
	Tags           map[string]TagValue
	DbName         string
	CollectionName string
	PrincipalName  string
}

type ListRLSPrincipalsRequest struct {
	DbName         string
	CollectionName string
}

func (request *ListRLSPrincipalsRequest) GetDbName() string {
	if request == nil {
		return ""
	}
	return request.DbName
}

func (request *ListRLSPrincipalsRequest) GetCollectionName() string {
	if request == nil {
		return ""
	}
	return request.CollectionName
}

type ListRLSPrincipalsResponse struct {
	Status         *commonpb.Status
	PrincipalNames []string
	DbName         string
	CollectionName string
}

type DeleteRLSPrincipalTagsRequest struct {
	DbName         string
	CollectionName string
	PrincipalName  string
	TagKeys        []string
}

func (request *DeleteRLSPrincipalTagsRequest) GetDbName() string {
	if request == nil {
		return ""
	}
	return request.DbName
}

func (request *DeleteRLSPrincipalTagsRequest) GetCollectionName() string {
	if request == nil {
		return ""
	}
	return request.CollectionName
}

func (request *DeleteRLSPrincipalTagsRequest) GetPrincipalName() string {
	if request == nil {
		return ""
	}
	return request.PrincipalName
}

func (request *DeleteRLSPrincipalTagsRequest) GetTagKeys() []string {
	if request == nil {
		return nil
	}
	return request.TagKeys
}

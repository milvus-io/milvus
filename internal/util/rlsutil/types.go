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
	"math"
	"strconv"
	"strings"
	"unsafe"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type TagValueKind int32

const (
	TagValueKindUnknown TagValueKind = iota
	TagValueKindString
	TagValueKindInt64
	TagValueKindDouble
	TagValueKindArray
)

type TagValue struct {
	Kind        TagValueKind
	StringValue string
	Int64Value  int64
	DoubleValue float64
	// arrayValue is immutable after construction so TagValue remains safely
	// shallow-copyable across metadata snapshots and caches.
	arrayValue []TagValue
}

const tagValueRetainedSize = int64(unsafe.Sizeof(TagValue{}))

func NewStringTagValue(value string) TagValue {
	return TagValue{Kind: TagValueKindString, StringValue: value}
}

func NewInt64TagValue(value int64) TagValue {
	return TagValue{Kind: TagValueKindInt64, Int64Value: value}
}

func NewDoubleTagValue(value float64) TagValue {
	return TagValue{Kind: TagValueKindDouble, DoubleValue: value}
}

// NewArrayTagValue copies elements and promotes numeric arrays to double when
// any element is a double. String/number mixtures remain invalid for validation.
func NewArrayTagValue(values []TagValue) TagValue {
	cloned := make([]TagValue, len(values))
	hasDouble := false
	for i, value := range values {
		cloned[i] = value
		hasDouble = hasDouble || value.Kind == TagValueKindDouble
	}
	if hasDouble {
		for i, value := range cloned {
			if value.Kind == TagValueKindInt64 {
				cloned[i] = NewDoubleTagValue(float64(value.Int64Value))
			}
		}
	}
	return TagValue{
		Kind:       TagValueKindArray,
		arrayValue: cloned,
	}
}

// ArrayValues returns a caller-owned copy of an array tag's elements.
func (value TagValue) ArrayValues() []TagValue {
	if value.arrayValue == nil {
		return nil
	}
	return append([]TagValue(nil), value.arrayValue...)
}

// PrincipalTagsSize returns the bytes charged to the principal cache. Values
// include their TagValue storage, and arrays include their backing elements, so
// scalar representation growth and empty strings cannot bypass the cap.
func PrincipalTagsSize(principalName string, tags map[string]TagValue) (int64, error) {
	size := int64(len(principalName))
	for key, value := range tags {
		size += int64(len(key))
		valueSize, ok := tagValueSize(value)
		if !ok {
			return 0, merr.WrapErrServiceInternalMsg("RLS principal tag %q has unsupported internal value type", key)
		}
		size += valueSize
	}
	return size, nil
}

func tagValueSize(value TagValue) (int64, bool) {
	size := tagValueRetainedSize
	switch value.Kind {
	case TagValueKindString:
		if int64(len(value.StringValue)) > math.MaxInt64-size {
			return 0, false
		}
		return size + int64(len(value.StringValue)), true
	case TagValueKindInt64, TagValueKindDouble:
		return size, true
	case TagValueKindArray:
		if value.arrayValue == nil {
			return 0, false
		}
		for _, element := range value.arrayValue {
			if element.Kind == TagValueKindArray {
				return 0, false
			}
			elementSize, ok := tagValueSize(element)
			if !ok || elementSize > math.MaxInt64-size {
				return 0, false
			}
			size += elementSize
		}
		return size, true
	default:
		return 0, false
	}
}

func TagsFromJSON(payload string) (map[string]TagValue, error) {
	return tagsFromJSON(payload, 0)
}

// TagsFromJSONWithLimit bounds the raw payload, object members, and array
// elements while decoding untrusted requests.
func TagsFromJSONWithLimit(payload string, maxTags int) (map[string]TagValue, error) {
	if maxTags > 0 {
		if err := validatePrincipalTagsJSONTransportSize(payload, maxTags); err != nil {
			return nil, err
		}
	}
	return tagsFromJSON(payload, maxTags)
}

func tagsFromJSON(payload string, maxTags int) (map[string]TagValue, error) {
	decoder := json.NewDecoder(strings.NewReader(payload))
	decoder.UseNumber()
	token, err := decoder.Token()
	if err != nil {
		return nil, merr.WrapErrParameterInvalidMsg("RLS principal tags must be a valid JSON object: %s", err)
	}
	delim, ok := token.(json.Delim)
	if !ok || delim != '{' {
		return nil, merr.WrapErrParameterInvalidMsg("RLS principal tags must be a JSON object")
	}

	tags := make(map[string]TagValue)
	entryCount := 0
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return nil, merr.WrapErrParameterInvalidMsg("RLS principal tags must be a valid JSON object: %s", err)
		}
		key, ok := keyToken.(string)
		if !ok {
			return nil, merr.WrapErrParameterInvalidMsg("RLS principal tags must be a valid JSON object")
		}
		entryCount++
		if maxTags > 0 && entryCount > maxTags {
			return nil, merr.WrapErrServiceQuotaExceeded("unable to set RLS principal tags because the number of tags has reached the limit")
		}
		if maxTags > 0 {
			maxTagKeyLength := paramtable.Get().ProxyCfg.RLSMaxTagKeyLength.GetAsInt()
			if len(key) > maxTagKeyLength {
				return nil, merr.WrapErrParameterInvalidMsg("RLS principal tag key exceeds max length %d", maxTagKeyLength)
			}
		}

		maxArrayElements := 0
		if maxTags > 0 {
			maxArrayElements = paramtable.Get().ProxyCfg.RLSMaxArrayLiteralElements.GetAsInt()
		}
		tagValue, err := decodeTagValue(decoder, key, maxArrayElements, true)
		if err != nil {
			return nil, err
		}
		if maxTags > 0 {
			if err := validateTagValue(key, tagValue); err != nil {
				return nil, err
			}
		}
		tags[key] = tagValue
	}
	closingToken, err := decoder.Token()
	if err != nil {
		return nil, merr.WrapErrParameterInvalidMsg("RLS principal tags must be a valid JSON object: %s", err)
	}
	closingDelim, ok := closingToken.(json.Delim)
	if !ok || closingDelim != '}' {
		return nil, merr.WrapErrParameterInvalidMsg("RLS principal tags must be a valid JSON object")
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return nil, err
	}
	return tags, nil
}

func decodeTagValue(decoder *json.Decoder, key string, maxArrayElements int, allowArray bool) (TagValue, error) {
	value, err := decoder.Token()
	if err != nil {
		return TagValue{}, merr.WrapErrParameterInvalidMsg("RLS principal tags must be a valid JSON object: %s", err)
	}
	switch typed := value.(type) {
	case string:
		return NewStringTagValue(typed), nil
	case json.Number:
		number := typed.String()
		if strings.ContainsAny(number, ".eE") {
			value, err := strconv.ParseFloat(number, 64)
			if err != nil {
				return TagValue{}, merr.WrapErrParameterInvalidMsg("RLS principal tag %q has an invalid double value", key)
			}
			return NewDoubleTagValue(value), nil
		}
		value, err := strconv.ParseInt(number, 10, 64)
		if err == nil {
			return NewInt64TagValue(value), nil
		}
		// encoding/json may serialize an integral double without a decimal
		// point. Preserve values outside int64 as doubles on round trip.
		doubleValue, doubleErr := strconv.ParseFloat(number, 64)
		if doubleErr != nil {
			return TagValue{}, merr.WrapErrParameterInvalidMsg("RLS principal tag %q has an invalid numeric value", key)
		}
		return NewDoubleTagValue(doubleValue), nil
	case json.Delim:
		if typed != '[' || !allowArray {
			return TagValue{}, merr.WrapErrParameterInvalidMsg("RLS principal tag %q must be a string, int64, double, or one-dimensional array of those types", key)
		}
		elements := make([]TagValue, 0)
		for decoder.More() {
			if maxArrayElements > 0 && len(elements) >= maxArrayElements {
				return TagValue{}, merr.WrapErrParameterInvalidMsg("RLS principal tag %q exceeds max array elements %d", key, maxArrayElements)
			}
			element, err := decodeTagValue(decoder, key, maxArrayElements, false)
			if err != nil {
				return TagValue{}, err
			}
			if len(elements) > 0 && element.Kind != elements[0].Kind &&
				(element.Kind == TagValueKindString || elements[0].Kind == TagValueKindString) {
				return TagValue{}, merr.WrapErrParameterInvalidMsg("RLS principal tag %q array cannot mix strings and numbers", key)
			}
			if maxArrayElements > 0 {
				if err := validateTagValue(key, element); err != nil {
					return TagValue{}, err
				}
			}
			elements = append(elements, element)
		}
		closing, err := decoder.Token()
		if err != nil {
			return TagValue{}, merr.WrapErrParameterInvalidMsg("RLS principal tags must be a valid JSON object: %s", err)
		}
		if closing != json.Delim(']') {
			return TagValue{}, merr.WrapErrParameterInvalidMsg("RLS principal tag %q has an invalid array value", key)
		}
		return NewArrayTagValue(elements), nil
	default:
		return TagValue{}, merr.WrapErrParameterInvalidMsg("RLS principal tag %q must be a string, int64, double, or one-dimensional array of those types", key)
	}
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
		encoded, ok := tagValueToJSON(value)
		if !ok {
			return "", merr.WrapErrServiceInternalMsg("RLS principal tag %q has unsupported internal value type", key)
		}
		values[key] = encoded
	}
	payload, err := json.Marshal(values)
	if err != nil {
		return "", merr.WrapErrDataIntegrity(err, "encode RLS principal tags")
	}
	return string(payload), nil
}

func tagValueToJSON(value TagValue) (any, bool) {
	switch value.Kind {
	case TagValueKindString:
		return value.StringValue, true
	case TagValueKindInt64:
		return value.Int64Value, true
	case TagValueKindDouble:
		encoded := strconv.FormatFloat(value.DoubleValue, 'g', -1, 64)
		if !strings.ContainsAny(encoded, ".eE") {
			encoded += ".0"
		}
		return json.Number(encoded), true
	case TagValueKindArray:
		if value.arrayValue == nil {
			return nil, false
		}
		values := make([]any, len(value.arrayValue))
		for i, element := range value.arrayValue {
			if element.Kind != value.arrayValue[0].Kind {
				return nil, false
			}
			var ok bool
			values[i], ok = tagValueToJSON(element)
			if !ok || element.Kind == TagValueKindArray {
				return nil, false
			}
		}
		return values, true
	default:
		return nil, false
	}
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
		return "Query"
	case PolicyActionSearch:
		return "Search"
	case PolicyActionInsert:
		return "Insert"
	case PolicyActionDelete:
		return "Delete"
	case PolicyActionUpsert:
		return "Upsert"
	case PolicyActionQueryIterator:
		return "QueryIterator"
	case PolicyActionSearchIterator:
		return "SearchIterator"
	case PolicyActionHybridSearch:
		return "HybridSearch"
	default:
		return "Unknown"
	}
}

func PolicyActionOperation(action PolicyAction) string {
	switch action {
	case PolicyActionQuery:
		return "query"
	case PolicyActionQueryIterator:
		return "query iterator"
	case PolicyActionSearch:
		return "search"
	case PolicyActionSearchIterator:
		return "search iterator"
	case PolicyActionHybridSearch:
		return "hybrid search"
	case PolicyActionDelete:
		return "delete"
	case PolicyActionInsert:
		return "insert"
	case PolicyActionUpsert:
		return "upsert"
	default:
		return "unknown"
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

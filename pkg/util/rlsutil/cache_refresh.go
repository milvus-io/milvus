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
	"fmt"

	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
)

const (
	// RLSProxyRefreshOpBase is used to tunnel RLS cache refresh events through
	// proxypb.RefreshPolicyInfoCacheRequest(opType/opKey).
	RLSProxyRefreshOpBase int32 = 1000
)

type refreshPayload struct {
	OpType         int32  `json:"op_type"`
	DBName         string `json:"db_name,omitempty"`
	CollectionName string `json:"collection_name,omitempty"`
	PolicyName     string `json:"policy_name,omitempty"`
	UserName       string `json:"user_name,omitempty"`
}

// EncodeRefresh encodes an RLS refresh event into proxypb opType/opKey.
func EncodeRefresh(in *messagespb.RefreshRLSCacheRequest) (int32, string, error) {
	if in == nil {
		return 0, "", fmt.Errorf("nil refresh request")
	}

	rawOpType := int32(in.GetOpType())
	if rawOpType < 0 {
		return 0, "", fmt.Errorf("invalid rls op type: %d", rawOpType)
	}

	payload := refreshPayload{
		OpType:         rawOpType,
		DBName:         in.GetDbName(),
		CollectionName: in.GetCollectionName(),
		PolicyName:     in.GetPolicyName(),
		UserName:       in.GetUserName(),
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return 0, "", err
	}

	return RLSProxyRefreshOpBase + rawOpType, string(data), nil
}

// DecodeRefresh decodes proxypb opType/opKey into an RLS refresh event.
// The second return value indicates whether the opType belongs to RLS.
func DecodeRefresh(opType int32, opKey string) (*messagespb.RefreshRLSCacheRequest, bool, error) {
	if opType < RLSProxyRefreshOpBase {
		return nil, false, nil
	}

	rawOpType := opType - RLSProxyRefreshOpBase
	req := &messagespb.RefreshRLSCacheRequest{
		OpType: messagespb.RLSCacheOpType(rawOpType),
	}

	if opKey == "" {
		return req, true, nil
	}

	var payload refreshPayload
	if err := json.Unmarshal([]byte(opKey), &payload); err != nil {
		return nil, true, err
	}

	if payload.OpType >= 0 {
		req.OpType = messagespb.RLSCacheOpType(payload.OpType)
	}
	req.DbName = payload.DBName
	req.CollectionName = payload.CollectionName
	req.PolicyName = payload.PolicyName
	req.UserName = payload.UserName
	return req, true, nil
}

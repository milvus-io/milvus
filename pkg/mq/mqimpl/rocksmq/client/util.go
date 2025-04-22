// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package client

import (
	"google.golang.org/protobuf/proto"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
)

func MarshalHeader(header *commonpb.MsgHeader) ([]byte, error) {
	hb, err := proto.Marshal(header)
	if err != nil {
		return nil, err
	}
	return hb, nil
}

func UnmarshalHeader(headerbyte []byte) (*commonpb.MsgHeader, error) {
	header := commonpb.MsgHeader{}
	if headerbyte == nil {
		return &header, errors.New("failed to unmarshal message header, payload is empty")
	}
	err := proto.Unmarshal(headerbyte, &header)
	if err != nil {
		return &header, err
	}
	if header.Base == nil {
		return nil, errors.New("failed to unmarshal message, header is uncomplete")
	}
	return &header, nil
}

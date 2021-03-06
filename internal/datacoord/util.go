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
package datacoord

import (
	"errors"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
)

type Response interface {
	GetStatus() *commonpb.Status
}

var errNilResponse = errors.New("response is nil")
var errUnknownResponseType = errors.New("unknown response type")

func VerifyResponse(response interface{}, err error) error {
	if err != nil {
		return err
	}
	if response == nil {
		return errNilResponse
	}
	switch resp := response.(type) {
	case Response:
		if resp.GetStatus().ErrorCode != commonpb.ErrorCode_Success {
			return errors.New(resp.GetStatus().Reason)
		}
	case *commonpb.Status:
		if resp.ErrorCode != commonpb.ErrorCode_Success {
			return errors.New(resp.Reason)
		}
	default:
		return errUnknownResponseType
	}
	return nil
}

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

package common

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
)

var (
	// ErrNodeIDNotMatch stands for the error that grpc target id and node session id not match.
	ErrNodeIDNotMatch = errors.New("target node id not match")
)

// WrapNodeIDNotMatchError wraps `ErrNodeIDNotMatch` with targetID and sessionID.
func WrapNodeIDNotMatchError(targetID, nodeID int64) error {
	return fmt.Errorf("%w target id = %d, node id = %d", ErrNodeIDNotMatch, targetID, nodeID)
}

// WrapNodeIDNotMatchMsg fmt error msg with `ErrNodeIDNotMatch`, targetID and sessionID.
func WrapNodeIDNotMatchMsg(targetID, nodeID int64) string {
	return fmt.Sprintf("%s target id = %d, node id = %d", ErrNodeIDNotMatch.Error(), targetID, nodeID)
}

type IgnorableError struct {
	msg string
}

func (i *IgnorableError) Error() string {
	return i.msg
}

func NewIgnorableError(err error) error {
	return &IgnorableError{
		msg: err.Error(),
	}
}

func IsIgnorableError(err error) bool {
	_, ok := err.(*IgnorableError)
	return ok
}

var _ error = &KeyNotExistError{}

func NewKeyNotExistError(key string) error {
	return &KeyNotExistError{key: key}
}

func IsKeyNotExistError(err error) bool {
	_, ok := err.(*KeyNotExistError)
	return ok
}

type KeyNotExistError struct {
	key string
}

func (k *KeyNotExistError) Error() string {
	return fmt.Sprintf("there is no value on key = %s", k.key)
}

type statusError struct {
	commonpb.Status
}

func (e *statusError) Error() string {
	return fmt.Sprintf("code: %s, reason: %s", e.GetErrorCode().String(), e.GetReason())
}

func NewStatusError(code commonpb.ErrorCode, reason string) *statusError {
	return &statusError{Status: commonpb.Status{ErrorCode: code, Reason: reason}}
}

func IsStatusError(e error) bool {
	_, ok := e.(*statusError)
	return ok
}

var (
	// static variable, save temporary memory.
	collectionNotExistCodes = []commonpb.ErrorCode{
		commonpb.ErrorCode_UnexpectedError, // TODO: remove this after SDK remove this dependency.
		commonpb.ErrorCode_CollectionNotExists,
	}
)

func NewCollectionNotExistError(msg string) *statusError {
	return NewStatusError(commonpb.ErrorCode_CollectionNotExists, msg)
}

func IsCollectionNotExistError(e error) bool {
	statusError, ok := e.(*statusError)
	if !ok {
		return false
	}
	// cycle import: common -> funcutil -> types -> sessionutil -> common
	// return funcutil.SliceContain(collectionNotExistCodes, statusError.GetErrorCode())
	if statusError.Status.ErrorCode == commonpb.ErrorCode_CollectionNotExists {
		return true
	}

	if (statusError.Status.ErrorCode == commonpb.ErrorCode_UnexpectedError) && strings.Contains(statusError.Status.Reason, "can't find collection") {
		return true
	}
	return false
}

func IsCollectionNotExistErrorV2(e error) bool {
	statusError, ok := e.(*statusError)
	if !ok {
		return false
	}
	return statusError.GetErrorCode() == commonpb.ErrorCode_CollectionNotExists
}

func StatusFromError(e error) *commonpb.Status {
	if e == nil {
		return &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}
	}
	statusError, ok := e.(*statusError)
	if !ok {
		return &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError, Reason: e.Error()}
	}
	return &commonpb.Status{ErrorCode: statusError.GetErrorCode(), Reason: statusError.GetReason()}
}

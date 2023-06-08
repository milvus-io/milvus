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
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/stretchr/testify/assert"
)

func TestIgnorableError(t *testing.T) {
	err := errors.New("test err")
	iErr := NewIgnorableError(err)
	assert.True(t, IsIgnorableError(iErr))
	assert.False(t, IsIgnorableError(err))
}

func TestNotExistError(t *testing.T) {
	err := errors.New("err")
	assert.Equal(t, false, IsKeyNotExistError(err))
	assert.Equal(t, true, IsKeyNotExistError(NewKeyNotExistError("foo")))
}

func TestStatusError_Error(t *testing.T) {
	err := NewCollectionNotExistError("collection not exist")
	assert.True(t, IsStatusError(err))
	assert.True(t, strings.Contains(err.Error(), "collection not exist"))
}

func TestIsStatusError(t *testing.T) {
	err := NewCollectionNotExistError("collection not exist")
	assert.True(t, IsStatusError(err))
	assert.False(t, IsStatusError(errors.New("not status error")))
	assert.False(t, IsStatusError(nil))
}

func Test_IsCollectionNotExistError(t *testing.T) {
	assert.False(t, IsCollectionNotExistError(nil))
	assert.False(t, IsCollectionNotExistError(errors.New("not status error")))
	for _, code := range collectionNotExistCodes {
		err := NewStatusError(code, "can't find collection")
		assert.True(t, IsCollectionNotExistError(err))
	}
	assert.True(t, IsCollectionNotExistError(NewCollectionNotExistError("collection not exist")))
	assert.False(t, IsCollectionNotExistError(NewStatusError(commonpb.ErrorCode_BuildIndexError, "")))
}

func TestIsCollectionNotExistErrorV2(t *testing.T) {
	assert.False(t, IsCollectionNotExistErrorV2(nil))
	assert.False(t, IsCollectionNotExistErrorV2(errors.New("not status error")))
	assert.True(t, IsCollectionNotExistErrorV2(NewCollectionNotExistError("collection not exist")))
	assert.False(t, IsCollectionNotExistErrorV2(NewStatusError(commonpb.ErrorCode_BuildIndexError, "")))
}

func TestStatusFromError(t *testing.T) {
	var status *commonpb.Status
	status = StatusFromError(nil)
	assert.Equal(t, commonpb.ErrorCode_Success, status.GetErrorCode())
	status = StatusFromError(errors.New("not status error"))
	assert.Equal(t, commonpb.ErrorCode_UnexpectedError, status.GetErrorCode())
	assert.Equal(t, "not status error", status.GetReason())
	status = StatusFromError(NewCollectionNotExistError("collection not exist"))
	assert.Equal(t, commonpb.ErrorCode_CollectionNotExists, status.GetErrorCode())
}

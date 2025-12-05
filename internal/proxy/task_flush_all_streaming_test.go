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

package proxy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

func TestFlushAllTask_Success(t *testing.T) {
	ctx := context.Background()
	mixCoord := mocks.NewMockMixCoordClient(t)

	task := &flushAllTask{
		baseTask: baseTask{},
		ctx:      ctx,
		mixCoord: mixCoord,
	}

	mixCoord.EXPECT().FlushAll(mock.Anything, mock.Anything).Return(&datapb.FlushAllResponse{
		Status: merr.Success(),
	}, nil)

	err := task.Execute(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, task.result)
}

func TestFlushAllTask_Failed(t *testing.T) {
	ctx := context.Background()
	mixCoord := mocks.NewMockMixCoordClient(t)

	task := &flushAllTask{
		baseTask: baseTask{},
		ctx:      ctx,
		mixCoord: mixCoord,
	}

	mixCoord.EXPECT().FlushAll(mock.Anything, mock.Anything).Return(&datapb.FlushAllResponse{
		Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError, Reason: "test"},
	}, nil)

	err := task.Execute(ctx)
	assert.Error(t, err)
	assert.Nil(t, task.result)
}

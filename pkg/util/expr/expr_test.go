/*
 * Licensed to the LF AI & Data foundation under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package expr

import (
	"context"
	"fmt"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type FooID = int64

func TestExec(t *testing.T) {
	paramtable.Init()
	t.Run("not init", func(t *testing.T) {
		_, err := Exec("1+1", "by-dev")
		assert.Error(t, err)
	})
	Init()
	Register("foo", "hello")
	Register("FuncWithContext", func(ctx context.Context, i FooID) int {
		return int(100 + i)
	})
	mockMessage := &milvuspb.UserEntity{Name: "foo"}
	Register("GetMockMessage", func() proto.Message {
		return mockMessage
	})

	t.Run("empty code", func(t *testing.T) {
		_, err := Exec("", "by-dev")
		assert.Error(t, err)
	})

	t.Run("empty auth", func(t *testing.T) {
		_, err := Exec("1+1", "")
		assert.Error(t, err)
	})

	t.Run("invalid auth", func(t *testing.T) {
		_, err := Exec("1+1", "000")
		assert.Error(t, err)
	})

	t.Run("invalid code", func(t *testing.T) {
		_, err := Exec("1+", "by-dev")
		assert.Error(t, err)
	})

	t.Run("valid code", func(t *testing.T) {
		out, err := Exec("foo", "by-dev")
		assert.NoError(t, err)
		assert.Equal(t, "hello", out)
	})

	t.Run("context function", func(t *testing.T) {
		out, err := Exec("FuncWithContext(100)", "by-dev")
		assert.NoError(t, err)
		assert.Equal(t, "200", out)
	})

	innerSize := func(p any) int {
		message, ok := p.(proto.Message)
		if !ok {
			return int(unsafe.Sizeof(p))
		}
		return proto.Size(message)
	}

	t.Run("size", func(t *testing.T) {
		out, err := Exec("objSize(1)", "by-dev")
		assert.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("%d", innerSize(1)), out)
	})

	t.Run("proto size", func(t *testing.T) {
		out, err := Exec("objSize(GetMockMessage())", "by-dev")
		assert.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("%d", innerSize(mockMessage)), out)
	})
}

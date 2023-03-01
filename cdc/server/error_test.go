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

package server

import (
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
)

func TestClientError(t *testing.T) {
	err := NewClientError("foo")
	assert.Equal(t, "foo", err.Error())
	assert.True(t, errors.Is(err, ClientErr))
	assert.False(t, errors.Is(err, ServerErr))
}

func TestServerError(t *testing.T) {
	fooErr := errors.New("foo")
	err := NewServerError(fooErr)
	assert.Equal(t, "foo", err.Error())
	assert.True(t, errors.Is(err, ServerErr))
	assert.False(t, errors.Is(err, ClientErr))
}

func TestNotFoundError(t *testing.T) {
	err := NewNotFoundError("foo")
	assert.Contains(t, err.Error(), "not found the key")
	assert.Contains(t, err.Error(), "foo")
	assert.True(t, errors.Is(err, NotFoundErr))
	assert.False(t, errors.Is(err, ClientErr))
}

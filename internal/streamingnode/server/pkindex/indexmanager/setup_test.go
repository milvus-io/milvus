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

package indexmanager

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/pkindex/engine"
)

func TestOpenFileLimitShort(t *testing.T) {
	need, short := openFileLimitShort(14095, 4096)
	assert.Equal(t, uint64(14096), need)
	assert.True(t, short)

	_, short = openFileLimitShort(14096, 4096)
	assert.False(t, short)

	need, _ = openFileLimitShort(0, 0)
	assert.Equal(t, uint64(engine.DefaultMaxOpenFiles+openFileReserve), need)
}

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

package message

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIndexMessagesAreExclusive(t *testing.T) {
	assert.True(t, MessageTypeCreateIndex.IsExclusiveRequired())
	assert.True(t, MessageTypeDropIndex.IsExclusiveRequired())
	assert.False(t, MessageTypeAlterIndex.IsExclusiveRequired())
}

func TestIndexMessageHeadersCarryIndexType(t *testing.T) {
	create := &CreateIndexMessageHeader{IndexType: "PRIMARY_KEY"}
	assert.Equal(t, "PRIMARY_KEY", create.GetIndexType())
	drop := &DropIndexMessageHeader{IndexType: "PRIMARY_KEY"}
	assert.Equal(t, "PRIMARY_KEY", drop.GetIndexType())
}

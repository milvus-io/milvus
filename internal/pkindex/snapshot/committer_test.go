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

package snapshot

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestSSTKey fixes the object key format. The manifest service and its
// garbage collection use the same format, so a change breaks both.
func TestSSTKey(t *testing.T) {
	assert.Equal(t, "root/pk_index/100/v0/sst/1001.sst", SSTKey("root/pk_index/100/v0/sst", 1001))
	assert.Equal(t, "base/0.sst", SSTKey("base", 0))
}

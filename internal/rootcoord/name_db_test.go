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

package rootcoord

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNameDbRemoveIfMatched(t *testing.T) {
	names := newNameDb()
	names.insert("db", "old", 100)
	names.insert("db", "reused", 200)
	names.insert("other-db", "old", 300)
	assert.False(t, names.removeIfMatched("missing-db", "old", 100))
	assert.False(t, names.removeIfMatched("db", "missing", 100))
	assert.False(t, names.removeIfMatched("db", "reused", 100))
	assert.True(t, names.removeIfMatched("db", "old", 100))
	assert.False(t, names.removeIfMatched("db", "old", 100))
	assert.Equal(t, map[string]UniqueID{"reused": 200}, names.listCollections("db"))
	assert.Equal(t, map[string]UniqueID{"old": 300}, names.listCollections("other-db"))
}

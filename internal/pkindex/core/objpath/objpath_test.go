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

package objpath

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSSTKey(t *testing.T) {
	base := SSTBasePath("files", 100, "by-dev-rootcoord-dml_0_100v0")
	assert.Equal(t, "files/pk_index/100/by-dev-rootcoord-dml_0_100v0/sst", base)
	key := SSTKey(base, 42)
	assert.Equal(t, base+"/42.sst", key)

	id, ok := ParseSSTKey(base, key)
	assert.True(t, ok)
	assert.Equal(t, int64(42), id)

	for _, bad := range []string{base + "/42.pb", base + "/x.sst", base + "/a/42.sst", "other/42.sst"} {
		_, ok := ParseSSTKey(base, bad)
		assert.False(t, ok, bad)
	}
}

func TestManifestBodyKey(t *testing.T) {
	dir := ManifestDir("files", 100, "by-dev-rootcoord-dml_0_100v0")
	assert.Equal(t, "files/pk_index/100/by-dev-rootcoord-dml_0_100v0/manifest", dir)
	key := ManifestBodyKey(dir, 7, "uuid")
	assert.Equal(t, dir+"/7-uuid.pb", key)

	v, ok := ParseManifestBodyKey(dir, key)
	assert.True(t, ok)
	assert.Equal(t, int64(7), v)

	for _, bad := range []string{dir + "/7.pb", dir + "/7-.pb", dir + "/x-uuid.pb", dir + "/a/7-uuid.pb", dir + "/7-uuid.sst"} {
		_, ok := ParseManifestBodyKey(dir, bad)
		assert.False(t, ok, bad)
	}
}

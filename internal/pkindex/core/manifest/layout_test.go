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

package manifest

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLayoutKeys(t *testing.T) {
	l := Layout{RootPath: "files"}
	assert.Equal(t, "pkindex-meta/manifest/7/by-dev-dml_0_7v0", PointerKey(7, "by-dev-dml_0_7v0"))
	assert.Equal(t, "files/pk_index/7/by-dev-dml_0_7v0/sst", l.SSTBasePath(7, "by-dev-dml_0_7v0"))
	assert.Equal(t, "files/pk_index/7/by-dev-dml_0_7v0/sst/42.sst", SSTKey("files/pk_index/7/by-dev-dml_0_7v0/sst", 42))
	assert.Equal(t, "files/pk_index/7/by-dev-dml_0_7v0/manifest/3-abc.pb", l.ManifestBodyKey(7, "by-dev-dml_0_7v0", 3, "abc"))
}

func TestParsePointerKey(t *testing.T) {
	coll, vch, ok := ParsePointerKey("pkindex-meta/manifest/7/by-dev-dml_0_7v0")
	assert.True(t, ok)
	assert.EqualValues(t, 7, coll)
	assert.Equal(t, "by-dev-dml_0_7v0", vch)
	for _, bad := range []string{
		"pkindex-meta/manifest/x/v",
		"pkindex-meta/manifest/7/a/b",
		"pkindex-meta/manifest/7/",
		"pkindex-meta/manifest/7",
	} {
		_, _, ok = ParsePointerKey(bad)
		assert.False(t, ok, bad)
	}
}

func TestParseSSTKey(t *testing.T) {
	base := "files/pk_index/7/v/sst"
	id, ok := ParseSSTKey(base, base+"/42.sst")
	assert.True(t, ok)
	assert.EqualValues(t, 42, id)
	for _, bad := range []string{base + "/42.tmp", base + "/x.sst", "other/42.sst", base + "/sub/42.sst"} {
		_, ok := ParseSSTKey(base, bad)
		assert.False(t, ok, bad)
	}
}

func TestParseManifestBodyKey(t *testing.T) {
	l := Layout{RootPath: "files"}
	dir := l.ManifestDir(7, "v")
	v, ok := ParseManifestBodyKey(dir, dir+"/12-abc.pb")
	assert.True(t, ok)
	assert.EqualValues(t, 12, v)
	v, ok = ParseManifestBodyKey(dir, dir+"/13-0b2f6c1e-8d4a-4f7e-9c3b-5a1d2e3f4a5b.pb")
	assert.True(t, ok)
	assert.EqualValues(t, 13, v)
	for _, bad := range []string{dir + "/12.pb", dir + "/x-abc.pb", dir + "/12-abc.txt"} {
		_, ok := ParseManifestBodyKey(dir, bad)
		assert.False(t, ok, bad)
	}
}

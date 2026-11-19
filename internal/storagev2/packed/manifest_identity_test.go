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

//go:build dynamic

package packed

import (
	"maps"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPaimonIdentityPreservesOpaqueDescriptor(t *testing.T) {
	first := Fragment{FilePath: "data.parquet", StartRow: 0, EndRow: 10, Properties: map[string]string{
		"metadata":  `{"version":1,"read_path":"direct-file","snapshot_id":1,"future":{"id":9007199254740992}}`,
		"file_size": "123",
	}}
	original := maps.Clone(first.Properties)
	identity := FragmentIdentity(first)
	require.Equal(t, original, first.Properties, "identity calculation must not mutate the shared descriptor")

	reordered := first
	reordered.Properties = maps.Clone(first.Properties)
	reordered.Properties["metadata"] = `{"future":{"id":9007199254740992},"snapshot_id":2,"read_path":"direct-file","version":1}`
	require.Equal(t, identity, FragmentIdentity(reordered))

	changed := first
	changed.Properties = maps.Clone(first.Properties)
	changed.Properties["metadata"] = `{"version":1,"read_path":"direct-file","snapshot_id":2,"future":{"id":9007199254740993}}`
	require.NotEqual(t, identity, FragmentIdentity(changed), "unknown integer fields must not lose precision")
}

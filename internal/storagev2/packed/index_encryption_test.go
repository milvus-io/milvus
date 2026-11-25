// Copyright 2026 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package packed

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

func TestManifestIndexEncryptionProperties(t *testing.T) {
	for _, state := range []indexpb.IndexEncryptionState{indexpb.IndexEncryptionState_INDEX_ENCRYPTION_UNKNOWN, indexpb.IndexEncryptionState_INDEX_ENCRYPTION_PLAIN, indexpb.IndexEncryptionState_INDEX_ENCRYPTION_ENCRYPTED} {
		entry := ManifestIndexInfo{Properties: IndexEncryptionProperties(state)}
		require.Equal(t, state, entry.EncryptionState())
	}
	require.Equal(t, indexpb.IndexEncryptionState_INDEX_ENCRYPTION_UNKNOWN, (ManifestIndexInfo{}).EncryptionState())
	require.Equal(t, indexpb.IndexEncryptionState_INDEX_ENCRYPTION_UNKNOWN, (ManifestIndexInfo{Properties: map[string]string{"milvus.index.encryption_state": "future"}}).EncryptionState())
}

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

package textindex

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestTextFstReaderHeapAndMmap(t *testing.T) {
	artifact, err := BuildTextFst([][]byte{[]byte("fuzzy"), []byte("milvus")})
	require.NoError(t, err)

	heap, err := LoadTextFstBytes(artifact.Data)
	require.NoError(t, err)
	defer heap.Close()
	assert.False(t, heap.IsMemoryMapped())
	assert.Equal(t, artifact.TermCount, heap.TermCount())
	assert.EqualValues(t, len(artifact.Data), heap.DataSize())

	filePath := filepath.Join(t.TempDir(), "terms.fst")
	require.NoError(t, os.WriteFile(filePath, artifact.Data, 0o600))
	mapped, err := LoadTextFstFile(filePath, true)
	require.NoError(t, err)
	defer mapped.Close()
	assert.True(t, mapped.IsMemoryMapped())
	assert.Equal(t, artifact.TermCount, mapped.TermCount())
}

func TestTextFstReaderRejectsCorruptChecksum(t *testing.T) {
	artifact, err := BuildTextFst([][]byte{[]byte("fuzzy")})
	require.NoError(t, err)
	corrupt := append([]byte(nil), artifact.Data...)
	corrupt[len(corrupt)-1] ^= 0xff

	_, err = LoadTextFstBytes(corrupt)
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	assert.False(t, merr.IsRetryableErr(err))
}

func TestTextFstReaderPreservesMmapFailureClassification(t *testing.T) {
	_, err := LoadTextFstFile(filepath.Join(t.TempDir(), "missing.fst"), true)
	require.Error(t, err)
	assert.NotErrorIs(t, err, merr.ErrDataIntegrity)
	assert.True(t, merr.IsRetryableErr(err))
}

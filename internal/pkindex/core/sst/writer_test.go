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

package sst

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/pkindex/core/codec"
)

// The ID names the file, here and in object storage, which is what lets an
// upload retry under the same ID stay idempotent.
func TestWriterNamesFileByID(t *testing.T) {
	dir := t.TempDir()
	info, path := writeFixtureID(t, dir, ID(987654321), 10)

	assert.Equal(t, ID(987654321), info.ID)
	assert.Equal(t, filepath.Join(dir, "987654321.sst"), path)
	_, err := os.Stat(path)
	require.NoError(t, err)

	// nothing else is left behind, so a crash cannot leave a short file under
	// the name the ID promises
	ents, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Len(t, ents, 1)
}

func TestEmptyWriter(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(dir, fixtureID)
	require.NoError(t, err)
	info, err := w.Close()
	require.NoError(t, err)
	assert.Zero(t, info.NumEntries)
	assert.Nil(t, info.MinKey)
	assert.Nil(t, info.MaxKey)
	assert.Equal(t, fixtureID, info.ID)
}

func TestAbort(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(dir, fixtureID)
	require.NoError(t, err)
	require.NoError(t, w.Add(codec.EncodeInt64PK(1), codec.EncodePKEntry(codec.PKEntry{SegmentID: 1})))
	w.Abort()

	ents, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Empty(t, ents, "abort must leave nothing behind")
}

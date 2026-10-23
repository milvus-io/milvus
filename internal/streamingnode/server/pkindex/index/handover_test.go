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

package index

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pkroot "github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/internal/pkindex/engine"
	"github.com/milvus-io/milvus/internal/pkindex/sst"
)

func TestLinkCommittedReusesExistingLink(t *testing.T) {
	src := filepath.Join(t.TempDir(), "staged.sst")
	require.NoError(t, os.WriteFile(src, []byte("x"), 0o600))
	dir := filepath.Join(t.TempDir(), "committed")
	s := engine.StagedTable{Info: sst.Info{ID: 42}, Path: src}

	p1, err := linkCommitted(dir, s)
	require.NoError(t, err)
	assert.Equal(t, filepath.Join(dir, "42.sst"), p1)
	p2, err := linkCommitted(dir, s)
	require.NoError(t, err)
	assert.Equal(t, p1, p2)

	_, err = linkCommitted(dir, engine.StagedTable{Info: sst.Info{ID: 43}, Path: filepath.Join(t.TempDir(), "missing")})
	assert.True(t, errors.Is(err, pkroot.ErrIO))
}

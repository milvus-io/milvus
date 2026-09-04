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

package querynodev2

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/util/pathutil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestCleanupOrphanedTextTermFiles(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().LocalStorageCfg.Path.Key, t.TempDir())
	t.Cleanup(func() {
		paramtable.Get().Reset(paramtable.Get().LocalStorageCfg.Path.Key)
	})

	const nodeID = int64(101)
	cacheDir := pathutil.GetPath(pathutil.TextLogV2Path, nodeID)
	require.NoError(t, os.MkdirAll(cacheDir, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(cacheDir, "orphan.fst"), []byte("orphan"), 0o600))

	cleanupOrphanedTextTermFiles(context.Background(), nodeID)
	_, err := os.Stat(cacheDir)
	assert.ErrorIs(t, err, os.ErrNotExist)

	// An already-clean node directory is a no-op.
	cleanupOrphanedTextTermFiles(context.Background(), nodeID)
}

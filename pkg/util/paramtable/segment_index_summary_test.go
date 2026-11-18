// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package paramtable

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/config"
)

func TestSegmentIndexSummariesConfig(t *testing.T) {
	var params ComponentParam
	params.Init(NewBaseTable(SkipRemote(true)))
	require.False(t, params.DataCoordCfg.SegmentIndexSummariesEnabled.GetAsBool())
	params.Save(params.DataCoordCfg.SegmentIndexSummariesEnabled.Key, "true")
	require.True(t, params.DataCoordCfg.SegmentIndexSummariesEnabled.GetAsBool())
	require.True(t, params.DataCoordCfg.WriteSegmentIndexToManifest.GetAsBool())
}

func TestSegmentIndexSummariesPinStorageV3AcrossRefresh(t *testing.T) {
	for _, mode := range []string{"false", "true"} {
		t.Run(mode, func(t *testing.T) {
			base := &BaseTable{mgr: config.NewManager()}
			defer base.mgr.Close()
			require.NoError(t, base.Save("localStorage.path", t.TempDir()))
			filename := filepath.Join(t.TempDir(), "milvus.yaml")
			writeConfig := func(summaryMode, useLoonFFI string) {
				t.Helper()
				require.NoError(t, os.WriteFile(filename, []byte(
					"dataCoord:\n  index:\n    segmentIndexSummariesEnabled: "+summaryMode+
						"\ncommon:\n  storage:\n    useLoonFFI: "+useLoonFFI+"\n"), 0o600))
			}
			writeConfig(mode, "true")
			source := config.NewFileSource(&config.FileInfo{Files: []string{filename}})
			require.NoError(t, base.mgr.AddSource(source))
			var params ComponentParam
			params.Init(base)
			require.True(t, params.CommonCfg.UseLoonFFI.GetAsBool())

			writeConfig(strconv.FormatBool(mode != "true"), "false")
			_, err := source.GetConfigurations()
			require.NoError(t, err)
			require.Equal(t, mode == "true", params.DataCoordCfg.SegmentIndexSummariesEnabled.GetAsBool())
			require.Equal(t, mode == "true", params.CommonCfg.UseLoonFFI.GetAsBool())
		})
	}
}

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

package importutilv2

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestImportSourceValidationBoundaries(t *testing.T) {
	files := []*internalpb.ImportFile{{Paths: []string{"data.json"}}}
	for _, tc := range []struct {
		name       string
		options    Options
		files      []*internalpb.ImportFile
		requestErr error
		planErr    error
	}{
		{
			name:  "cluster storage retains absolute paths",
			files: []*internalpb.ImportFile{{Paths: []string{"/milvus/data.json"}}},
		},
		{
			name: "external literal keys", options: externalStorageOptions(),
			files: []*internalpb.ImportFile{{Paths: []string{"a/../data.json", "a//vector.npy"}}},
		},
		{
			name: "incomplete external credentials", options: externalStorageOptions()[:3], files: files,
			requestErr: merr.ErrParameterInvalid, planErr: merr.ErrParameterInvalid,
		},
		{
			name: "nil option rejected before incomplete credentials", files: files,
			options:    append(externalStorageOptions()[:3], nil),
			requestErr: merr.ErrImportFailed, planErr: merr.ErrImportFailed,
		},
		{
			name: "external source mixed with backup", files: files,
			options:    append(externalStorageOptions(), &commonpb.KeyValuePair{Key: BackupFlag, Value: "true"}),
			requestErr: merr.ErrParameterInvalid, planErr: merr.ErrParameterInvalid,
		},
		{
			name: "invalid path in later file", options: externalStorageOptions(),
			files:   []*internalpb.ImportFile{files[0], {Paths: []string{"pk.npy", "s3://source/vector.npy"}}},
			planErr: merr.ErrParameterInvalid,
		},
		{
			name: "external source cannot bypass snapshot descriptor validation", options: externalStorageOptions(),
			files: []*internalpb.ImportFile{{
				Paths:          []string{"data.json"},
				SnapshotSource: &internalpb.SnapshotImportSource{Version: 1},
			}},
			planErr: merr.ErrServiceInternal,
		},
		{
			name: "snapshot routing options are internal only",
			options: Options{
				{Key: SourceType, Value: SourceTypeSnapshot},
				{Key: BackupFlag, Value: "true"},
				{Key: SnapshotSourceURI, Value: "s3://source/metadata.json"},
				{Key: SnapshotLayout, Value: "referenced"},
			},
			files:      []*internalpb.ImportFile{{SnapshotSource: &internalpb.SnapshotImportSource{Version: SnapshotPreparationVersion}}},
			requestErr: merr.ErrImportFailed,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.ErrorIs(t, ValidateImportSourceRequest(tc.options), tc.requestErr)
			require.ErrorIs(t, ValidateImportFiles(tc.files, tc.options), tc.planErr)
			require.ErrorIs(t, ValidateImportPlan(tc.files, tc.options, nil), tc.planErr)
		})
	}
}

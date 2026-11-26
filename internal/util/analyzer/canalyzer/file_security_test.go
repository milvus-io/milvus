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

package canalyzer

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/milvus-io/milvus/internal/util/pathutil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestAnalyzerFileSecurity(t *testing.T) {
	paramtable.Init()
	root := t.TempDir()
	outside := t.TempDir()
	cfg := &paramtable.Get().FunctionCfg
	previousRoot := cfg.LocalResourcePath.SwapTempValue(root)
	previousLimit := cfg.AnalyzerMaxDictSize.SwapTempValue("128")
	require.NoError(t, updateParams())
	t.Cleanup(func() {
		cfg.LocalResourcePath.SwapTempValue(previousRoot)
		cfg.AnalyzerMaxDictSize.SwapTempValue(previousLimit)
		require.NoError(t, updateParams())
	})
	for name, data := range map[string]string{
		"valid": "the\n", "synonym": "FILE_SECRET_MARKER=>a=>b", "jieba": "FILE_SECRET_MARKER invalid_frequency",
		"large": strings.Repeat("x", 129), "invalid_utf8": "FILE_SECRET_MARKER\xff",
	} {
		require.NoError(t, os.WriteFile(filepath.Join(root, name), []byte(data), 0o600))
	}
	require.NoError(t, os.WriteFile(filepath.Join(outside, "secret"), []byte("FILE_SECRET_MARKER"), 0o600))
	require.NoError(t, os.Symlink(outside, filepath.Join(root, "link")))
	require.NoError(t, unix.Mkfifo(filepath.Join(root, "fifo"), 0o600))
	for _, tc := range []struct {
		name, kind, path string
		allowed          bool
	}{
		{"relative", "stop", "valid", true},
		{"absolute", "stop", filepath.Join(root, "valid"), true},
		{"outside_exists", "stop", filepath.Join(outside, "secret"), false},
		{"outside_absent", "stop", filepath.Join(outside, "absent"), false},
		{"traversal", "stop", "../secret", false},
		{"symlink", "stop", "link/secret", false},
		{"fifo", "stop", "fifo", false},
		{"oversized", "stop", "large", false},
		{"invalid_utf8", "stop", "invalid_utf8", false},
		{"synonym_error", "synonym", "synonym", false},
		{"jieba_error", "jieba", "jieba", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			file := map[string]any{"type": "local", "path": tc.path}
			params := map[string]any{"tokenizer": "standard"}
			switch tc.kind {
			case "jieba":
				params["tokenizer"] = map[string]any{"type": "jieba", "dict": []string{}, "extra_dict_file": file}
			case "synonym":
				params["filter"] = []any{map[string]any{"type": "synonym", "synonyms_file": file}}
			default:
				params["filter"] = []any{map[string]any{"type": "stop", "stop_words_file": file}}
			}
			filters, _ := params["filter"].([]any)
			params["filter"] = append(filters, map[string]any{"type": "stop", "stop_words": []string{"REQUEST_SECRET_MARKER"}})
			data, err := json.Marshal(params)
			require.NoError(t, err)
			created, createErr := NewAnalyzer(string(data), "")
			if created != nil {
				created.Destroy()
			}
			_, validateErr := ValidateAnalyzer(string(data), "")
			for _, err := range []error{createErr, validateErr} {
				if tc.allowed {
					require.NoError(t, err)
				} else {
					require.Error(t, err)
					require.NotContains(t, err.Error(), "FILE_SECRET_MARKER")
					require.NotContains(t, err.Error(), "REQUEST_SECRET_MARKER")
				}
			}
		})
	}
}

func TestAnalyzerResourceNamespaceStaysUnderCache(t *testing.T) {
	paramtable.Init()
	setting := &paramtable.Get().LocalStorageCfg.Path
	old := setting.SwapTempValue(t.TempDir())
	require.NoError(t, updateParams())
	t.Cleanup(func() {
		setting.SwapTempValue(old)
		require.NoError(t, updateParams())
	})
	dir := filepath.Join(pathutil.GetPath(pathutil.FileResourcePath, paramtable.GetNodeID()), "namespace", "7")
	require.NoError(t, os.MkdirAll(dir, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "stop.txt"), []byte("the"), 0o600))
	params := `{"tokenizer":"standard","filter":[{"type":"stop","stop_words_file":{"type":"remote","resource_name":"dict","file_name":"stop.txt"}}]}`
	for _, namespace := range []string{"namespace", "/namespace"} {
		extra, err := json.Marshal(map[string]any{"storage_name": namespace, "resource_map": map[string]int64{"dict": 7}})
		require.NoError(t, err)
		_, err = ValidateAnalyzer(params, string(extra))
		require.NoError(t, err)
	}
	for _, namespace := range []string{"../namespace", "/../namespace"} {
		extra, err := json.Marshal(map[string]any{"storage_name": namespace, "resource_map": map[string]int64{"dict": 7}})
		require.NoError(t, err)
		_, err = ValidateAnalyzer(params, string(extra))
		require.ErrorContains(t, err, "invalid file resource namespace")
	}
}

func TestAnalyzerRestoreIgnoresAdmissionPolicyChanges(t *testing.T) {
	paramtable.Init()
	cfg := &paramtable.Get().FunctionCfg
	root := t.TempDir()
	file := filepath.Join(root, "stop.txt")
	require.NoError(t, os.WriteFile(file, []byte("the\n"), 0o600))
	oldRoot := cfg.LocalResourcePath.SwapTempValue(root)
	oldLimit := cfg.AnalyzerMaxDictSize.SwapTempValue("128")
	t.Cleanup(func() {
		cfg.LocalResourcePath.SwapTempValue(oldRoot)
		cfg.AnalyzerMaxDictSize.SwapTempValue(oldLimit)
		require.NoError(t, updateParams())
	})
	data, err := json.Marshal(map[string]any{"tokenizer": "standard", "filter": []any{map[string]any{
		"type": "stop", "stop_words_file": map[string]any{"type": "local", "path": file},
	}}})
	require.NoError(t, err)
	require.NoError(t, updateParams())
	_, err = ValidateAnalyzer(string(data), "")
	require.NoError(t, err)
	cfg.LocalResourcePath.SwapTempValue(t.TempDir())
	cfg.AnalyzerMaxDictSize.SwapTempValue("1")
	require.NoError(t, updateParams())
	restored, err := RestoreAnalyzer(string(data), "")
	require.NoError(t, err)
	defer restored.Destroy()
	stream := restored.NewTokenStream("the cat")
	// TokenStream creation is implemented by the same native tokenizer used after recovery.
	require.True(t, stream.Advance())
	require.Equal(t, "cat", stream.Token())
	require.False(t, stream.Advance())
	stream.Destroy()
	_, err = NewAnalyzer(string(data), "")
	require.Error(t, err)
	_, err = ValidateAnalyzer(string(data), `{"file_access_admitted":true}`)
	require.Error(t, err, "validation cannot inherit a runtime exemption")
}

func TestAnalyzerRestorePreservesResourceBindingOrigin(t *testing.T) {
	paramtable.Init()
	setting := &paramtable.Get().LocalStorageCfg.Path
	previous := setting.SwapTempValue(t.TempDir())
	require.NoError(t, updateParams())
	t.Cleanup(func() {
		setting.SwapTempValue(previous)
		require.NoError(t, updateParams())
		require.NoError(t, UpdateGlobalResourceInfo(map[string]int64{}))
	})
	const id int64 = 9007199254740993
	dir := filepath.Join(pathutil.GetPath(pathutil.FileResourcePath, paramtable.GetNodeID()), "9007199254740993")
	require.NoError(t, os.MkdirAll(dir, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "stop.txt"), []byte("the\n"), 0600))
	require.NoError(t, UpdateGlobalResourceInfo(map[string]int64{"dictionary": id}))
	params := `{"tokenizer":"standard","filter":[{"type":"stop","stop_words_file":{"type":"remote","resource_name":"dictionary","file_name":"stop.txt"}}]}`
	for _, extra := range []string{"", `{"resource_map":{"dictionary":9007199254740993}}`} {
		restored, err := RestoreAnalyzer(params, extra)
		require.NoError(t, err)
		restored.Destroy()
	}
	// Linux permits backslashes in basenames that predate admission restrictions.
	require.NoError(t, os.WriteFile(filepath.Join(dir, `legacy\stop.txt`), []byte("the\n"), 0600))
	legacy := strings.Replace(params, `"file_name":"stop.txt"`, `"file_name":"legacy\\stop.txt"`, 1)
	restored, err := RestoreAnalyzer(legacy, "")
	require.NoError(t, err)
	restored.Destroy()
	_, err = ValidateAnalyzer(legacy, "")
	require.Error(t, err)

	_, err = RestoreAnalyzer(params, `{"resource_map":{}}`)
	require.Error(t, err, "explicit empty bindings must not fall back to global names")
}

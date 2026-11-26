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
			params := map[string]any{"tokenizer": "standard", "char_filter": []any{map[string]any{"type": "mapping", "mappings": []string{"REQUEST_SECRET_MARKER => safe"}}}}
			switch tc.kind {
			case "jieba":
				params["tokenizer"] = map[string]any{"type": "jieba", "dict": []string{}, "extra_dict_file": file}
			case "synonym":
				params["filter"] = []any{map[string]any{"type": "synonym", "synonyms_file": file}}
			default:
				params["filter"] = []any{map[string]any{"type": "stop", "stop_words_file": file}}
			}
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

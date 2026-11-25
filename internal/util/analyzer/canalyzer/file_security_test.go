package canalyzer

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestAnalyzerRejectsTLSFilePaths(t *testing.T) {
	for _, key := range []string{"ca_cert", "client_cert", "client_key"} {
		data, err := json.Marshal(map[string]any{"tokenizer": map[string]any{
			"type": "grpc", "endpoint": "https://127.0.0.1:1",
			"tls": map[string]any{key: "/REQUEST_SECRET_MARKER"},
		}})
		require.NoError(t, err)
		_, err = NewAnalyzer(string(data), "")
		require.ErrorContains(t, err, "TLS file paths cannot be supplied")
		require.NotContains(t, err.Error(), "REQUEST_SECRET_MARKER")
		_, err = ValidateAnalyzer(string(data), "")
		require.ErrorContains(t, err, "TLS file paths cannot be supplied")
		require.NotContains(t, err.Error(), "REQUEST_SECRET_MARKER")
	}
}

package milvusclient

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildTLSConfig(t *testing.T) {
	t.Run("valid CA only", func(t *testing.T) {
		caPem := filepath.Join("..", "..", "configs", "cert", "ca.pem")
		if _, err := os.Stat(caPem); os.IsNotExist(err) {
			t.Skip("test cert fixtures not found")
		}
		tlsConfig, err := BuildTLSConfig(caPem, "", "")
		require.NoError(t, err)
		assert.NotNil(t, tlsConfig.RootCAs)
		assert.Empty(t, tlsConfig.Certificates)
	})

	t.Run("valid mTLS", func(t *testing.T) {
		caPem := filepath.Join("..", "..", "configs", "cert", "ca.pem")
		clientPem := filepath.Join("..", "..", "configs", "cert", "client.pem")
		clientKey := filepath.Join("..", "..", "configs", "cert", "client.key")
		for _, f := range []string{caPem, clientPem, clientKey} {
			if _, err := os.Stat(f); os.IsNotExist(err) {
				t.Skip("test cert fixtures not found")
			}
		}
		tlsConfig, err := BuildTLSConfig(caPem, clientPem, clientKey)
		require.NoError(t, err)
		assert.NotNil(t, tlsConfig.RootCAs)
		assert.Len(t, tlsConfig.Certificates, 1)
	})

	t.Run("nonexistent CA", func(t *testing.T) {
		_, err := BuildTLSConfig("/nonexistent/ca.pem", "", "")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to read CA cert")
	})

	t.Run("invalid CA content", func(t *testing.T) {
		tmpDir := t.TempDir()
		badCA := filepath.Join(tmpDir, "bad-ca.pem")
		os.WriteFile(badCA, []byte("not a certificate"), 0o644)
		_, err := BuildTLSConfig(badCA, "", "")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to parse CA cert")
	})

	t.Run("nonexistent client cert", func(t *testing.T) {
		caPem := filepath.Join("..", "..", "configs", "cert", "ca.pem")
		if _, err := os.Stat(caPem); os.IsNotExist(err) {
			t.Skip("test cert fixtures not found")
		}
		_, err := BuildTLSConfig(caPem, "/nonexistent/client.pem", "/nonexistent/client.key")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to load client cert")
	})
}

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

package milvusclient

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// generateTestCerts creates a self-signed CA and a client cert signed by it.
// Returns paths to ca.pem, client.pem, client.key in a temp directory.
func generateTestCerts(t *testing.T) (caPem, clientPem, clientKey string) {
	t.Helper()
	dir := t.TempDir()

	// Generate CA key and self-signed cert.
	caPriv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Test CA"},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
	}
	caCertDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caPriv.PublicKey, caPriv)
	require.NoError(t, err)

	caPem = filepath.Join(dir, "ca.pem")
	require.NoError(t, os.WriteFile(caPem, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caCertDER}), 0o600))

	// Generate client key and cert signed by CA.
	clientPriv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	clientTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "Test Client"},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	clientCertDER, err := x509.CreateCertificate(rand.Reader, clientTemplate, caTemplate, &clientPriv.PublicKey, caPriv)
	require.NoError(t, err)

	clientPem = filepath.Join(dir, "client.pem")
	require.NoError(t, os.WriteFile(clientPem, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: clientCertDER}), 0o600))

	clientKeyBytes, err := x509.MarshalECPrivateKey(clientPriv)
	require.NoError(t, err)
	clientKey = filepath.Join(dir, "client.key")
	require.NoError(t, os.WriteFile(clientKey, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: clientKeyBytes}), 0o600))

	return caPem, clientPem, clientKey
}

func TestBuildTLSConfig(t *testing.T) {
	t.Run("valid CA only", func(t *testing.T) {
		caPem, _, _ := generateTestCerts(t)
		tlsConfig, err := BuildTLSConfig(caPem, "", "")
		require.NoError(t, err)
		assert.NotNil(t, tlsConfig.RootCAs)
		assert.Empty(t, tlsConfig.Certificates)
	})

	t.Run("valid mTLS", func(t *testing.T) {
		caPem, clientPem, clientKey := generateTestCerts(t)
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
		os.WriteFile(badCA, []byte("not a certificate"), 0o600)
		_, err := BuildTLSConfig(badCA, "", "")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to parse CA cert")
	})

	t.Run("nonexistent client cert", func(t *testing.T) {
		caPem, _, _ := generateTestCerts(t)
		_, err := BuildTLSConfig(caPem, "/nonexistent/client.pem", "/nonexistent/client.key")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to load client cert")
	})

	t.Run("partial client config errors", func(t *testing.T) {
		caPem, clientPem, _ := generateTestCerts(t)

		// Only clientPemPath set.
		_, err := BuildTLSConfig(caPem, clientPem, "")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "both clientPemPath and clientKeyPath must be set")

		// Only clientKeyPath set.
		_, err = BuildTLSConfig(caPem, "", "/some/client.key")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "both clientPemPath and clientKeyPath must be set")
	})
}

func TestWithTLSConfig(t *testing.T) {
	t.Run("sets tlsConfig and enables TLS auth", func(t *testing.T) {
		caPem, clientPem, clientKey := generateTestCerts(t)
		tlsConfig, err := BuildTLSConfig(caPem, clientPem, clientKey)
		require.NoError(t, err)

		config := &ClientConfig{
			Address: "localhost:19530",
		}
		assert.False(t, config.EnableTLSAuth)
		assert.Nil(t, config.tlsConfig)

		result := config.WithTLSConfig(tlsConfig)

		assert.True(t, config.EnableTLSAuth)
		assert.NotNil(t, config.tlsConfig)
		assert.Same(t, config, result) // Returns self for chaining
	})

	t.Run("chained with other config", func(t *testing.T) {
		caPem, _, _ := generateTestCerts(t)
		tlsConfig, err := BuildTLSConfig(caPem, "", "")
		require.NoError(t, err)

		config := (&ClientConfig{
			Address:  "localhost:19530",
			Username: "root",
			Password: "milvus",
		}).WithTLSConfig(tlsConfig)

		assert.Equal(t, "localhost:19530", config.Address)
		assert.Equal(t, "root", config.Username)
		assert.Equal(t, "milvus", config.Password)
		assert.True(t, config.EnableTLSAuth)
		assert.NotNil(t, config.tlsConfig)
	})
}

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

// generateTestCA creates a self-signed CA certificate and returns the cert/key PEM bytes.
func generateTestCA(t *testing.T) (caPEM []byte, caKeyPEM []byte, caCert *x509.Certificate, caKey *ecdsa.PrivateKey) {
	t.Helper()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"Test CA"},
		},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	require.NoError(t, err)

	caCert, err = x509.ParseCertificate(caDER)
	require.NoError(t, err)

	caPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})

	caKeyBytes, err := x509.MarshalECPrivateKey(caKey)
	require.NoError(t, err)
	caKeyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: caKeyBytes})

	return caPEM, caKeyPEM, caCert, caKey
}

// generateTestClientCert creates a client certificate signed by the given CA.
func generateTestClientCert(t *testing.T, caCert *x509.Certificate, caKey *ecdsa.PrivateKey) (certPEM []byte, keyPEM []byte) {
	t.Helper()

	clientKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	clientTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject: pkix.Name{
			Organization: []string{"Test Client"},
		},
		NotBefore: time.Now().Add(-time.Hour),
		NotAfter:  time.Now().Add(24 * time.Hour),
		KeyUsage:  x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{
			x509.ExtKeyUsageClientAuth,
		},
	}

	clientDER, err := x509.CreateCertificate(rand.Reader, clientTemplate, caCert, &clientKey.PublicKey, caKey)
	require.NoError(t, err)

	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: clientDER})

	clientKeyBytes, err := x509.MarshalECPrivateKey(clientKey)
	require.NoError(t, err)
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: clientKeyBytes})

	return certPEM, keyPEM
}

// writeTempFile writes data to a file in dir and returns the path.
func writeTempFile(t *testing.T, dir, name string, data []byte) string {
	t.Helper()
	p := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(p, data, 0o600))
	return p
}

func TestBuildTLSConfig(t *testing.T) {
	// Generate a CA and a client cert signed by that CA.
	caPEM, _, caCert, caKey := generateTestCA(t)
	clientCertPEM, clientKeyPEM := generateTestClientCert(t, caCert, caKey)

	// Generate a second, independent CA for the mismatch test.
	_, _, otherCACert, otherCAKey := generateTestCA(t)
	otherClientCertPEM, _ := generateTestClientCert(t, otherCACert, otherCAKey)

	// Generate an independent key (not matching the client cert).
	independentKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	independentKeyBytes, err := x509.MarshalECPrivateKey(independentKey)
	require.NoError(t, err)
	independentKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: independentKeyBytes})

	t.Run("full_mTLS", func(t *testing.T) {
		dir := t.TempDir()
		caPath := writeTempFile(t, dir, "ca.pem", caPEM)
		certPath := writeTempFile(t, dir, "client.pem", clientCertPEM)
		keyPath := writeTempFile(t, dir, "client.key", clientKeyPEM)

		tlsCfg, err := BuildTLSConfig(caPath, certPath, keyPath)
		require.NoError(t, err)
		assert.NotNil(t, tlsCfg.RootCAs)
		assert.Len(t, tlsCfg.Certificates, 1)
	})

	t.Run("one_way_TLS_ca_only", func(t *testing.T) {
		dir := t.TempDir()
		caPath := writeTempFile(t, dir, "ca.pem", caPEM)

		tlsCfg, err := BuildTLSConfig(caPath, "", "")
		require.NoError(t, err)
		assert.NotNil(t, tlsCfg.RootCAs)
		assert.Empty(t, tlsCfg.Certificates)
	})

	t.Run("ca_file_not_found", func(t *testing.T) {
		_, err := BuildTLSConfig("/nonexistent/ca.pem", "", "")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to read CA cert")
	})

	t.Run("ca_file_invalid_pem", func(t *testing.T) {
		dir := t.TempDir()
		caPath := writeTempFile(t, dir, "bad-ca.pem", []byte("not a valid PEM"))

		_, err := BuildTLSConfig(caPath, "", "")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to parse CA cert")
	})

	t.Run("client_cert_not_found", func(t *testing.T) {
		dir := t.TempDir()
		caPath := writeTempFile(t, dir, "ca.pem", caPEM)
		keyPath := writeTempFile(t, dir, "client.key", clientKeyPEM)

		_, err := BuildTLSConfig(caPath, "/nonexistent/client.pem", keyPath)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to load client cert")
	})

	t.Run("client_key_not_found", func(t *testing.T) {
		dir := t.TempDir()
		caPath := writeTempFile(t, dir, "ca.pem", caPEM)
		certPath := writeTempFile(t, dir, "client.pem", clientCertPEM)

		_, err := BuildTLSConfig(caPath, certPath, "/nonexistent/client.key")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to load client cert")
	})

	t.Run("client_cert_key_mismatch", func(t *testing.T) {
		dir := t.TempDir()
		caPath := writeTempFile(t, dir, "ca.pem", caPEM)
		// Use a cert signed by a different CA paired with an independent key — the key
		// does not correspond to the certificate's public key, so LoadX509KeyPair fails.
		certPath := writeTempFile(t, dir, "other-client.pem", otherClientCertPEM)
		keyPath := writeTempFile(t, dir, "independent.key", independentKeyPEM)

		_, err := BuildTLSConfig(caPath, certPath, keyPath)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to load client cert")
	})
}

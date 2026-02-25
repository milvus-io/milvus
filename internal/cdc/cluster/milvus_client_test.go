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

package cluster

import (
	"context"
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

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
)

// generateTestCerts creates a self-signed CA certificate and a client certificate
// signed by the CA. It writes the PEM files to the given directory and returns
// paths to (caCert, clientCert, clientKey).
func generateTestCerts(t *testing.T, dir string) (string, string, string) {
	t.Helper()

	// Generate CA key
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	// Create CA certificate template
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

	// Self-sign the CA certificate
	caCertDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	require.NoError(t, err)

	// Write CA cert PEM
	caCertPath := filepath.Join(dir, "ca.pem")
	err = os.WriteFile(caCertPath, pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: caCertDER,
	}), 0o600)
	require.NoError(t, err)

	// Generate client key
	clientKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	// Create client certificate template
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

	// Sign client cert with CA
	clientCertDER, err := x509.CreateCertificate(rand.Reader, clientTemplate, caTemplate, &clientKey.PublicKey, caKey)
	require.NoError(t, err)

	// Write client cert PEM
	clientCertPath := filepath.Join(dir, "client.pem")
	err = os.WriteFile(clientCertPath, pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: clientCertDER,
	}), 0o600)
	require.NoError(t, err)

	// Marshal and write client key PEM
	clientKeyDER, err := x509.MarshalECPrivateKey(clientKey)
	require.NoError(t, err)

	clientKeyPath := filepath.Join(dir, "client.key")
	err = os.WriteFile(clientKeyPath, pem.EncodeToMemory(&pem.Block{
		Type:  "EC PRIVATE KEY",
		Bytes: clientKeyDER,
	}), 0o600)
	require.NoError(t, err)

	return caCertPath, clientCertPath, clientKeyPath
}

func TestNewMilvusClient_TLSValidation(t *testing.T) {
	t.Run("no_tls_paths", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		cluster := &commonpb.MilvusCluster{
			ClusterId: "test-cluster",
			ConnectionParam: &commonpb.ConnectionParam{
				Uri:   "localhost:19530",
				Token: "",
			},
		}

		cli, err := NewMilvusClient(ctx, cluster)
		// Either creation succeeds (gRPC lazy connect) or fails with a
		// connection error — but it must NOT fail with a TLS error.
		if err != nil {
			assert.NotContains(t, err.Error(), "TLS")
			assert.NotContains(t, err.Error(), "tls")
			assert.NotContains(t, err.Error(), "ca_pem_path")
		} else {
			assert.NotNil(t, cli)
			cli.Close(ctx)
		}
	})

	t.Run("client_cert_without_ca_errors", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		cluster := &commonpb.MilvusCluster{
			ClusterId: "test-cluster-no-ca",
			ConnectionParam: &commonpb.ConnectionParam{
				Uri:           "localhost:19530",
				ClientPemPath: "/some/client.pem",
				ClientKeyPath: "/some/client.key",
			},
		}

		_, err := NewMilvusClient(ctx, cluster)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ca_pem_path")
	})

	t.Run("ca_path_not_found_errors", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		cluster := &commonpb.MilvusCluster{
			ClusterId: "test-cluster-bad-ca",
			ConnectionParam: &commonpb.ConnectionParam{
				Uri:       "localhost:19530",
				CaPemPath: "/nonexistent/ca.pem",
			},
		}

		_, err := NewMilvusClient(ctx, cluster)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to build TLS config")
	})

	t.Run("valid_mtls_certs_accepted", func(t *testing.T) {
		tmpDir := t.TempDir()
		caCertPath, clientCertPath, clientKeyPath := generateTestCerts(t, tmpDir)

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		cluster := &commonpb.MilvusCluster{
			ClusterId: "test-cluster-mtls",
			ConnectionParam: &commonpb.ConnectionParam{
				Uri:           "localhost:19530",
				CaPemPath:     caCertPath,
				ClientPemPath: clientCertPath,
				ClientKeyPath: clientKeyPath,
			},
		}

		cli, err := NewMilvusClient(ctx, cluster)
		// TLS config building must succeed — no "failed to build TLS config" error.
		// The client may or may not connect (no server), but TLS setup is valid.
		if err != nil {
			assert.NotContains(t, err.Error(), "failed to build TLS config")
			assert.Contains(t, err.Error(), "failed to create milvus client")
		} else {
			assert.NotNil(t, cli)
			cli.Close(ctx)
		}
	})
}

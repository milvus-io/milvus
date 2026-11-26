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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestAnalyzerTLSFiles(t *testing.T) {
	paramtable.Init()
	root := t.TempDir()
	setting := &paramtable.Get().TLSAllowedPath
	previous := setting.SwapTempValue(root)
	require.NoError(t, updateParams())
	t.Cleanup(func() { setting.SwapTempValue(previous); require.NoError(t, updateParams()) })

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	ca := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test CA"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	require.NoError(t, err)
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	leaf := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "localhost"},
		NotBefore: ca.NotBefore, NotAfter: ca.NotAfter, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames: []string{"localhost"}, KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	certDER, err := x509.CreateCertificate(rand.Reader, leaf, ca, &key.PublicKey, caKey)
	require.NoError(t, err)
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	for name, data := range map[string][]byte{
		"ca.pem": caPEM, "client.pem": certPEM, "client.key": keyPEM,
		"bad.key": []byte("TLS_FILE_SECRET_MARKER"), "large.pem": []byte(strings.Repeat("x", 1024*1024+1)),
	} {
		require.NoError(t, os.WriteFile(filepath.Join(root, name), data, 0o600))
	}
	outside := filepath.Join(t.TempDir(), "outside.pem")
	require.NoError(t, os.WriteFile(outside, caPEM, 0o600))
	require.NoError(t, os.Symlink(outside, filepath.Join(root, "link.pem")))
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	require.NoError(t, err)
	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(caPEM))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{
		MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{pair},
		ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool,
	})))
	t.Cleanup(server.Stop)
	go func() { _ = server.Serve(listener) }()
	for _, tc := range []struct{ name, ca, cert, key, want string }{
		{"relative", "ca.pem", "client.pem", "client.key", ""},
		{"absolute", filepath.Join(root, "ca.pem"), filepath.Join(root, "client.pem"), filepath.Join(root, "client.key"), ""},
		{"outside", outside, "client.pem", "client.key", "outside the configured directory"},
		{"traversal", "../outside.pem", "client.pem", "client.key", "outside the configured directory"},
		{"symlink", "link.pem", "client.pem", "client.key", "symbolic link"},
		{"oversized", "large.pem", "client.pem", "client.key", "read limit"},
		{"malformed_key", "ca.pem", "client.pem", "bad.key", "TLS configuration is invalid"},
		{"key_outside", "ca.pem", "client.pem", outside, "outside the configured directory"},
		{"empty_key", "ca.pem", "client.pem", "", "non-empty string"},
		{"cert_outside", "ca.pem", outside, "client.key", "outside the configured directory"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			params, err := json.Marshal(map[string]any{"tokenizer": map[string]any{
				"type": "grpc", "endpoint": "https://" + listener.Addr().String(),
				"tls": map[string]any{"ca_cert": tc.ca, "client_cert": tc.cert, "client_key": tc.key},
			}})
			require.NoError(t, err)
			analyzer, err := NewAnalyzer(string(params))
			if err != nil && strings.Contains(err.Error(), "analyzer-remote-tls feature") {
				t.Skip("TLS support disabled in this build")
			}
			if tc.want == "" {
				require.NoError(t, err)
				analyzer.Destroy()
				err = ValidateAnalyzer(string(params))
				require.NoError(t, err)
				if tc.name == "absolute" {
					setting.SwapTempValue(t.TempDir())
					require.NoError(t, updateParams())
					restored, err := RestoreAnalyzer(string(params))
					require.NoError(t, err)
					restored.Destroy()
					_, err = NewAnalyzer(string(params))
					require.ErrorContains(t, err, "outside the configured directory")
					setting.SwapTempValue(root)
					require.NoError(t, updateParams())
				}
			} else {
				require.ErrorContains(t, err, tc.want)
				require.NotContains(t, err.Error(), "TLS_FILE_SECRET_MARKER")
				err = ValidateAnalyzer(string(params))
				require.ErrorContains(t, err, tc.want)
				require.NotContains(t, err.Error(), "TLS_FILE_SECRET_MARKER")
			}
		})
	}
	// Older definitions could omit an unusable client identity on a server-auth connection.
	serverOnlyListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	serverOnly := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{
		MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{pair},
	})))
	t.Cleanup(serverOnly.Stop)
	go func() { _ = serverOnly.Serve(serverOnlyListener) }()
	require.NoError(t, os.WriteFile(filepath.Join(root, "empty.pem"), nil, 0o600))
	for name, identity := range map[string]map[string]any{
		"cert_only":  {"client_cert": filepath.Join(root, "missing.pem")},
		"key_only":   {"client_key": filepath.Join(root, "missing.key")},
		"non_string": {"client_cert": 42, "client_key": filepath.Join(root, "client.key")},
		"unreadable": {"client_cert": filepath.Join(root, "missing.pem"), "client_key": filepath.Join(root, "client.key")},
		"empty":      {"client_cert": filepath.Join(root, "empty.pem"), "client_key": filepath.Join(root, "client.key")},
	} {
		t.Run("restore_legacy_identity_"+name, func(t *testing.T) {
			identity["ca_cert"] = filepath.Join(root, "ca.pem")
			params, err := json.Marshal(map[string]any{"tokenizer": map[string]any{
				"type": "grpc", "endpoint": "https://" + serverOnlyListener.Addr().String(), "tls": identity,
			}})
			require.NoError(t, err)
			restored, err := RestoreAnalyzer(string(params))
			if err != nil && strings.Contains(err.Error(), "analyzer-remote-tls feature") {
				t.Skip("TLS support disabled in this build")
			}
			require.NoError(t, err)
			restored.Destroy()
			_, err = NewAnalyzer(string(params))
			require.Error(t, err)
			err = ValidateAnalyzer(string(params))
			require.Error(t, err)
		})
	}
}

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
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// BuildTLSConfig creates a tls.Config for outbound mTLS connections.
// caPemPath is required (for server cert verification).
// clientPemPath + clientKeyPath are optional (for mutual TLS client auth).
func BuildTLSConfig(caPemPath, clientPemPath, clientKeyPath string) (*tls.Config, error) {
	caPem, err := os.ReadFile(caPemPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read CA cert %s: %w", caPemPath, err)
	}

	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(caPem) {
		return nil, fmt.Errorf("failed to parse CA cert from %s", caPemPath)
	}

	tlsConfig := &tls.Config{
		RootCAs:    certPool,
		MinVersion: tls.VersionTLS13,
	}

	if (clientPemPath != "") != (clientKeyPath != "") {
		return nil, fmt.Errorf("both clientPemPath and clientKeyPath must be set for mTLS, got clientPemPath=%q clientKeyPath=%q", clientPemPath, clientKeyPath)
	}

	if clientPemPath != "" && clientKeyPath != "" {
		clientCert, err := tls.LoadX509KeyPair(clientPemPath, clientKeyPath)
		if err != nil {
			return nil, fmt.Errorf("failed to load client cert (%s, %s): %w", clientPemPath, clientKeyPath, err)
		}
		tlsConfig.Certificates = []tls.Certificate{clientCert}
	}

	return tlsConfig, nil
}

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
		RootCAs: certPool,
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

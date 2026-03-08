package objectstorage

import (
	"context"
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseTLSMinVersion(t *testing.T) {
	tests := []struct {
		input    string
		expected uint16
		hasError bool
	}{
		{"1.0", tls.VersionTLS10, false},
		{"1.1", tls.VersionTLS11, false},
		{"1.2", tls.VersionTLS12, false},
		{"1.3", tls.VersionTLS13, false},
		{"", 0, true},
		{"2.0", 0, true},
		{"invalid", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			ver, err := parseTLSMinVersion(tt.input)
			if tt.hasError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, ver)
			}
		})
	}
}

// TestNewTLSHTTPClientTransportConfig verifies that newTLSHTTPClient
// correctly sets MinVersion on the Transport's TLSClientConfig.
func TestNewTLSHTTPClientTransportConfig(t *testing.T) {
	tests := []struct {
		input       string
		expectedMin uint16
	}{
		{"1.0", tls.VersionTLS10},
		{"1.1", tls.VersionTLS11},
		{"1.2", tls.VersionTLS12},
		{"1.3", tls.VersionTLS13},
	}
	for _, tt := range tests {
		t.Run("min_version_"+tt.input, func(t *testing.T) {
			client, err := newTLSHTTPClient(tt.input)
			require.NoError(t, err)
			require.NotNil(t, client)

			tr, ok := client.Transport.(*http.Transport)
			require.True(t, ok, "Transport should be *http.Transport")
			require.NotNil(t, tr.TLSClientConfig)
			assert.Equal(t, tt.expectedMin, tr.TLSClientConfig.MinVersion)
		})
	}

	t.Run("invalid_version", func(t *testing.T) {
		_, err := newTLSHTTPClient("2.0")
		assert.Error(t, err)
	})

	t.Run("preserves_default_transport_settings", func(t *testing.T) {
		client, err := newTLSHTTPClient("1.3")
		require.NoError(t, err)

		tr := client.Transport.(*http.Transport)
		defaultTr := http.DefaultTransport.(*http.Transport)
		// Verify key settings from DefaultTransport are preserved
		assert.Equal(t, defaultTr.MaxIdleConns, tr.MaxIdleConns)
		assert.Equal(t, defaultTr.IdleConnTimeout, tr.IdleConnTimeout)
		assert.Equal(t, defaultTr.ForceAttemptHTTP2, tr.ForceAttemptHTTP2)
	})
}

// TestNewTLSHTTPClientRejectsLowerVersion starts a TLS 1.2-only server
// and verifies that a client configured with MinVersion=1.3 cannot connect.
// This proves the TLS version config actually takes effect.
func TestNewTLSHTTPClientRejectsLowerVersion(t *testing.T) {
	// Start a TLS server that only supports TLS 1.2
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	server.TLS = &tls.Config{
		MinVersion: tls.VersionTLS12,
		MaxVersion: tls.VersionTLS12,
	}
	server.StartTLS()
	defer server.Close()

	t.Run("tls13_client_fails_against_tls12_server", func(t *testing.T) {
		client, err := newTLSHTTPClient("1.3")
		require.NoError(t, err)

		// Trust the test server's self-signed cert
		tr := client.Transport.(*http.Transport)
		tr.TLSClientConfig.InsecureSkipVerify = true

		_, err = client.Get(server.URL)
		require.Error(t, err, "TLS 1.3 client must fail against TLS 1.2-only server")
		t.Logf("confirmed: TLS 1.3 client rejected TLS 1.2 server: %v", err)
	})

	t.Run("tls12_client_succeeds_against_tls12_server", func(t *testing.T) {
		client, err := newTLSHTTPClient("1.2")
		require.NoError(t, err)

		tr := client.Transport.(*http.Transport)
		tr.TLSClientConfig.InsecureSkipVerify = true

		resp, err := client.Get(server.URL)
		require.NoError(t, err, "TLS 1.2 client should succeed against TLS 1.2 server")
		defer resp.Body.Close()

		require.NotNil(t, resp.TLS)
		assert.Equal(t, uint16(tls.VersionTLS12), resp.TLS.Version)
		t.Logf("confirmed: negotiated %s", tlsVersionName(resp.TLS.Version))
	})
}

func tlsVersionName(v uint16) string {
	switch v {
	case tls.VersionTLS10:
		return "TLS 1.0"
	case tls.VersionTLS11:
		return "TLS 1.1"
	case tls.VersionTLS12:
		return "TLS 1.2"
	case tls.VersionTLS13:
		return "TLS 1.3"
	default:
		return "unknown"
	}
}

// TestNewMinioClientTLSVersion tests TLS version configuration via NewMinioClient.
// Auth: ACCESS_KEY+SECRET_KEY, or USE_IAM=true.
//
// ACCESS_KEY+SECRET_KEY require:
//   - ADDRESS, BUCKET_NAME, CLOUD_PROVIDER, ACCESS_KEY, SECRET_KEY. Optional: REGION.
//
// USE_IAM require:
//   - ADDRESS, BUCKET_NAME, CLOUD_PROVIDER, USE_IAM=true. Optional: REGION.
//
// CLOUD_PROVIDER: aws or gcp (S3 compatibility mode, supports AK/SK or IAM).
func TestNewMinioClientTLSVersion(t *testing.T) {
	address := os.Getenv("ADDRESS")
	accessKey := os.Getenv("ACCESS_KEY")
	secretKey := os.Getenv("SECRET_KEY")
	bucketName := os.Getenv("BUCKET_NAME")
	region := os.Getenv("REGION")
	cloudProvider := os.Getenv("CLOUD_PROVIDER")
	useIAM := os.Getenv("USE_IAM") == "true"

	if address == "" || bucketName == "" {
		t.Skip("Skipping: set ADDRESS, BUCKET_NAME env vars to run this test")
	}
	hasAKSK := accessKey != "" && secretKey != ""
	if !hasAKSK && !useIAM {
		t.Skip("Skipping: set ACCESS_KEY+SECRET_KEY or USE_IAM=true to run this test")
	}
	if cloudProvider == "" {
		cloudProvider = "aws"
	}

	newConfig := func(tlsMinVersion string) *Config {
		return &Config{
			Address:           address,
			AccessKeyID:       accessKey,
			SecretAccessKeyID: secretKey,
			BucketName:        bucketName,
			Region:            region,
			UseSSL:            true,
			SslTLSMinVersion:  tlsMinVersion,
			CloudProvider:     cloudProvider,
			UseIAM:            useIAM,
		}
	}

	ctx := context.Background()

	t.Run("check_server_tls_support", func(t *testing.T) {
		for _, ver := range []struct {
			name string
			ver  uint16
		}{
			{"TLS 1.2", tls.VersionTLS12},
			{"TLS 1.3", tls.VersionTLS13},
		} {
			conn, err := tls.Dial("tcp", address+":443", &tls.Config{
				MinVersion: ver.ver,
				MaxVersion: ver.ver,
			})
			if err != nil {
				t.Logf("%s -> %s: NOT supported (%v)", address, ver.name, err)
			} else {
				state := conn.ConnectionState()
				t.Logf("%s -> %s: supported (negotiated: %s)", address, ver.name, tlsVersionName(state.Version))
				conn.Close()
			}
		}
	})

	t.Run("tls12_via_NewMinioClient", func(t *testing.T) {
		client, err := NewMinioClient(ctx, newConfig("1.2"))
		require.NoError(t, err, "NewMinioClient with TLS 1.2 should succeed")

		exists, err := client.BucketExists(ctx, bucketName)
		require.NoError(t, err, "BucketExists should succeed over TLS 1.2")
		assert.True(t, exists, "bucket %s should exist", bucketName)
		t.Logf("NewMinioClient(SslTLSMinVersion=1.2, CloudProvider=%s, UseIAM=%v): BucketExists(%s) = %v", cloudProvider, useIAM, bucketName, exists)
	})

	t.Run("tls13_via_NewMinioClient", func(t *testing.T) {
		conn, err := tls.Dial("tcp", address+":443", &tls.Config{
			MinVersion: tls.VersionTLS13,
		})
		if err != nil {
			t.Skipf("Skipping: %s does not support TLS 1.3 (%v)", address, err)
		}
		conn.Close()

		client, err := NewMinioClient(ctx, newConfig("1.3"))
		require.NoError(t, err, "NewMinioClient with TLS 1.3 should succeed")

		exists, err := client.BucketExists(ctx, bucketName)
		require.NoError(t, err, "BucketExists should succeed over TLS 1.3")
		assert.True(t, exists, "bucket %s should exist", bucketName)
		t.Logf("NewMinioClient(SslTLSMinVersion=1.3, CloudProvider=%s, UseIAM=%v): BucketExists(%s) = %v", cloudProvider, useIAM, bucketName, exists)
	})
}

// TestNewAzureClientTLSVersion tests TLS version configuration via NewAzureObjectStorageClient.
//
// Require:
//   - ADDRESS, BUCKET_NAME, CLOUD_PROVIDER=azure, ACCESS_KEY (storage account name), SECRET_KEY (storage account key).
func TestNewAzureClientTLSVersion(t *testing.T) {
	address := os.Getenv("ADDRESS")
	accessKey := os.Getenv("ACCESS_KEY")
	secretKey := os.Getenv("SECRET_KEY")
	bucketName := os.Getenv("BUCKET_NAME")
	cloudProvider := os.Getenv("CLOUD_PROVIDER")

	if cloudProvider != "azure" {
		t.Skip("Skipping: CLOUD_PROVIDER is not azure")
	}
	if address == "" || accessKey == "" || secretKey == "" || bucketName == "" {
		t.Skip("Skipping: set ADDRESS, ACCESS_KEY, SECRET_KEY, BUCKET_NAME env vars to run this test")
	}

	ctx := context.Background()

	// Probe TLS support on the Azure Blob endpoint
	azureHost := accessKey + ".blob." + address
	t.Run("check_server_tls_support", func(t *testing.T) {
		for _, ver := range []struct {
			name string
			ver  uint16
		}{
			{"TLS 1.2", tls.VersionTLS12},
			{"TLS 1.3", tls.VersionTLS13},
		} {
			conn, err := tls.Dial("tcp", azureHost+":443", &tls.Config{
				MinVersion: ver.ver,
				MaxVersion: ver.ver,
			})
			if err != nil {
				t.Logf("%s -> %s: NOT supported (%v)", azureHost, ver.name, err)
			} else {
				state := conn.ConnectionState()
				t.Logf("%s -> %s: supported (negotiated: %s)", azureHost, ver.name, tlsVersionName(state.Version))
				conn.Close()
			}
		}
	})

	t.Run("tls12_via_NewAzureObjectStorageClient", func(t *testing.T) {
		c := &Config{
			Address:           address,
			AccessKeyID:       accessKey,
			SecretAccessKeyID: secretKey,
			BucketName:        bucketName,
			UseSSL:            true,
			SslTLSMinVersion:  "1.2",
			CloudProvider:     cloudProvider,
		}

		client, err := NewAzureObjectStorageClient(ctx, c)
		require.NoError(t, err, "NewAzureObjectStorageClient with TLS 1.2 should succeed")
		require.NotNil(t, client)
		t.Logf("NewAzureObjectStorageClient(SslTLSMinVersion=1.2): success")
	})

	t.Run("tls13_via_NewAzureObjectStorageClient", func(t *testing.T) {
		conn, err := tls.Dial("tcp", azureHost+":443", &tls.Config{
			MinVersion: tls.VersionTLS13,
		})
		if err != nil {
			t.Skipf("Skipping: %s does not support TLS 1.3 (%v)", azureHost, err)
		}
		conn.Close()

		c := &Config{
			Address:           address,
			AccessKeyID:       accessKey,
			SecretAccessKeyID: secretKey,
			BucketName:        bucketName,
			UseSSL:            true,
			SslTLSMinVersion:  "1.3",
			CloudProvider:     cloudProvider,
		}

		client, err := NewAzureObjectStorageClient(ctx, c)
		require.NoError(t, err, "NewAzureObjectStorageClient with TLS 1.3 should succeed")
		require.NotNil(t, client)
		t.Logf("NewAzureObjectStorageClient(SslTLSMinVersion=1.3): success")
	})

	t.Run("no_tls_version_set", func(t *testing.T) {
		c := &Config{
			Address:           address,
			AccessKeyID:       accessKey,
			SecretAccessKeyID: secretKey,
			BucketName:        bucketName,
			UseSSL:            true,
			CloudProvider:     cloudProvider,
		}

		client, err := NewAzureObjectStorageClient(ctx, c)
		require.NoError(t, err, "NewAzureObjectStorageClient without TLS version should succeed")
		require.NotNil(t, client)
		t.Logf("NewAzureObjectStorageClient(SslTLSMinVersion=<empty>): success (default behavior)")
	})
}

// TestNewGcpNativeClientTLSVersion tests TLS version configuration via NewGcpObjectStorageClient.
// Auth: GCP_CREDENTIAL_JSON, or USE_IAM=true (uses ADC).
//
// GCP_CREDENTIAL_JSON require:
//   - BUCKET_NAME, CLOUD_PROVIDER=gcpnative, GCP_CREDENTIAL_JSON (service account JSON string).
//
// USE_IAM require:
//   - BUCKET_NAME, CLOUD_PROVIDER=gcpnative, USE_IAM=true.
func TestNewGcpNativeClientTLSVersion(t *testing.T) {
	bucketName := os.Getenv("BUCKET_NAME")
	cloudProvider := os.Getenv("CLOUD_PROVIDER")
	gcpCredentialJSON := os.Getenv("GCP_CREDENTIAL_JSON")
	useIAM := os.Getenv("USE_IAM") == "true"

	if cloudProvider != "gcpnative" {
		t.Skip("Skipping: CLOUD_PROVIDER is not gcpnative")
	}
	if bucketName == "" {
		t.Skip("Skipping: set BUCKET_NAME env var to run this test")
	}
	if gcpCredentialJSON == "" && !useIAM {
		t.Skip("Skipping: set GCP_CREDENTIAL_JSON or USE_IAM=true to run this test")
	}

	newConfig := func(tlsMinVersion string) *Config {
		return &Config{
			BucketName:        bucketName,
			UseSSL:            true,
			SslTLSMinVersion:  tlsMinVersion,
			CloudProvider:     cloudProvider,
			GcpCredentialJSON: gcpCredentialJSON,
			UseIAM:            useIAM,
		}
	}

	ctx := context.Background()
	tlsHost := "storage.googleapis.com"

	t.Run("check_server_tls_support", func(t *testing.T) {
		for _, ver := range []struct {
			name string
			ver  uint16
		}{
			{"TLS 1.2", tls.VersionTLS12},
			{"TLS 1.3", tls.VersionTLS13},
		} {
			conn, err := tls.Dial("tcp", tlsHost+":443", &tls.Config{
				MinVersion: ver.ver,
				MaxVersion: ver.ver,
			})
			if err != nil {
				t.Logf("%s -> %s: NOT supported (%v)", tlsHost, ver.name, err)
			} else {
				state := conn.ConnectionState()
				t.Logf("%s -> %s: supported (negotiated: %s)", tlsHost, ver.name, tlsVersionName(state.Version))
				conn.Close()
			}
		}
	})

	t.Run("tls12_via_NewGcpObjectStorageClient", func(t *testing.T) {
		client, err := NewGcpObjectStorageClient(ctx, newConfig("1.2"))
		require.NoError(t, err, "NewGcpObjectStorageClient with TLS 1.2 should succeed")
		require.NotNil(t, client)
		t.Logf("NewGcpObjectStorageClient(SslTLSMinVersion=1.2): success")
	})

	t.Run("tls13_via_NewGcpObjectStorageClient", func(t *testing.T) {
		conn, err := tls.Dial("tcp", tlsHost+":443", &tls.Config{
			MinVersion: tls.VersionTLS13,
		})
		if err != nil {
			t.Skipf("Skipping: %s does not support TLS 1.3 (%v)", tlsHost, err)
		}
		conn.Close()

		client, err := NewGcpObjectStorageClient(ctx, newConfig("1.3"))
		require.NoError(t, err, "NewGcpObjectStorageClient with TLS 1.3 should succeed")
		require.NotNil(t, client)
		t.Logf("NewGcpObjectStorageClient(SslTLSMinVersion=1.3): success")
	})

	t.Run("no_tls_version_set", func(t *testing.T) {
		client, err := NewGcpObjectStorageClient(ctx, newConfig(""))
		require.NoError(t, err, "NewGcpObjectStorageClient without TLS version should succeed")
		require.NotNil(t, client)
		t.Logf("NewGcpObjectStorageClient(SslTLSMinVersion=<empty>): success (default behavior)")
	})
}

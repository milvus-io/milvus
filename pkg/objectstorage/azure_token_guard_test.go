// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0

package objectstorage

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
)

type azureGuardCredentialFunc func(context.Context, policy.TokenRequestOptions) (azcore.AccessToken, error)

func (f azureGuardCredentialFunc) GetToken(ctx context.Context, options policy.TokenRequestOptions) (azcore.AccessToken, error) {
	return f(ctx, options)
}

type azureGuardTransportFunc func(*http.Request) (*http.Response, error)

func (f azureGuardTransportFunc) Do(request *http.Request) (*http.Response, error) { return f(request) }
func (f azureGuardTransportFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func azureGuardResponse(request *http.Request, status int) *http.Response {
	return &http.Response{
		StatusCode: status, Request: request,
		Header: http.Header{"Etag": {`"synthetic"`}, "Last-Modified": {"Mon, 16 Nov 2026 00:00:00 GMT"}},
		Body:   io.NopCloser(strings.NewReader("")),
	}
}

func TestAzureTokenGuardBlocksSDKExpiredFallbackAndRecovers(t *testing.T) {
	// Exercise the real azblob client and azcore temporal/Bearer policy. A fake
	// clock advances past SDK's 30s eager-refresh backoff, then past real expiry
	// while the credential fetch fails. No sleep or live endpoint is needed.
	for _, guarded := range []bool{false, true} {
		t.Run(map[bool]string{false: "sdk_control", true: "guarded"}[guarded], func(t *testing.T) {
			base := time.Now().UTC()
			var clock atomic.Int64
			clock.Store(base.UnixNano())
			patch := mockey.Mock(time.Now).To(func() time.Time { return time.Unix(0, clock.Load()) }).Build()
			defer patch.UnPatch()
			calls, sends := 0, 0
			cred := azureGuardCredentialFunc(func(context.Context, policy.TokenRequestOptions) (azcore.AccessToken, error) {
				calls++
				switch calls {
				case 1:
					return azcore.AccessToken{Token: "old-token", ExpiresOn: base.Add(time.Minute)}, nil
				case 2:
					clock.Store(base.Add(61 * time.Second).UnixNano())
					return azcore.AccessToken{}, errors.New("synthetic refresh failure")
				default:
					return azcore.AccessToken{Token: "new-token", ExpiresOn: time.Now().Add(time.Hour)}, nil
				}
			})
			var lastHeader string
			transport := azureGuardTransportFunc(func(request *http.Request) (*http.Response, error) {
				sends++
				lastHeader = request.Header.Get("Authorization")
				return azureGuardResponse(request, http.StatusOK), nil
			})
			opts := &service.ClientOptions{}
			opts.Retry.MaxRetries = -1
			var credential azcore.TokenCredential = cred
			opts.Transport = transport
			if guarded {
				credential, opts.Transport = protectAzureTokenExpiry(credential, opts.Transport)
			}
			client, err := service.NewClient("https://synthetic.blob.core.windows.net/", credential, opts)
			require.NoError(t, err)
			container := client.NewContainerClient("container")
			_, err = container.GetProperties(context.Background(), nil)
			require.NoError(t, err)
			require.Equal(t, 1, sends)
			clock.Store(base.Add(31 * time.Second).UnixNano())
			_, err = container.GetProperties(context.Background(), nil)
			require.Equal(t, 2, calls, "the SDK must attempt the eager refresh")
			if guarded {
				require.ErrorContains(t, err, "expired before request dispatch")
				require.Equal(t, 1, sends, "expired fallback must never reach the configured transport")
			} else {
				require.NoError(t, err)
				require.Equal(t, 2, sends, "control demonstrates the pinned SDK's stale fallback")
				require.Equal(t, "Bearer old-token", lastHeader)
			}
			_, err = container.GetProperties(context.Background(), nil)
			require.NoError(t, err)
			require.Equal(t, 3, calls)
			require.Equal(t, "Bearer new-token", lastHeader)
		})
	}
}

func TestAzureTokenGuardPreservesNativeCAEAndCustomTransport(t *testing.T) {
	claims := `{"access_token":{"xms_cc":{"values":["cp1"]}}}`
	calls, sends := 0, 0
	cred := azureGuardCredentialFunc(func(_ context.Context, options policy.TokenRequestOptions) (azcore.AccessToken, error) {
		calls++
		require.True(t, options.EnableCAE)
		require.Equal(t, []string{"https://storage.azure.com/.default"}, options.Scopes)
		token := "initial"
		if calls > 1 {
			require.Equal(t, claims, options.Claims)
			token = "claims-token"
		}
		return azcore.AccessToken{Token: token, ExpiresOn: time.Now().Add(time.Hour)}, nil
	})
	transport := azureGuardTransportFunc(func(request *http.Request) (*http.Response, error) {
		sends++
		if sends == 1 {
			response := azureGuardResponse(request, http.StatusUnauthorized)
			response.Header.Set("WWW-Authenticate", `Bearer error="insufficient_claims", claims="`+base64.StdEncoding.EncodeToString([]byte(claims))+`"`)
			return response, nil
		}
		require.Equal(t, "Bearer claims-token", request.Header.Get("Authorization"))
		return azureGuardResponse(request, http.StatusOK), nil
	})
	opts := &service.ClientOptions{}
	opts.Retry.MaxRetries = -1
	protected, transportGuard := protectAzureTokenExpiry(cred, transport)
	opts.Transport = transportGuard
	client, err := service.NewClient("https://synthetic.blob.core.windows.net/", protected, opts)
	require.NoError(t, err)
	_, err = client.NewContainerClient("container").GetProperties(context.Background(), nil)
	require.NoError(t, err)
	require.Equal(t, 2, calls)
	require.Equal(t, 2, sends)
}

func TestAzureTokenGuardRejectsInvalidCredentialResponses(t *testing.T) {
	for _, token := range []azcore.AccessToken{
		{},
		{Token: "", ExpiresOn: time.Now().Add(time.Hour)},
		{Token: "unknown-expiry"},
		{Token: "expired", ExpiresOn: time.Now().Add(-time.Second)},
		{Token: "bad\r\nheader", ExpiresOn: time.Now().Add(time.Hour)},
	} {
		protected, _ := protectAzureTokenExpiry(azureGuardCredentialFunc(func(context.Context, policy.TokenRequestOptions) (azcore.AccessToken, error) {
			return token, nil
		}), nil)
		_, err := protected.GetToken(context.Background(), policy.TokenRequestOptions{})
		require.Error(t, err)
		require.NotContains(t, err.Error(), "bad\r\nheader")
	}
}

func TestAzureTokenGuardConcurrentTokensAndCustomTransport(t *testing.T) {
	var next atomic.Int64
	var sent atomic.Int64
	cred := azureGuardCredentialFunc(func(_ context.Context, options policy.TokenRequestOptions) (azcore.AccessToken, error) {
		return azcore.AccessToken{Token: options.Claims, ExpiresOn: time.Now().Add(time.Hour)}, nil
	})
	transport := azureGuardTransportFunc(func(request *http.Request) (*http.Response, error) {
		sent.Add(1)
		return azureGuardResponse(request, http.StatusOK), nil
	})
	protected, guarded := protectAzureTokenExpiry(cred, transport)
	var wg sync.WaitGroup
	errors := make(chan error, 16)
	for i := 0; i != 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := next.Add(1)
			token, err := protected.GetToken(context.Background(), policy.TokenRequestOptions{Claims: string(rune('a' + id))})
			if err == nil {
				request, _ := http.NewRequest(http.MethodHead, "https://synthetic.blob.core.windows.net/container", nil)
				request.Header.Set("Authorization", "Bearer "+token.Token)
				_, err = guarded.Do(request)
			}
			errors <- err
		}()
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	require.EqualValues(t, 16, sent.Load())
}

func TestAzureSharedKeyKeepsConfiguredTLSTransportWithoutBearerGuard(t *testing.T) {
	sends := 0
	custom := &http.Client{Transport: azureGuardTransportFunc(func(request *http.Request) (*http.Response, error) {
		sends++
		require.True(t, strings.HasPrefix(request.Header.Get("Authorization"), "SharedKey synthetic:"))
		return azureGuardResponse(request, http.StatusOK), nil
	})}
	patch := mockey.Mock(newTLSHTTPClient).To(func(string) (*http.Client, error) { return custom, nil }).Build()
	defer patch.UnPatch()
	cfg := &Config{
		Address: "core.windows.net", AccessKeyID: "synthetic", SecretAccessKeyID: base64.StdEncoding.EncodeToString([]byte("synthetic-secret")),
		BucketName: "container", UseSSL: true, SslTLSMinVersion: "1.2", SkipBucketCheck: true, IgnoreAzureConnectionString: true,
	}
	client, err := NewAzureObjectStorageClient(context.Background(), cfg)
	require.NoError(t, err)
	_, err = client.NewContainerClient("container").GetProperties(context.Background(), nil)
	require.NoError(t, err)
	require.Equal(t, 1, sends)
}

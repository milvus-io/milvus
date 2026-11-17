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
	"crypto/sha256"
	"crypto/tls"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"golang.org/x/net/http2"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// The SDK's temporal cache can return an old token after a failed early refresh
// without rechecking its expiration. Keep its Bearer/CAE machinery intact, but
// validate the exact token chosen by that machinery at transport dispatch.
type azureTokenExpiryGuard struct {
	credential azcore.TokenCredential
	transport  policy.Transporter
	mu         sync.RWMutex
	expires    map[[sha256.Size]byte]time.Time
}

func protectAzureTokenExpiry(credential azcore.TokenCredential, transport policy.Transporter) (azcore.TokenCredential, policy.Transporter) {
	if transport == nil {
		transport = azureDefaultTokenTransport
	}
	guard := &azureTokenExpiryGuard{credential: credential, transport: transport, expires: make(map[[sha256.Size]byte]time.Time)}
	return guard, guard
}

func (g *azureTokenExpiryGuard) GetToken(ctx context.Context, options policy.TokenRequestOptions) (azcore.AccessToken, error) {
	// Pass claims, tenant, scopes and CAE flags through without reinterpretation.
	token, err := g.credential.GetToken(ctx, options)
	if err != nil {
		return azcore.AccessToken{}, err
	}
	now := time.Now()
	if token.Token == "" || !token.ExpiresOn.After(now) ||
		strings.IndexFunc(token.Token, func(r rune) bool { return r <= 0x20 || r >= 0x7f }) >= 0 {
		return azcore.AccessToken{}, merr.WrapErrServiceUnavailableMsg("Azure credential refresh returned an invalid or expired token")
	}
	g.mu.Lock()
	for key, expiration := range g.expires {
		if !expiration.After(now) {
			delete(g.expires, key)
		}
	}
	// Retain only a digest as the lookup key, never another copy of the secret.
	g.expires[sha256.Sum256([]byte(token.Token))] = token.ExpiresOn
	g.mu.Unlock()
	return token, nil
}

func (g *azureTokenExpiryGuard) Do(request *http.Request) (*http.Response, error) {
	kind, token, ok := strings.Cut(request.Header.Get("Authorization"), " ")
	if !ok || !strings.EqualFold(kind, "Bearer") || token == "" {
		return nil, merr.WrapErrServiceUnavailableMsg("Azure request has no validated bearer token")
	}
	g.mu.RLock()
	expiration, known := g.expires[sha256.Sum256([]byte(token))]
	g.mu.RUnlock()
	if !known || !expiration.After(time.Now()) {
		return nil, merr.WrapErrServiceUnavailableMsg("Azure bearer token expired before request dispatch")
	}
	return g.transport.Do(request)
}

// azcore v1.21.0 keeps its default transport private. Match its transport defaults
// (runtime/transport_default_http_client.go, Microsoft Corporation, MIT) instead
// of silently substituting http.DefaultClient. Configured TLS/custom transports
// are preserved above, and the pool is shared just like the SDK's default pool.
var azureDefaultTokenTransport = newAzureDefaultTokenTransport()

// The following transport defaults are adapted from Azure SDK for Go.
// Copyright (c) Microsoft Corporation. All rights reserved.
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
// of the Software, and to permit persons to whom the Software is furnished to do
// so, subject to the following conditions: The above copyright notice and this
// permission notice shall be included in all copies or substantial portions of
// the Software. THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO
// EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES
// OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
// ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.
func newAzureDefaultTokenTransport() *http.Client {
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   10,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: time.Second,
		TLSClientConfig: &tls.Config{
			MinVersion:    tls.VersionTLS12,
			Renegotiation: tls.RenegotiateFreelyAsClient,
		},
	}
	if h2, err := http2.ConfigureTransports(transport); err == nil {
		h2.ReadIdleTimeout = 10 * time.Second
		h2.PingTimeout = 5 * time.Second
	}
	return &http.Client{Transport: transport}
}

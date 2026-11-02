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

package specutil

import (
	"net"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// NormalizeBrokerEndpoint defines the exact endpoint identity shared with C++.
// Reject paths the HTTP transport would rewrite instead of authorizing one URL
// and sending another. Empty paths normalize to the HTTP root path.
func NormalizeBrokerEndpoint(raw string) (string, error) {
	for _, c := range raw {
		if c <= 32 || c >= 127 || c == '\\' {
			return "", merr.WrapErrParameterInvalidMsg("credential broker endpoint contains invalid characters")
		}
	}
	u, err := url.Parse(raw)
	if err != nil || u.Opaque != "" || u.Host == "" || u.User != nil || u.Fragment != "" || strings.Contains(raw, "#") {
		return "", merr.WrapErrParameterInvalidMsg("credential broker endpoint must be a valid HTTP(S) URL without userinfo or fragment")
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return "", merr.WrapErrParameterInvalidMsg("credential broker endpoint must be a valid HTTP(S) URL")
	}
	host := strings.ToLower(u.Hostname())
	if host == "" || strings.ContainsAny(host, "%*\\") || strings.HasSuffix(u.Host, ":") {
		return "", merr.WrapErrParameterInvalidMsg("credential broker endpoint has an invalid host")
	}
	// net/url validates bracketed IPv6 literals. Restrict DNS names further.
	if !strings.Contains(host, ":") {
		for _, c := range host {
			if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '.' && c != '-' && c != '_' {
				return "", merr.WrapErrParameterInvalidMsg("credential broker endpoint has an invalid hostname")
			}
		}
	}
	port := u.Port()
	if port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || len(port) > 5 || n < 1 || n > 65535 {
			return "", merr.WrapErrParameterInvalidMsg("credential broker endpoint has an invalid port")
		}
		port = strconv.Itoa(n)
		if scheme == "http" && n == 80 || scheme == "https" && n == 443 {
			port = ""
		}
	}
	if port != "" {
		host = net.JoinHostPort(host, port)
	} else if strings.Contains(host, ":") {
		host = "[" + host + "]"
	}
	path := u.EscapedPath()
	if path == "" {
		path = "/"
	}
	if strings.Contains(path, "//") {
		return "", merr.WrapErrParameterInvalidMsg("credential broker path must not contain repeated separators")
	}
	for _, segment := range strings.Split(path, "/") {
		if segment == "." || segment == ".." {
			return "", merr.WrapErrParameterInvalidMsg("credential broker path must not contain dot segments")
		}
	}
	for _, c := range path {
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') && !strings.ContainsRune("-._~/", c) {
			return "", merr.WrapErrParameterInvalidMsg("credential broker path must use unescaped URL-unreserved characters and slashes")
		}
	}
	// Validate query syntax without changing its exact URL identity.
	if _, err := url.QueryUnescape(u.RawQuery); err != nil {
		return "", merr.WrapErrParameterInvalidMsg("credential broker query contains an invalid percent escape")
	}
	for _, c := range u.RawQuery {
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') && !strings.ContainsRune("-._~!$&'()*+,;=:@/?%", c) {
			return "", merr.WrapErrParameterInvalidMsg("credential broker query contains an invalid URL character")
		}
	}
	query := ""
	if u.RawQuery != "" || u.ForceQuery {
		query = "?" + u.RawQuery
	}
	return scheme + "://" + host + path + query, nil
}

// ResolveBrokerEndpoint accepts the old Azure spelling without mutating metadata.
func ResolveBrokerEndpoint(extfs map[string]string) (string, error) {
	current, legacy := extfs[ExtfsKeyCredentialEndpoint], extfs[ExtfsKeyAzureCredentialEndpoint]
	if legacy != "" && extfs[ExtfsKeyCloudProvider] != "azure" {
		return "", merr.WrapErrParameterInvalidMsg("Azure credential broker requires scheme=\"azure\" and cloud_provider=azure")
	}
	var endpoint string
	for _, value := range []string{current, legacy} {
		if value == "" {
			continue
		}
		normalized, err := NormalizeBrokerEndpoint(value)
		if err != nil {
			return "", err
		}
		if endpoint != "" && endpoint != normalized {
			return "", merr.WrapErrParameterInvalidMsg("credential_endpoint conflicts with azure_credential_endpoint")
		}
		endpoint = normalized
	}
	return endpoint, nil
}

// BrokerPolicy is an immutable server-owned allowlist. A nil policy denies brokers.
type BrokerPolicy struct{ endpoints map[string]struct{} }

func NewBrokerPolicy(endpoints []string) (*BrokerPolicy, error) {
	p := &BrokerPolicy{endpoints: make(map[string]struct{}, len(endpoints))}
	for _, endpoint := range endpoints {
		normalized, err := NormalizeBrokerEndpoint(endpoint)
		if err != nil {
			return nil, merr.Wrap(err, "invalid credentialBrokerAllowedEndpoints entry")
		}
		p.endpoints[normalized] = struct{}{}
	}
	return p, nil
}

// Endpoints returns a copy for publishing the same startup policy to native readers.
func (p *BrokerPolicy) Endpoints() []string {
	if p == nil {
		return []string{}
	}
	endpoints := make([]string, 0, len(p.endpoints))
	for endpoint := range p.endpoints {
		endpoints = append(endpoints, endpoint)
	}
	sort.Strings(endpoints)
	return endpoints
}

func (p *BrokerPolicy) Validate(extfs map[string]string) error {
	endpoint, err := ResolveBrokerEndpoint(extfs)
	if err != nil || endpoint == "" {
		return err
	}
	if p != nil {
		if _, ok := p.endpoints[endpoint]; ok {
			return nil
		}
	}
	return merr.WrapErrParameterInvalidMsg("credential broker endpoint is not allowed by common.security.credentialBrokerAllowedEndpoints")
}

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

package storage

import (
	"net"
	"net/url"
	"strings"

	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/util/externalspec"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type storageEndpointIdentity struct {
	cloudProvider string
	region        string
	azureAccount  string
	azureSuffix   string
}

func normalizeEndpointHost(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", nil
	}

	var host string
	if strings.Contains(raw, "://") {
		u, err := url.Parse(raw)
		if err != nil {
			return "", merr.WrapErrParameterInvalidMsg("invalid endpoint %q: %s", raw, err.Error())
		}
		if u.User != nil {
			return "", merr.WrapErrParameterInvalidMsg("endpoint must not embed credentials")
		}
		if hasPathTraversal(u.EscapedPath()) {
			return "", merr.WrapErrParameterInvalidMsg("endpoint path must not contain path traversal")
		}
		if strings.Trim(u.Path, "/") != "" {
			return "", merr.WrapErrParameterInvalidMsg("endpoint must be a host, not a URL path")
		}
		host = u.Host
	} else {
		if strings.Contains(raw, "@") {
			return "", merr.WrapErrParameterInvalidMsg("endpoint must not embed credentials")
		}
		if strings.Contains(raw, "/") || strings.Contains(raw, "\\") {
			return "", merr.WrapErrParameterInvalidMsg("endpoint must be a host, not a path")
		}
		host = raw
	}

	host = strings.TrimSuffix(strings.ToLower(strings.TrimSpace(host)), ".")
	if host == "" {
		return "", merr.WrapErrParameterInvalidMsg("endpoint host is empty")
	}
	return host, nil
}

func endpointAllowedByAllowlist(endpointHost, allowlist, cloudProvider, region string) bool {
	host, err := normalizeEndpointHost(endpointHost)
	if err != nil || host == "" {
		return false
	}
	if isCanonicalCloudEndpoint(host, cloudProvider, region) {
		return true
	}
	// Custom endpoints are only trusted when the operator explicitly allowlists
	// them. This prevents a request-level external_spec from redirecting copy
	// credentials to an arbitrary host.
	return endpointInAllowlist(host, allowlist)
}

func endpointsCompatible(instanceHost, foreignHost, allowlist, provider, region string) bool {
	instanceHost, _ = normalizeEndpointHost(instanceHost)
	foreignHost, _ = normalizeEndpointHost(foreignHost)
	if foreignHost == "" {
		return true
	}
	if instanceHost == "" {
		return false
	}
	if instanceHost == foreignHost {
		return true
	}
	if !endpointAllowedByAllowlist(foreignHost, allowlist, provider, region) {
		return false
	}
	if service := s3EndpointService(instanceHost); service != "" && service == s3EndpointService(foreignHost) {
		return true
	}
	return false
}

func effectiveEndpointHost(cfg *objectstorage.Config) string {
	if cfg == nil {
		return ""
	}
	host, _ := normalizeEndpointHost(cfg.Address)
	if host != "" {
		return stripDefaultEndpointPort(host, cfg.UseSSL)
	}
	derived := externalspec.DeriveEndpoint(cfg.CloudProvider, cfg.Region)
	host, _ = normalizeEndpointHost(derived)
	return stripDefaultEndpointPort(host, cfg.UseSSL)
}

func stripDefaultEndpointPort(host string, useSSL bool) string {
	hostname, port, err := net.SplitHostPort(host)
	if err != nil {
		return host
	}
	if (useSSL && port == "443") || (!useSSL && port == "80") {
		return strings.Trim(hostname, "[]")
	}
	return host
}

// inferStorageEndpointIdentity recognizes the standard endpoint forms Milvus
// emits. Custom and provider-specific endpoint variants require explicit
// external_spec provider settings instead of heuristic classification.
func inferStorageEndpointIdentity(raw string) storageEndpointIdentity {
	host, err := normalizeEndpointHost(raw)
	if err != nil || host == "" {
		return storageEndpointIdentity{}
	}
	host = hostWithoutPort(host)

	for _, suffix := range []string{
		"core.windows.net",
		"core.chinacloudapi.cn",
		"core.usgovcloudapi.net",
		"core.cloudapi.de",
	} {
		marker := ".blob." + suffix
		if strings.HasSuffix(host, marker) {
			account := strings.TrimSuffix(host, marker)
			if account != "" && !strings.Contains(account, ".") {
				return storageEndpointIdentity{
					cloudProvider: objectstorage.CloudProviderAzure,
					azureAccount:  account,
					azureSuffix:   suffix,
				}
			}
		}
		if host == suffix {
			return storageEndpointIdentity{
				cloudProvider: objectstorage.CloudProviderAzure,
				azureSuffix:   suffix,
			}
		}
	}

	provider := s3EndpointService(host)
	identity := storageEndpointIdentity{cloudProvider: provider}
	switch provider {
	case objectstorage.CloudProviderAWS:
		identity.region = endpointRegion(host, "s3.", ".amazonaws.com")
		if identity.region == "" {
			identity.region = endpointRegion(host, "s3.", ".amazonaws.com.cn")
		}
	case objectstorage.CloudProviderAliyun:
		identity.region = endpointRegion(host, "oss-", ".aliyuncs.com")
		identity.region = strings.TrimSuffix(identity.region, "-internal")
		if identity.region == "accelerate" || identity.region == "accelerate-overseas" {
			identity.region = ""
		}
	case objectstorage.CloudProviderTencent:
		identity.region = endpointRegion(host, "cos.", ".myqcloud.com")
	case objectstorage.CloudProviderHuawei:
		identity.region = endpointRegion(host, "obs.", ".myhuaweicloud.com")
	}
	return identity
}

func endpointRegion(host, prefix, suffix string) string {
	if !strings.HasPrefix(host, prefix) || !strings.HasSuffix(host, suffix) {
		return ""
	}
	region := strings.TrimSuffix(strings.TrimPrefix(host, prefix), suffix)
	if strings.Contains(region, ".") {
		return ""
	}
	return region
}

func isCanonicalCloudEndpoint(host, cloudProvider, region string) bool {
	hostOnly := hostWithoutPort(host)
	cloudProvider = strings.ToLower(strings.TrimSpace(cloudProvider))
	derived := externalspec.DeriveEndpoint(cloudProvider, region)
	derivedHost, _ := normalizeEndpointHost(derived)
	if derivedHost != "" && host == derivedHost {
		return true
	}
	switch cloudProvider {
	case objectstorage.CloudProviderAWS:
		return s3EndpointService(hostOnly) == objectstorage.CloudProviderAWS
	case objectstorage.CloudProviderGCP:
		return s3EndpointService(hostOnly) == objectstorage.CloudProviderGCP
	case objectstorage.CloudProviderGCPNative:
		return hostOnly == "storage.googleapis.com"
	case objectstorage.CloudProviderAliyun:
		return s3EndpointService(hostOnly) == objectstorage.CloudProviderAliyun
	case objectstorage.CloudProviderTencent:
		return s3EndpointService(hostOnly) == objectstorage.CloudProviderTencent
	case objectstorage.CloudProviderHuawei:
		return s3EndpointService(hostOnly) == objectstorage.CloudProviderHuawei
	case objectstorage.CloudProviderAzure:
		return isAzureEndpointService(hostOnly)
	case "minio":
		return false
	default:
		return cloudProvider == "" && externalspec.IsCloudEndpointHost(hostOnly)
	}
}

func endpointInAllowlist(host, allowlist string) bool {
	entries := strings.FieldsFunc(allowlist, func(r rune) bool {
		return r == ',' || r == ';' || r == '\n' || r == '\t' || r == ' '
	})
	for _, entry := range entries {
		normalized, err := normalizeEndpointHost(entry)
		if err == nil && normalized == host {
			return true
		}
	}
	return false
}

func hostWithoutPort(host string) string {
	if host == "" {
		return ""
	}
	if h, _, err := net.SplitHostPort(host); err == nil {
		return strings.Trim(h, "[]")
	}
	return strings.Trim(host, "[]")
}

func s3EndpointService(host string) string {
	host = hostWithoutPort(strings.ToLower(host))
	switch {
	case host == "s3.amazonaws.com",
		strings.HasPrefix(host, "s3.") && strings.HasSuffix(host, ".amazonaws.com"),
		strings.HasPrefix(host, "s3.") && strings.HasSuffix(host, ".amazonaws.com.cn"):
		return objectstorage.CloudProviderAWS
	case host == "storage.googleapis.com":
		return objectstorage.CloudProviderGCP
	case strings.HasPrefix(host, "oss-") && strings.HasSuffix(host, ".aliyuncs.com"):
		return objectstorage.CloudProviderAliyun
	case strings.HasPrefix(host, "cos.") && strings.HasSuffix(host, ".myqcloud.com"):
		return objectstorage.CloudProviderTencent
	case strings.HasPrefix(host, "obs.") && strings.HasSuffix(host, ".myhuaweicloud.com"):
		return objectstorage.CloudProviderHuawei
	default:
		return ""
	}
}

func isAzureEndpointService(host string) bool {
	host = hostWithoutPort(strings.ToLower(host))
	return host == "core.windows.net" ||
		host == "core.chinacloudapi.cn" ||
		host == "core.usgovcloudapi.net" ||
		host == "core.cloudapi.de"
}

func hasPathTraversal(escapedPath string) bool {
	unescaped, err := url.PathUnescape(escapedPath)
	if err != nil {
		return true
	}
	for _, part := range strings.Split(unescaped, "/") {
		if part == ".." {
			return true
		}
	}
	return false
}

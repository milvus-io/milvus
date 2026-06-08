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

package snapshotstorage

import (
	"net"
	"net/url"
	"strings"

	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/util/externalspec"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func NormalizeEndpointHost(raw string) (string, error) {
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

func EndpointAllowedByAllowlist(endpointHost, instanceEndpointHost, allowlist, cloudProvider, region string) bool {
	host, err := NormalizeEndpointHost(endpointHost)
	if err != nil || host == "" {
		return false
	}

	instanceHost, _ := NormalizeEndpointHost(instanceEndpointHost)
	if instanceHost != "" && host == instanceHost {
		return true
	}
	if endpointIsRestrictedIP(host) {
		return endpointInAllowlist(host, allowlist)
	}
	if isCanonicalCloudEndpoint(host, cloudProvider, region) {
		return true
	}
	return endpointInAllowlist(host, allowlist)
}

func endpointsCompatible(instanceHost, foreignHost, allowlist, provider, region string) bool {
	instanceHost, _ = NormalizeEndpointHost(instanceHost)
	foreignHost, _ = NormalizeEndpointHost(foreignHost)
	if foreignHost == "" {
		return true
	}
	if instanceHost == "" {
		return false
	}
	if instanceHost == foreignHost {
		return true
	}
	if !EndpointAllowedByAllowlist(foreignHost, instanceHost, allowlist, provider, region) {
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
	host, _ := NormalizeEndpointHost(cfg.Address)
	if host != "" {
		return host
	}
	derived := externalspec.DeriveEndpoint(cfg.CloudProvider, cfg.Region)
	host, _ = NormalizeEndpointHost(derived)
	return host
}

func isCanonicalCloudEndpoint(host, cloudProvider, region string) bool {
	hostOnly := hostWithoutPort(host)
	cloudProvider = strings.ToLower(strings.TrimSpace(cloudProvider))
	derived := externalspec.DeriveEndpoint(cloudProvider, region)
	derivedHost, _ := NormalizeEndpointHost(derived)
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
	for _, entry := range splitAllowlist(allowlist) {
		normalized, err := NormalizeEndpointHost(entry)
		if err == nil && normalized == host {
			return true
		}
	}
	return false
}

func splitAllowlist(allowlist string) []string {
	return strings.FieldsFunc(allowlist, func(r rune) bool {
		return r == ',' || r == ';' || r == '\n' || r == '\t' || r == ' '
	})
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

func endpointIsRestrictedIP(host string) bool {
	ip := net.ParseIP(hostWithoutPort(host))
	if ip == nil {
		return false
	}
	return ip.IsLoopback() ||
		ip.IsPrivate() ||
		ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() ||
		ip.IsMulticast() ||
		ip.IsUnspecified()
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

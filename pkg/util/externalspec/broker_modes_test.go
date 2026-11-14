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

package externalspec

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/externalspec/specutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const matrixBrokerEndpoint = "https://broker.example/token?scope=a,b"

func brokerModeSpec(cloud, iam string) (string, map[string]string) {
	values := map[string]string{ExtfsKeyCloudProvider: cloud, ExtfsKeyRegion: "test-region", ExtfsKeyCredentialEndpoint: matrixBrokerEndpoint}
	source := "s3://bucket/key"
	if iam != "" {
		values[ExtfsKeyUseIAM] = iam
	}
	switch cloud {
	case CloudProviderAWS, CloudProviderAliyun, CloudProviderTencent:
		if iam != "true" {
			values[ExtfsKeyRoleARN] = "target-role"
		}
	case CloudProviderGCP:
		source = "gs://bucket/key"
		if iam != "true" {
			values[ExtfsKeyGCPTargetServiceAccount] = "reader@project.iam.gserviceaccount.com"
		}
	case CloudProviderAzure:
		source = "azure://core.windows.net/container/path"
		values[ExtfsKeyAccessKeyID] = "storageaccount"
		if iam != "true" {
			values[ExtfsKeyAzureClientID] = "target-client"
			values[ExtfsKeyAzureTenantID] = "target-tenant"
		}
	}
	return source, values
}

func marshalBrokerSpec(t *testing.T, format string, values map[string]string) string {
	t.Helper()
	spec, err := json.Marshal(ExternalSpec{Format: format, Extfs: values})
	require.NoError(t, err)
	return string(spec)
}

func TestFiveCloudBrokerModes(t *testing.T) {
	allowed, err := specutil.NewBrokerPolicy([]string{matrixBrokerEndpoint})
	require.NoError(t, err)
	for _, cloud := range []string{CloudProviderAWS, CloudProviderAliyun, CloudProviderTencent, CloudProviderGCP, CloudProviderAzure} {
		for _, iam := range []string{"true", "false", ""} {
			mode := iam
			if mode == "" {
				mode = "omitted"
			}
			for _, format := range []string{FormatParquet, FormatMilvusTable} {
				t.Run(cloud+"/"+mode+"/"+format, func(t *testing.T) {
					source, values := brokerModeSpec(cloud, iam)
					spec := marshalBrokerSpec(t, format, values)
					require.NoError(t, ValidateSourceAndSpec(source, spec, allowed))
					require.ErrorContains(t, ValidateSourceAndSpec(source, spec, nil), "credentialBrokerAllowedEndpoints")
				})
			}
		}
	}
}

func TestAzureBrokerIAMRequiredFieldsAndConflicts(t *testing.T) {
	allowed, err := specutil.NewBrokerPolicy([]string{matrixBrokerEndpoint})
	require.NoError(t, err)
	t.Run("missing "+ExtfsKeyAccessKeyID, func(t *testing.T) {
		source, values := brokerModeSpec(CloudProviderAzure, "true")
		delete(values, ExtfsKeyAccessKeyID)
		err := ValidateSourceAndSpec(source, marshalBrokerSpec(t, FormatParquet, values), allowed)
		require.ErrorIs(t, err, merr.ErrParameterMissing)
		require.ErrorContains(t, err, "extfs."+ExtfsKeyAccessKeyID)
	})
	// Without an endpoint the same flag selects the node's workload identity,
	// not the Broker's: the spec is admitted and needs no Broker policy.
	t.Run("without endpoint is node identity", func(t *testing.T) {
		source, values := brokerModeSpec(CloudProviderAzure, "true")
		delete(values, ExtfsKeyCredentialEndpoint)
		require.NoError(t, ValidateSourceAndSpec(source, marshalBrokerSpec(t, FormatParquet, values), nil))
	})
	_, withoutRegion := brokerModeSpec(CloudProviderAzure, "true")
	delete(withoutRegion, ExtfsKeyRegion)
	withoutRegionSpec := marshalBrokerSpec(t, FormatParquet, withoutRegion)
	// Without a region the source must name the storage authority itself. A
	// container root needs its trailing slash: the storage URI parser requires
	// the separator after the container.
	for _, source := range []string{
		"azure://core.windows.net/container/path",
		"azure://core.windows.net/container/",
		"azure://core.chinacloudapi.cn/container/",
	} {
		t.Run("no region "+source, func(t *testing.T) {
			require.NoError(t, ValidateSourceAndSpec(source, withoutRegionSpec, allowed))
		})
	}
	for _, iam := range []string{"true", "false", ""} {
		keys := []string{ExtfsKeyAccessKeyValue, ExtfsKeyRoleARN, ExtfsKeyGCPTargetServiceAccount, ExtfsKeyAnonymous}
		if iam == "true" {
			keys = append(keys, ExtfsKeyExternalID, ExtfsKeySessionName, ExtfsKeyAzureClientID, ExtfsKeyAzureTenantID)
		}
		for _, key := range keys {
			t.Run(iam+" rejects "+key, func(t *testing.T) {
				source, values := brokerModeSpec(CloudProviderAzure, iam)
				values[key] = "inactive"
				if key == ExtfsKeyAnonymous {
					values[key] = "true"
				}
				err := ValidateSourceAndSpec(source, marshalBrokerSpec(t, FormatParquet, values), allowed)
				require.ErrorIs(t, err, merr.ErrParameterInvalid)
				require.ErrorContains(t, err, "extfs."+key)
				require.NotContains(t, err.Error(), "inactive")
			})
		}
	}
	// Target mode predates Broker IAM: persisted specs may carry the S3 role
	// fields, which Azure never reads.
	for _, iam := range []string{"false", ""} {
		for _, key := range []string{ExtfsKeyExternalID, ExtfsKeySessionName} {
			t.Run(iam+" keeps "+key, func(t *testing.T) {
				source, values := brokerModeSpec(CloudProviderAzure, iam)
				values[key] = "unused"
				require.NoError(t, ValidateSourceAndSpec(source, marshalBrokerSpec(t, FormatParquet, values), allowed))
			})
		}
	}
	for _, key := range []string{ExtfsKeyAzureClientID, ExtfsKeyAzureTenantID, ExtfsKeyRegion} {
		for _, iam := range []string{"false", ""} {
			t.Run(iam+" needs "+key, func(t *testing.T) {
				source, values := brokerModeSpec(CloudProviderAzure, iam)
				delete(values, key)
				err := ValidateSourceAndSpec(source, marshalBrokerSpec(t, FormatParquet, values), allowed)
				require.ErrorIs(t, err, merr.ErrParameterMissing)
				require.ErrorContains(t, err, "extfs."+key)
			})
		}
	}
	for _, duration := range []string{"899", "3601", "bad"} {
		source, values := brokerModeSpec(CloudProviderAzure, "true")
		values[ExtfsKeyLoadFrequency] = duration
		require.ErrorContains(t, ValidateSourceAndSpec(source, marshalBrokerSpec(t, FormatParquet, values), allowed), "[900, 3600]")
	}
}

// Broker IAM attaches a SAS to every request: milvus-storage sends it only to
// an Azure Storage authority over HTTPS, the storage URI parser needs a
// separator after the container, and the Broker rejects invalid container
// names. A local probe confirmed that each rejected shape fails at read.
func TestAzureBrokerIAMSourceShapes(t *testing.T) {
	allowed, err := specutil.NewBrokerPolicy([]string{matrixBrokerEndpoint})
	require.NoError(t, err)
	withRegion := func(map[string]string) {}
	withoutRegion := func(values map[string]string) { delete(values, ExtfsKeyRegion) }
	plainHTTP := func(values map[string]string) { values[ExtfsKeyUseSSL] = "false" }
	const authority = "lowercase Azure Storage authority"
	const container = "valid container name followed by"
	const containerHost = "an Azure Storage authority or a valid container name"
	const unresolved = "cannot resolve endpoint"
	for _, tc := range []struct {
		iam    string
		source string
		edit   func(map[string]string)
		want   string // empty when admitted
	}{
		// azure://<authority>/<container>/[<path>], with or without a region.
		{"true", "azure://core.windows.net/container/path", withRegion, ""},
		{"true", "azure://core.windows.net/container/", withRegion, ""},
		{"true", "azure://core.usgovcloudapi.net/container/path", withoutRegion, ""},
		{"true", "azure://core.cloudapi.de/container/path", withoutRegion, ""},
		{"true", "azure://CORE.WINDOWS.NET/container/path", withoutRegion, authority},
		{"true", "azure://CORE.WINDOWS.NET/container/path", withRegion, authority},
		{"true", "azure://storage.example.com/container/", withoutRegion, authority},
		{"true", "azure://account.blob.core.windows.net/container/path", withRegion, authority},
		{"true", "azure://core.windows.net:443/container/path", withoutRegion, authority},
		{"true", "azure://core.windows.net/container", withRegion, container},
		{"true", "azure://core.windows.net/container", withoutRegion, container},
		{"true", "azure://core.windows.net/", withRegion, container},
		{"true", "azure://core.windows.net", withRegion, container},
		{"true", "azure://core.windows.net/My_Container/path", withRegion, container},
		{"true", "azure://core.windows.net/my--container/path", withRegion, container},
		// The reader splits the raw source, so an encoded separator or letter
		// is not the container name.
		{"true", "azure://core.windows.net/container%2Fblob", withRegion, container},
		{"true", "azure://core.windows.net/cont%61iner/path", withoutRegion, container},
		// Container names have 3 to 63 characters.
		{"true", "azure://core.windows.net/abc/path", withRegion, ""},
		{"true", "azure://core.windows.net/ab/path", withRegion, container},
		{"true", "azure://core.windows.net/" + strings.Repeat("a", 63) + "/path", withRegion, ""},
		{"true", "azure://core.windows.net/" + strings.Repeat("a", 64) + "/path", withRegion, container},
		{"true", "azure://ab/path", withRegion, containerHost},
		// azure://<container>/[<path>] takes its authority from the region.
		{"true", "azure://container/dir/file.parquet", withRegion, ""},
		{"true", "azure://container", withRegion, ""},
		{"true", "azure://container/dir/file.parquet", withoutRegion, unresolved},
		{"true", "azure://container/dir/", withoutRegion, unresolved},
		{"true", "azure://MyContainer/path", withRegion, containerHost},
		{"true", "azure://localhost:10000/container/", withRegion, containerHost},
		{"true", "azure://core.windows.net/container/path", plainHTTP, "extfs.use_ssl=false"},
		// Target mode predates this rule and refresh revalidates persisted
		// specs, so its source admission is unchanged.
		{"false", "azure://account.blob.core.windows.net/container/path", withRegion, ""},
		{"", "azure://core.windows.net/container/path", plainHTTP, ""},
	} {
		_, values := brokerModeSpec(CloudProviderAzure, tc.iam)
		tc.edit(values)
		name := fmt.Sprintf("use_iam=%q region=%q use_ssl=%q %s", tc.iam, values[ExtfsKeyRegion], values[ExtfsKeyUseSSL], tc.source)
		t.Run(name, func(t *testing.T) {
			err := ValidateSourceAndSpec(tc.source, marshalBrokerSpec(t, FormatParquet, values), allowed)
			if tc.want == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, merr.ErrParameterInvalid)
			require.ErrorContains(t, err, tc.want)
		})
	}
}

// A Broker lifetime is a plain decimal number of seconds: milvus-storage
// parses it with std::from_chars, which rejects a sign.
func TestBrokerCredentialLifetimeFormat(t *testing.T) {
	allowed, err := specutil.NewBrokerPolicy([]string{matrixBrokerEndpoint})
	require.NoError(t, err)
	type mode struct{ cloud, iam string }
	check := func(m mode, lifetime string) error {
		source, values := brokerModeSpec(m.cloud, m.iam)
		values[ExtfsKeyLoadFrequency] = lifetime
		return ValidateSourceAndSpec(source, marshalBrokerSpec(t, FormatParquet, values), allowed)
	}
	for _, m := range []mode{
		{CloudProviderAWS, "true"},
		{CloudProviderAWS, ""},
		{CloudProviderAliyun, "true"},
		{CloudProviderAliyun, ""},
		{CloudProviderTencent, "true"},
		{CloudProviderTencent, ""},
		{CloudProviderGCP, "true"},
		{CloudProviderGCP, ""},
		{CloudProviderAzure, "true"},
	} {
		for _, lifetime := range []string{"900", "0900", "3600"} {
			t.Run(fmt.Sprintf("%s use_iam=%q accepts %q", m.cloud, m.iam, lifetime), func(t *testing.T) {
				require.NoError(t, check(m, lifetime))
			})
		}
		for _, lifetime := range []string{"+900", "-900", " 900", "900 ", "9e2", "0x384", "900.0", "899", "3601", "99999999999999999999"} {
			t.Run(fmt.Sprintf("%s use_iam=%q rejects %q", m.cloud, m.iam, lifetime), func(t *testing.T) {
				err := check(m, lifetime)
				require.ErrorIs(t, err, merr.ErrParameterInvalid)
				require.ErrorContains(t, err, "[900, 3600]")
			})
		}
	}
	// Azure target mode predates this rule and never checked the lifetime.
	// Refresh revalidates persisted specs, so it stays unchecked.
	for _, lifetime := range []string{"100", "abc", "+900"} {
		t.Run("azure target keeps "+lifetime, func(t *testing.T) {
			require.NoError(t, check(mode{CloudProviderAzure, ""}, lifetime))
		})
	}
}

// Without a region, a dotted azure:// host at a container root is the storage
// address. The reader needs the separator after the container and splits the
// raw source, and the rule applies to Azure only.
func TestAzureContainerRootWithoutRegion(t *testing.T) {
	spec := func(cloud string) string {
		return marshalBrokerSpec(t, FormatParquet, map[string]string{
			ExtfsKeyCloudProvider:  cloud,
			ExtfsKeyAccessKeyID:    "storageaccount",
			ExtfsKeyAccessKeyValue: "key",
		})
	}
	for _, source := range []string{
		"azure://core.windows.net/container/",
		"azure://storage.example.com/container/",
	} {
		t.Run("admits "+source, func(t *testing.T) {
			require.NoError(t, ValidateSourceAndSpec(source, spec(CloudProviderAzure)))
		})
	}
	for _, tc := range []struct{ cloud, source string }{
		{CloudProviderAzure, "azure://core.windows.net/container"},
		{CloudProviderAzure, "azure://core.windows.net/container%2F"},
		{CloudProviderAzure, "azure://core.windows.net/"},
		{CloudProviderAzure, "azure://container/dir/"},
		{CloudProviderAliyun, "oss://storage.example.com/bucket/"},
	} {
		t.Run("rejects "+tc.source, func(t *testing.T) {
			err := ValidateSourceAndSpec(tc.source, spec(tc.cloud))
			require.ErrorIs(t, err, merr.ErrParameterInvalid)
			require.ErrorContains(t, err, "cannot resolve endpoint")
		})
	}
}

// Admission errors are returned to the caller and logged. A source may carry
// a signed token in its query, so no error repeats it.
func TestAzureSourceErrorsOmitQuery(t *testing.T) {
	allowed, err := specutil.NewBrokerPolicy([]string{matrixBrokerEndpoint})
	require.NoError(t, err)
	_, iam := brokerModeSpec(CloudProviderAzure, "true")
	delete(iam, ExtfsKeyRegion)
	staticKeys := map[string]string{
		ExtfsKeyCloudProvider:  CloudProviderAzure,
		ExtfsKeyAccessKeyID:    "storageaccount",
		ExtfsKeyAccessKeyValue: "key",
	}
	for _, tc := range []struct {
		name   string
		source string
		values map[string]string
	}{
		{"broker IAM container without separator", "azure://core.windows.net/container?sig=SECRETSAS", iam},
		{"broker IAM container without region", "azure://container/dir?sig=SECRETSAS", iam},
		{"static keys without region", "azure://container/dir?sig=SECRETSAS", staticKeys},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateSourceAndSpec(tc.source, marshalBrokerSpec(t, FormatParquet, tc.values), allowed)
			require.ErrorIs(t, err, merr.ErrParameterInvalid)
			require.NotContains(t, err.Error(), "SECRETSAS")
		})
	}
}

func TestBrokerRejectsUnsupportedProvider(t *testing.T) {
	err := ValidateExtfsComplete("obs://bucket/key", map[string]string{
		ExtfsKeyCloudProvider:      CloudProviderHuawei,
		ExtfsKeyRegion:             "cn-north-4",
		ExtfsKeyUseIAM:             "true",
		ExtfsKeyCredentialEndpoint: matrixBrokerEndpoint,
	})
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
	require.ErrorContains(t, err, "aws, aliyun, tencent, gcp or azure")
}

func TestAzureBrokerAliasesForBothModes(t *testing.T) {
	allowed, err := specutil.NewBrokerPolicy([]string{matrixBrokerEndpoint})
	require.NoError(t, err)
	for _, iam := range []string{"true", "false", ""} {
		t.Run(iam, func(t *testing.T) {
			source, values := brokerModeSpec(CloudProviderAzure, iam)
			delete(values, ExtfsKeyCredentialEndpoint)
			values[ExtfsKeyAzureCredentialEndpoint] = matrixBrokerEndpoint
			require.NoError(t, ValidateSourceAndSpec(source, marshalBrokerSpec(t, FormatParquet, values), allowed))
			values[ExtfsKeyCredentialEndpoint] = "HTTPS://BROKER.example:443/token?scope=a,b"
			require.NoError(t, ValidateSourceAndSpec(source, marshalBrokerSpec(t, FormatParquet, values), allowed))
			values[ExtfsKeyCredentialEndpoint] = matrixBrokerEndpoint + "&extra=1"
			require.ErrorContains(t, ValidateSourceAndSpec(source, marshalBrokerSpec(t, FormatParquet, values), allowed), "conflicts")
		})
	}
}

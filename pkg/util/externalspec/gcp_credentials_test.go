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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/externalspec/specutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestGCPCredentialAdmission(t *testing.T) {
	const target = "reader@project.iam.gserviceaccount.com"
	const broker = "https://broker.example/token?scope=a,b"
	policy, err := specutil.NewBrokerPolicy([]string{broker})
	require.NoError(t, err)
	cases := []struct {
		name   string
		source string
		values map[string]string
		want   string
	}{
		{"target alone", "gs://bucket/key", nil, ""},
		{"target absorbs IAM", "gcs://bucket/key", map[string]string{ExtfsKeyUseIAM: "true"}, ""},
		{"direct role alone", "gs://bucket/key", map[string]string{ExtfsKeyGCPTargetServiceAccount: "", ExtfsKeyRoleARN: "example-role"}, "use extfs.gcp_target_service_account"},
		{"direct role with node IAM", "gs://bucket/key", map[string]string{ExtfsKeyGCPTargetServiceAccount: "", ExtfsKeyRoleARN: "example-role", ExtfsKeyUseIAM: "true"}, "use extfs.gcp_target_service_account"},
		{"broker", "gs://storage.googleapis.com/bucket/key", map[string]string{ExtfsKeyCredentialEndpoint: broker}, ""},
		{"broker IAM conflicts", "gs://bucket/key", map[string]string{ExtfsKeyCredentialEndpoint: broker, ExtfsKeyUseIAM: "true"}, "conflicts"},
		{"broker needs target", "gs://bucket/key", map[string]string{ExtfsKeyGCPTargetServiceAccount: "", ExtfsKeyCredentialEndpoint: broker}, "gcp_target_service_account is required"},
		{"wrong provider", "gs://bucket/key", map[string]string{ExtfsKeyCloudProvider: "aws", ExtfsKeyRegion: "us-east-1"}, "requires cloud_provider=gcp"},
		{"wrong endpoint", "gs://s3.amazonaws.com/bucket/key", nil, "*.googleapis.com"},
		{"custom endpoint", "s3://bucket/key", map[string]string{ExtfsKeyEndpointURL: "https://evil.example"}, "*.googleapis.com"},
		{"lookalike endpoint", "s3://bucket/key", map[string]string{ExtfsKeyEndpointURL: "https://storage.googleapis.com.evil.example"}, "*.googleapis.com"},
		{"HTTP endpoint", "s3://bucket/key", map[string]string{ExtfsKeyEndpointURL: "http://storage.googleapis.com"}, "HTTPS"},
		{"HTTP scheme override", "gs://bucket/key", map[string]string{ExtfsKeyUseSSL: "false"}, "HTTPS"},
		{"explicit Google endpoint", "s3://bucket/key", map[string]string{ExtfsKeyEndpointURL: "https://storage.googleapis.com"}, ""},
		{"Google root is not storage endpoint", "s3://bucket/key", map[string]string{ExtfsKeyEndpointURL: "https://googleapis.com"}, "*.googleapis.com"},
		{"GCP endpoint suffix is case insensitive", "gs://STORAGE.GOOGLEAPIS.COM/bucket/key", nil, ""},
		{"lifetime low", "gs://bucket/key", map[string]string{ExtfsKeyLoadFrequency: "899"}, "[900, 3600]"},
		{"lifetime high", "gs://bucket/key", map[string]string{ExtfsKeyLoadFrequency: "3601"}, "[900, 3600]"},
		{"lifetime invalid", "gs://bucket/key", map[string]string{ExtfsKeyLoadFrequency: "abc"}, "[900, 3600]"},
		{"lifetime signed", "gs://bucket/key", map[string]string{ExtfsKeyLoadFrequency: "+900"}, "[900, 3600]"},
		{"lifetime min", "gs://bucket/key", map[string]string{ExtfsKeyLoadFrequency: "900"}, ""},
		{"lifetime max", "gs://bucket/key", map[string]string{ExtfsKeyLoadFrequency: "3600"}, ""},
		{"broker lifetime", "gs://bucket/key", map[string]string{ExtfsKeyCredentialEndpoint: broker, ExtfsKeyLoadFrequency: "3601"}, "[900, 3600]"},
		{"broker custom endpoint", "s3://bucket/key", map[string]string{ExtfsKeyCredentialEndpoint: broker, ExtfsKeyEndpointURL: "https://custom.example"}, "*.googleapis.com"},
		{"broker HTTP endpoint", "s3://bucket/key", map[string]string{ExtfsKeyCredentialEndpoint: broker, ExtfsKeyEndpointURL: "http://storage.googleapis.com"}, "HTTPS"},
		{"broker HTTP scheme override", "gs://bucket/key", map[string]string{ExtfsKeyCredentialEndpoint: broker, ExtfsKeyUseSSL: "false"}, "HTTPS"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			values := map[string]string{ExtfsKeyCloudProvider: CloudProviderGCP, ExtfsKeyGCPTargetServiceAccount: target, ExtfsKeyRegion: "us-east-1"}
			for k, v := range tc.values {
				values[k] = v
			}
			spec, err := json.Marshal(ExternalSpec{Format: FormatParquet, Extfs: values})
			require.NoError(t, err)
			err = ValidateSourceAndSpec(tc.source, string(spec), policy)
			if tc.want == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.want)
				require.Equal(t, merr.InputError, merr.GetErrorType(err))
			}
		})
	}
	spec := `{"format":"parquet","extfs":{"cloud_provider":"gcp","gcp_target_service_account":"` + target + `","credential_endpoint":"` + broker + `"}}`
	require.NoError(t, ValidateSourceAndSpec("gs://bucket/key", spec, policy))
	require.ErrorContains(t, ValidateSourceAndSpec("gs://bucket/key", spec), "credentialBrokerAllowedEndpoints")
	for _, extfs := range []string{
		`"cloud_provider":"gcp","use_iam":"true"`,
		`"cloud_provider":"gcp","gcp_target_service_account":"` + target + `"`,
		`"cloud_provider":"aws","region":"us-east-1","role_arn":"arn:aws:iam::1:role/reader"`,
	} {
		source := "gs://bucket/key"
		if strings.Contains(extfs, `"cloud_provider":"aws"`) {
			source = "s3://bucket/key"
		}
		spec := `{"format":"parquet","extfs":{` + extfs + `}}`
		require.NoError(t, ValidateSourceAndSpec(source, spec))
	}
}

// Existing node-IAM admission remains unchanged; the endpoint restrictions
// introduced for a target service account do not apply to this existing mode.
func TestGCPNodeIAMAdmissionCompatibility(t *testing.T) {
	for _, tc := range []struct {
		name   string
		source string
		values map[string]string
	}{
		{"custom HTTPS endpoint", "s3://bucket/key", map[string]string{ExtfsKeyEndpointURL: "https://custom.example", ExtfsKeyRegion: "us-east-1"}},
		{"SSL false", "gs://bucket/key", map[string]string{ExtfsKeyUseSSL: "false"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			values := map[string]string{ExtfsKeyCloudProvider: CloudProviderGCP, ExtfsKeyUseIAM: "true"}
			for k, v := range tc.values {
				values[k] = v
			}
			spec, err := json.Marshal(ExternalSpec{Format: FormatParquet, Extfs: values})
			require.NoError(t, err)
			require.NoError(t, ValidateSourceAndSpec(tc.source, string(spec)))
		})
	}
}

func TestGCSEndpointSourcePaths(t *testing.T) {
	allowed, err := specutil.NewBrokerPolicy([]string{matrixBrokerEndpoint})
	require.NoError(t, err)
	_, broker := brokerModeSpec(CloudProviderGCP, "true")
	for _, extfs := range []map[string]string{
		broker,
		{ExtfsKeyCloudProvider: CloudProviderGCP, ExtfsKeyAccessKeyID: "ak", ExtfsKeyAccessKeyValue: "sk"},
	} {
		spec := marshalBrokerSpec(t, FormatParquet, extfs)
		for _, source := range []string{
			"gs://storage.googleapis.com/events/",
			"gs://storage.googleapis.com:443/events/",
			"gcs://STORAGE.GOOGLEAPIS.COM:443/events/dir/file.parquet",
			"gs://storage.googleapis.com:8443/events/file.parquet",
			"gs://events", "gs://events/dir", "gcs://events.prod", "gcs://events.prod/",
		} {
			require.NoError(t, ValidateSourceAndSpec(source, spec, allowed), source)
		}
		for _, source := range []string{
			"gs://storage.googleapis.com/events", "gcs://storage.googleapis.com:443/events",
			"gs://storage.googleapis.com", "gs://storage.googleapis.com/",
			"gs://storage.googleapis.com//dir", "gs://storage.googleapis.com/events%2Ffile",
			"gs://storage.googleapis.com/events?query=/private", "gs://storage.googleapis.com/events#fragment/",
		} {
			err := ValidateSourceAndSpec(source, spec, allowed)
			require.ErrorIs(t, err, merr.ErrParameterInvalid, source)
			require.ErrorContains(t, err, "trailing slash", source)
			require.NotContains(t, err.Error(), "private")
		}
		for _, port := range []string{":", ":0", ":65536"} {
			require.ErrorContains(t, ValidateSourceAndSpec("gs://storage.googleapis.com"+port+"/events/", spec, allowed), "port")
		}
		extfs[ExtfsKeyBucketName] = "events"
		withBucket := marshalBrokerSpec(t, FormatParquet, extfs)
		for _, source := range []string{"gs://storage.googleapis.com/events%2F", "gcs://storage.googleapis.com/events%2Ffile"} {
			require.NoError(t, ValidateSourceAndSpec(source, withBucket, allowed), source)
		}
		require.ErrorContains(t, ValidateSourceAndSpec("gs://storage.googleapis.com/events", withBucket, allowed), "trailing slash")
	}
}

func TestGCPBrokerRejectsInactiveFieldsAtAdmission(t *testing.T) {
	const broker = "https://broker.example/token"
	allowed, err := specutil.NewBrokerPolicy([]string{broker})
	require.NoError(t, err)
	for _, key := range []string{
		ExtfsKeyRoleARN, ExtfsKeyExternalID, ExtfsKeySessionName,
		ExtfsKeyAccessKeyID, ExtfsKeyAccessKeyValue,
		ExtfsKeyAzureClientID, ExtfsKeyAzureTenantID,
	} {
		t.Run(key, func(t *testing.T) {
			values := map[string]string{
				ExtfsKeyCloudProvider:           CloudProviderGCP,
				ExtfsKeyGCPTargetServiceAccount: "reader@project.iam.gserviceaccount.com",
				ExtfsKeyCredentialEndpoint:      broker,
				key:                             "unused-field-value",
			}
			spec, err := json.Marshal(ExternalSpec{Format: FormatParquet, Extfs: values})
			require.NoError(t, err)
			for _, policy := range []*specutil.BrokerPolicy{nil, allowed} {
				err := ValidateSourceAndSpec("gs://bucket/key", string(spec), policy)
				require.ErrorIs(t, err, merr.ErrParameterInvalid)
				require.ErrorContains(t, err, "extfs."+key)
				require.NotContains(t, err.Error(), "unused-field-value")
			}
		})
	}
}

func TestGCPBrokerIAMAdmission(t *testing.T) {
	const broker = "https://broker.example/token"
	allowed, err := specutil.NewBrokerPolicy([]string{broker})
	require.NoError(t, err)
	cases := []struct {
		name   string
		source string
		values map[string]string
		want   string
	}{
		{"broker IAM", "gs://bucket/key", nil, ""},
		{"false requires target", "gs://bucket/key", map[string]string{ExtfsKeyUseIAM: "false"}, "gcp_target_service_account is required"},
		{"missing IAM requires target", "gs://bucket/key", map[string]string{ExtfsKeyUseIAM: ""}, "gcp_target_service_account is required"},
		{"IAM conflicts with target", "gs://bucket/key", map[string]string{ExtfsKeyGCPTargetServiceAccount: "reader@project.iam.gserviceaccount.com"}, "gcp_target_service_account"},
		{"false uses target", "gs://bucket/key", map[string]string{ExtfsKeyUseIAM: "false", ExtfsKeyGCPTargetServiceAccount: "reader@project.iam.gserviceaccount.com"}, ""},
		{"HTTPS custom endpoint", "s3://bucket/key", map[string]string{ExtfsKeyEndpointURL: "https://custom.example", ExtfsKeyRegion: "us-east-1"}, "*.googleapis.com"},
		{"HTTP endpoint", "s3://bucket/key", map[string]string{ExtfsKeyEndpointURL: "http://storage.googleapis.com", ExtfsKeyRegion: "us-east-1"}, "HTTPS"},
		{"SSL false", "gs://bucket/key", map[string]string{ExtfsKeyUseSSL: "false"}, "HTTPS"},
		{"lifetime low", "gs://bucket/key", map[string]string{ExtfsKeyLoadFrequency: "899"}, "[900, 3600]"},
		{"lifetime high", "gs://bucket/key", map[string]string{ExtfsKeyLoadFrequency: "3601"}, "[900, 3600]"},
		{"lifetime min", "gs://bucket/key", map[string]string{ExtfsKeyLoadFrequency: "900"}, ""},
		{"lifetime max", "gs://bucket/key", map[string]string{ExtfsKeyLoadFrequency: "3600"}, ""},
	}
	for _, key := range []string{ExtfsKeyRoleARN, ExtfsKeyExternalID, ExtfsKeySessionName, ExtfsKeyAccessKeyID, ExtfsKeyAccessKeyValue, ExtfsKeyAzureClientID, ExtfsKeyAzureTenantID} {
		cases = append(cases, struct {
			name, source string
			values       map[string]string
			want         string
		}{"inactive " + key, "gs://bucket/key", map[string]string{key: "inactive"}, "extfs." + key})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			values := map[string]string{ExtfsKeyCloudProvider: CloudProviderGCP, ExtfsKeyUseIAM: "true", ExtfsKeyCredentialEndpoint: broker}
			for key, value := range tc.values {
				values[key] = value
			}
			if tc.name == "missing IAM requires target" {
				delete(values, ExtfsKeyUseIAM)
			}
			spec, err := json.Marshal(ExternalSpec{Format: FormatParquet, Extfs: values})
			require.NoError(t, err)
			err = ValidateSourceAndSpec(tc.source, string(spec), allowed)
			if tc.want == "" {
				require.NoError(t, err)
				require.ErrorContains(t, ValidateSourceAndSpec(tc.source, string(spec)), "credentialBrokerAllowedEndpoints")
			} else {
				require.ErrorContains(t, err, tc.want)
				require.Equal(t, merr.InputError, merr.GetErrorType(err))
			}
		})
	}
}

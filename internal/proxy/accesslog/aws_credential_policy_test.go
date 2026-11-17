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

package accesslog

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type accessLogCredentialTransport func(*http.Request) (*http.Response, error)

func (f accessLogCredentialTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

// Exercise the access-log uploader's independent factory and real S3 signer.
// Credential HTTP is intercepted; object-storage requests use loopback only.
func TestAccessLogAWSIAMPolicy(t *testing.T) {
	for _, name := range []string{"metadata_disabled", "profile_irsa", "unreadable_irsa"} {
		t.Run(name, func(t *testing.T) {
			for _, key := range []string{
				"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN",
				"AWS_ROLE_ARN", "AWS_ROLE_SESSION_NAME", "AWS_WEB_IDENTITY_TOKEN_FILE",
				"AWS_PROFILE", "AWS_DEFAULT_PROFILE", "AWS_CONFIG_FILE", "AWS_SHARED_CREDENTIALS_FILE",
				"AWS_REGION", "AWS_DEFAULT_REGION", "AWS_EC2_METADATA_DISABLED", "AWS_EC2_METADATA_ENABLE",
				"AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_CONTAINER_CREDENTIALS_FULL_URI",
				"AWS_CONTAINER_AUTHORIZATION_TOKEN", "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
				"AWS_EC2_METADATA_SERVICE_ENDPOINT",
			} {
				t.Setenv(key, "")
			}
			dir := t.TempDir()
			configPath := filepath.Join(dir, "config")
			t.Setenv("AWS_CONFIG_FILE", configPath)
			t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(dir, "missing-credentials"))
			t.Setenv("AWS_PROFILE", "accesslog-policy")
			t.Setenv("AWS_ACCESS_KEY_ID", "IGNORED_ENV_AK")
			t.Setenv("AWS_SECRET_ACCESS_KEY", "SYNTHETIC_ENV_SECRET")
			t.Setenv("AWS_EC2_METADATA_DISABLED", "TrUe")
			if name != "metadata_disabled" {
				tokenPath := filepath.Join(dir, "token")
				if name == "profile_irsa" {
					require.NoError(t, os.WriteFile(tokenPath, []byte("synthetic-web-token"), 0o600))
				}
				profile := fmt.Sprintf("[profile accesslog-policy]\nweb_identity_token_file = %s\nrole_arn = arn:aws:iam::123456789012:role/accesslog\nrole_session_name = accesslog-test\nregion = us-east-1\n", tokenPath)
				require.NoError(t, os.WriteFile(configPath, []byte(profile), 0o600))
				// A configured profile IRSA must win over this container source.
				t.Setenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", "http://127.0.0.1:1/container")
			}
			var stsRequests, unexpectedCredentialRequests, s3Requests atomic.Int32
			originalTransport := http.DefaultTransport
			http.DefaultTransport = accessLogCredentialTransport(func(r *http.Request) (*http.Response, error) {
				if r.URL.Host != "sts.fixture.test" {
					unexpectedCredentialRequests.Add(1)
					return nil, fmt.Errorf("unexpected credential request to %s", r.URL.Host)
				}
				stsRequests.Add(1)
				if err := r.ParseForm(); err != nil {
					return nil, err
				}
				if r.Form.Get("RoleArn") != "arn:aws:iam::123456789012:role/accesslog" || r.Form.Get("WebIdentityToken") != "synthetic-web-token" {
					return nil, fmt.Errorf("unexpected Web Identity request")
				}
				body := `<AssumeRoleWithWebIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><AssumeRoleWithWebIdentityResult><Credentials><AccessKeyId>PROFILE_AK</AccessKeyId><SecretAccessKey>SYNTHETIC_SECRET</SecretAccessKey><SessionToken>SYNTHETIC_SESSION</SessionToken><Expiration>2099-01-01T00:00:00Z</Expiration></Credentials></AssumeRoleWithWebIdentityResult></AssumeRoleWithWebIdentityResponse>`
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
			})
			t.Cleanup(func() { http.DefaultTransport = originalTransport })
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				s3Requests.Add(1)
				if !strings.Contains(r.Header.Get("Authorization"), "Credential=PROFILE_AK/") {
					t.Error("access-log request did not use the profile IRSA identity")
				}
				if r.Method == http.MethodGet && r.URL.Query().Has("location") {
					_, _ = io.WriteString(w, `<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">us-east-1</LocationConstraint>`)
					return
				}
				w.WriteHeader(http.StatusOK)
			}))
			defer server.Close()
			previousAttempts := CheckBucketRetryAttempts
			CheckBucketRetryAttempts = 1
			t.Cleanup(func() { CheckBucketRetryAttempts = previousAttempts })
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_, err := newMinioClient(ctx, config{
				address: strings.TrimPrefix(server.URL, "http://"), bucketName: "accesslog-policy",
				useIAM: true, iamEndpoint: "https://sts.fixture.test",
			})
			if name == "profile_irsa" {
				require.NoError(t, err)
				require.Equal(t, int32(1), stsRequests.Load())
				require.Positive(t, s3Requests.Load())
			} else {
				require.Error(t, err)
				require.Zero(t, stsRequests.Load())
				require.Zero(t, s3Requests.Load())
			}
			require.Zero(t, unexpectedCredentialRequests.Load())
		})
	}
}

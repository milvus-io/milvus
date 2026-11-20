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

package objectstorage

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

	miniosdk "github.com/minio/minio-go/v7"
)

type policyCredentialTransport struct {
	imds, sts, pod, ecs, unexpected atomic.Int32
	failSTS, failPod                bool
	wantRole, wantSession           string
}

func (p *policyCredentialTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	response := func(body string) *http.Response {
		return &http.Response{StatusCode: 200, Status: "200 OK", Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: r}
	}
	credentials := func(key string) string {
		return fmt.Sprintf(`{"Code":"Success","AccessKeyId":%q,"SecretAccessKey":"SYNTHETIC_SECRET","Token":"SYNTHETIC_SESSION","Expiration":"2099-01-01T00:00:00Z"}`, key)
	}
	switch r.URL.Host {
	case "169.254.169.254":
		p.imds.Add(1)
		switch r.URL.Path {
		case "/latest/api/token":
			return response("synthetic-imds-token"), nil
		case "/latest/meta-data/iam/security-credentials/":
			return response("synthetic-node-role"), nil
		case "/latest/meta-data/iam/security-credentials/synthetic-node-role":
			return response(credentials("NODE_AK")), nil
		}
	case "sts.us-east-1.amazonaws.com":
		p.sts.Add(1)
		if err := r.ParseForm(); err != nil {
			return nil, err
		}
		if r.Form.Get("RoleArn") != p.wantRole || r.Form.Get("RoleSessionName") != p.wantSession || r.Form.Get("WebIdentityToken") != "synthetic-irsa-token" {
			return nil, fmt.Errorf("unexpected Web Identity request fields")
		}
		if p.failSTS {
			resp := response(`<ErrorResponse><Error><Code>AccessDenied</Code><Message>synthetic STS failure</Message></Error></ErrorResponse>`)
			resp.StatusCode = http.StatusForbidden
			return resp, nil
		}
		return response(`<AssumeRoleWithWebIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><AssumeRoleWithWebIdentityResult><Credentials><AccessKeyId>IRSA_AK</AccessKeyId><SecretAccessKey>SYNTHETIC_SECRET</SecretAccessKey><SessionToken>SYNTHETIC_SESSION</SessionToken><Expiration>2099-01-01T00:00:00Z</Expiration></Credentials></AssumeRoleWithWebIdentityResult></AssumeRoleWithWebIdentityResponse>`), nil
	case "169.254.170.2":
		p.ecs.Add(1)
		if r.URL.Path == "/relative-credentials" {
			return response(credentials("ECS_AK")), nil
		}
	case "169.254.170.23":
		p.pod.Add(1)
		if p.failPod {
			resp := response("synthetic container failure")
			resp.StatusCode = http.StatusForbidden
			return resp, nil
		}
		if r.Header.Get("Authorization") != "synthetic-pod-token" {
			return nil, fmt.Errorf("unexpected pod authorization")
		}
		if r.URL.Path == "/v1/credentials" {
			return response(credentials("POD_AK")), nil
		}
	}
	p.unexpected.Add(1)
	return nil, fmt.Errorf("unexpected credential request to %s", r.URL.Host)
}

// The actual factory constructs an S3 client. Credential endpoints are mocked;
// the S3 request is sent only to this test's loopback HTTP server and signed by
// the real MinIO signer. Never parallelize these tests: IAM captures the process
// http.DefaultTransport, which is replaced and restored for each subtest.
func TestAWSCredentialPolicyFactory(t *testing.T) {
	cases := []struct {
		name, disabled, legacyEnable, wantKey string
		env, irsa, pod, explicit, wantError   bool
		profile, envToken, envRole, badToken  bool
		failSTS, failPod, relative            bool
		wantIMDS, wantPod, wantSTS, wantECS   int32
	}{
		{name: "metadata_disabled_does_not_disable_irsa", env: true, irsa: true, disabled: "true", wantKey: "IRSA_AK", wantSTS: 1},
		{name: "environment_only_respects_metadata_disabled", env: true, disabled: "true", wantError: true},
		{name: "metadata_enabled_when_disabled_unset", env: true, wantKey: "NODE_AK", wantIMDS: 3},
		{name: "metadata_enabled_when_disabled_false", env: true, disabled: "false", wantKey: "NODE_AK", wantIMDS: 3},
		{name: "metadata_disabled_case_insensitive", env: true, disabled: "TrUe", wantError: true},
		{name: "legacy_enable_false_is_ignored", env: true, legacyEnable: "false", wantKey: "NODE_AK", wantIMDS: 3},
		{name: "legacy_enable_true_cannot_override_disabled", env: true, legacyEnable: "true", disabled: "true", wantError: true},
		{name: "metadata_disabled_does_not_disable_pod_identity", env: true, pod: true, disabled: "true", wantKey: "POD_AK", wantPod: 1},
		{name: "profile_irsa_precedes_pod_identity", env: true, profile: true, pod: true, disabled: "true", wantKey: "IRSA_AK", wantSTS: 1},
		{name: "environment_token_uses_profile_role", profile: true, envToken: true, pod: true, wantKey: "IRSA_AK", wantSTS: 1},
		{name: "profile_token_uses_environment_role", profile: true, envRole: true, pod: true, wantKey: "IRSA_AK", wantSTS: 1},
		{name: "unreadable_irsa_does_not_fall_back", irsa: true, pod: true, badToken: true, wantError: true},
		{name: "failed_sts_does_not_fall_back", profile: true, pod: true, failSTS: true, wantError: true, wantSTS: 1},
		{name: "failed_container_does_not_fall_back", pod: true, failPod: true, wantError: true, wantPod: 1},
		{name: "ecs_relative_precedes_pod_identity", pod: true, relative: true, wantKey: "ECS_AK", wantECS: 1},
		{name: "explicit_keys_ignore_ambient", env: true, irsa: true, explicit: true, disabled: "true", wantKey: "EXPLICIT_AK"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, name := range []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_ROLE_ARN", "AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_ROLE_SESSION_NAME", "AWS_REGION", "AWS_DEFAULT_REGION", "AWS_CONTAINER_CREDENTIALS_FULL_URI", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_CONTAINER_AUTHORIZATION_TOKEN", "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE", "AWS_EC2_METADATA_ENABLE", "AWS_EC2_METADATA_DISABLED", "AWS_PROFILE", "AWS_DEFAULT_PROFILE", "AWS_CONFIG_FILE", "AWS_SHARED_CREDENTIALS_FILE", "AWS_EC2_METADATA_SERVICE_ENDPOINT"} {
				t.Setenv(name, "")
				if err := os.Unsetenv(name); err != nil {
					t.Fatal(err)
				}
			}
			t.Setenv("AWS_REGION", "us-east-1")
			t.Setenv("AWS_PROFILE", "credential-policy")
			t.Setenv("AWS_CONFIG_FILE", filepath.Join(t.TempDir(), "missing-config"))
			t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "missing-credentials"))
			if tc.disabled != "" {
				t.Setenv("AWS_EC2_METADATA_DISABLED", tc.disabled)
			}
			if tc.legacyEnable != "" {
				t.Setenv("AWS_EC2_METADATA_ENABLE", tc.legacyEnable)
			}
			if tc.env {
				t.Setenv("AWS_ACCESS_KEY_ID", "ENV_AK")
				t.Setenv("AWS_SECRET_ACCESS_KEY", "SYNTHETIC_ENV_SECRET")
				t.Setenv("AWS_SESSION_TOKEN", "SYNTHETIC_ENV_TOKEN")
			}
			writeToken := func(name, value string) string {
				p := filepath.Join(t.TempDir(), name)
				if err := os.WriteFile(p, []byte(value), 0o600); err != nil {
					t.Fatal(err)
				}
				return p
			}
			// Static profile keys are intentionally present even for IAM tests.
			t.Setenv("AWS_SHARED_CREDENTIALS_FILE", writeToken("credentials", "[credential-policy]\naws_access_key_id = STATIC_PROFILE_AK\naws_secret_access_key = SYNTHETIC_PROFILE_SECRET\n"))
			wantRole := "arn:aws:iam::123456789012:role/environment"
			wantSession := "environment-session"
			if tc.profile {
				wantRole = "arn:aws:iam::123456789012:role/profile"
				wantSession = "profile-session"
				tokenPath := writeToken("profile-token", "synthetic-irsa-token")
				if tc.envToken {
					t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", writeToken("environment-token", "synthetic-irsa-token"))
					tokenPath = filepath.Join(t.TempDir(), "unused-profile-token")
				}
				profile := fmt.Sprintf("[profile credential-policy]\nweb_identity_token_file = %s\nrole_arn = %s\nrole_session_name = %s\nregion = us-east-1\n", tokenPath, wantRole, wantSession)
				t.Setenv("AWS_CONFIG_FILE", writeToken("config", profile))
			}
			if tc.envRole {
				wantRole = "arn:aws:iam::123456789012:role/environment"
				wantSession = "environment-session"
				t.Setenv("AWS_ROLE_ARN", wantRole)
				t.Setenv("AWS_ROLE_SESSION_NAME", wantSession)
			}
			if tc.irsa {
				t.Setenv("AWS_ROLE_ARN", wantRole)
				t.Setenv("AWS_ROLE_SESSION_NAME", wantSession)
				tokenPath := writeToken("irsa-token", "synthetic-irsa-token")
				if tc.badToken {
					tokenPath = filepath.Join(t.TempDir(), "unreadable-token")
				}
				t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", tokenPath)
			}
			if tc.pod {
				t.Setenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", "http://169.254.170.23/v1/credentials")
				t.Setenv("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE", writeToken("pod-token", "synthetic-pod-token"))
			}
			if tc.relative {
				t.Setenv("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "/relative-credentials")
			}
			credentialHTTP := &policyCredentialTransport{failSTS: tc.failSTS, failPod: tc.failPod, wantRole: wantRole, wantSession: wantSession}
			originalTransport := http.DefaultTransport
			http.DefaultTransport = credentialHTTP
			t.Cleanup(func() { http.DefaultTransport = originalTransport })
			var s3Requests atomic.Int32
			var authorization atomic.Value
			authorization.Store("")
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				s3Requests.Add(1)
				authorization.Store(r.Header.Get("Authorization"))
				if r.Method != http.MethodHead || !strings.HasPrefix(r.URL.Path, "/credential-policy") {
					t.Errorf("unexpected S3 method/path: %s %s", r.Method, r.URL.Path)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				w.WriteHeader(http.StatusOK)
			}))
			defer server.Close()
			client, err := newPolicyFactoryClient(server.URL, !tc.explicit)
			if err != nil {
				t.Fatalf("factory returned error: %v", err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			exists, err := client.BucketExists(ctx, "credential-policy")
			if tc.wantError {
				if err == nil {
					t.Fatal("expected missing-credentials error")
				}
				if s3Requests.Load() != 0 {
					t.Errorf("credential failure sent %d S3 requests", s3Requests.Load())
				}
			} else {
				if err != nil || !exists {
					t.Fatalf("signed bucket request failed: exists=%v err=%v", exists, err)
				}
				if got := authorization.Load().(string); !strings.Contains(got, "Credential="+tc.wantKey+"/") {
					t.Errorf("request did not use expected synthetic credential %s", tc.wantKey)
				}
				if s3Requests.Load() != 1 {
					t.Errorf("S3 requests=%d, want 1", s3Requests.Load())
				}
			}
			if got := credentialHTTP.imds.Load(); got != tc.wantIMDS {
				t.Errorf("IMDS requests=%d, want %d", got, tc.wantIMDS)
			}
			if got := credentialHTTP.sts.Load(); got != tc.wantSTS {
				t.Errorf("STS requests=%d, want %d", got, tc.wantSTS)
			}
			if got := credentialHTTP.pod.Load(); got != tc.wantPod {
				t.Errorf("Pod Identity requests=%d, want %d", got, tc.wantPod)
			}
			if got := credentialHTTP.ecs.Load(); got != tc.wantECS {
				t.Errorf("ECS requests=%d, want %d", got, tc.wantECS)
			}
			if got := credentialHTTP.unexpected.Load(); got != 0 {
				t.Errorf("unexpected credential requests=%d", got)
			}
		})
	}
}

func newPolicyFactoryClient(endpoint string, useIAM bool) (*miniosdk.Client, error) {
	return NewMinioClient(context.Background(), &Config{
		Address: strings.TrimPrefix(endpoint, "http://"), BucketName: "credential-policy", CloudProvider: CloudProviderAWS,
		Region: "us-east-1", UseIAM: useIAM, SkipBucketCheck: true,
		AccessKeyID: "EXPLICIT_AK", SecretAccessKeyID: "SYNTHETIC_EXPLICIT_SECRET",
	})
}

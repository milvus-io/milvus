package objectstorage

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/milvus-io/milvus/pkg/v3/proto/clusteringpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestIAMCredentialSourceValidation(t *testing.T) {
	for _, tc := range []struct {
		source, cloud string
		iam, valid    bool
	}{
		{"", "aws", false, true}, {"default", "gcp", true, true},
		{"web_identity", "aws", true, true}, {"instance_profile", "aws", true, true},
		{"web_identity", "gcp", true, false}, {"instance_profile", "aws", false, false},
		{"typo", "aws", true, false},
	} {
		t.Run(fmt.Sprint(tc), func(t *testing.T) {
			c := &Config{IAMCredentialSource: tc.source, CloudProvider: tc.cloud, UseIAM: tc.iam}
			err := validateIAMCredentialSource(c)
			require.Equal(t, tc.valid, err == nil)
			if !tc.valid {
				_, err = NewMinioClient(context.Background(), c)
				require.Error(t, err)
			}
		})
	}
}

func TestNewAWSCredentials(t *testing.T) {
	t.Setenv("AWS_ROLE_ARN", "")
	t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", "")
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_DEFAULT_REGION", "")
	for _, source := range []string{"", "default", "instance_profile"} {
		c, err := NewAWSCredentials(source, "", "")
		require.NoError(t, err)
		require.NotNil(t, c)
	}
	_, err := NewAWSCredentials("typo", "", "")
	require.Error(t, err)
	_, err = NewAWSCredentials("web_identity", "", "")
	require.Error(t, err)
	t.Setenv("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/storage")
	t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", "missing-token-file")
	_, err = NewAWSCredentials("web_identity", "", "")
	require.Error(t, err)
	for _, env := range []string{"AWS_REGION", "AWS_DEFAULT_REGION"} {
		t.Setenv(env, "us-east-1")
		c, err := NewAWSCredentials("web_identity", "", "")
		require.NoError(t, err)
		require.NotNil(t, c)
		t.Setenv(env, "")
	}
	c, err := NewAWSCredentials("web_identity", "us-east-1", "")
	require.NoError(t, err)
	// A missing token is retriable, not a reason to use environment/node credentials.
	t.Setenv("AWS_ACCESS_KEY_ID", "WRONG_NODE_KEY")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "wrong-node-secret")
	_, err = c.Get()
	require.Error(t, err)
}

func TestWebIdentityRecoversWithoutIdentityFallback(t *testing.T) {
	token := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(token, []byte("first-token"), 0600))
	recovered := false
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		require.NoError(t, r.ParseForm())
		require.Equal(t, "AssumeRoleWithWebIdentity", r.Form.Get("Action"))
		require.Equal(t, "arn:aws:iam::123456789012:role/storage", r.Form.Get("RoleArn"))
		if !recovered {
			w.WriteHeader(http.StatusForbidden)
			fmt.Fprint(w, `<ErrorResponse><Error><Code>AccessDenied</Code><Message>temporary STS failure</Message></Error></ErrorResponse>`)
			return
		}
		require.Equal(t, "rotated-token", r.Form.Get("WebIdentityToken"))
		fmt.Fprintf(w, `<AssumeRoleWithWebIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><AssumeRoleWithWebIdentityResult><Credentials><AccessKeyId>STORAGE_KEY</AccessKeyId><SecretAccessKey>storage-secret</SecretAccessKey><SessionToken>storage-session</SessionToken><Expiration>%s</Expiration></Credentials></AssumeRoleWithWebIdentityResult></AssumeRoleWithWebIdentityResponse>`, time.Now().Add(time.Hour).UTC().Format(time.RFC3339))
	}))
	defer server.Close()
	sdk := sts.New(sts.Options{Region: "us-east-1", BaseEndpoint: aws.String(server.URL), HTTPClient: server.Client(), RetryMaxAttempts: 1})
	p := &awsCredentialProvider{provider: stscreds.NewWebIdentityRoleProvider(sdk,
		"arn:aws:iam::123456789012:role/storage", stscreds.IdentityTokenFile(token))}
	c := credentials.New(p)
	t.Setenv("AWS_ACCESS_KEY_ID", "WRONG_NODE_KEY")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "wrong-node-secret")
	_, err := c.Get()
	require.Error(t, err)
	require.True(t, p.IsExpired())
	recovered = true
	require.NoError(t, os.WriteFile(token, []byte("rotated-token"), 0600))
	v, err := c.Get()
	require.NoError(t, err)
	require.Equal(t, "STORAGE_KEY", v.AccessKeyID)
	require.False(t, p.IsExpired())
	_, err = c.Get()
	require.NoError(t, err)
	require.Equal(t, 2, calls)
	c.Expire()
	_, err = c.Get()
	require.NoError(t, err)
	require.Equal(t, 3, calls)
}

type fakeAWSCredentials struct{ value aws.Credentials }

func (p fakeAWSCredentials) Retrieve(context.Context) (aws.Credentials, error) { return p.value, nil }
func TestAWSIAMEmptyCredentialsFailClosed(t *testing.T) {
	p := &awsCredentialProvider{provider: fakeAWSCredentials{}}
	_, err := p.Retrieve()
	require.Error(t, err)
	require.True(t, p.IsExpired())
	p.provider = fakeAWSCredentials{value: aws.Credentials{AccessKeyID: "key", SecretAccessKey: "secret"}}
	v, err := p.Retrieve()
	require.NoError(t, err)
	require.Equal(t, "key", v.AccessKeyID)
	require.False(t, strings.Contains(fmt.Sprint(err), "secret"))
}

func TestInstanceProfileIgnoresWebIdentity(t *testing.T) {
	t.Setenv("AWS_EC2_METADATA_DISABLED", "false")
	t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", "/missing-pod-token")
	t.Setenv("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/pod")
	t.Setenv("AWS_ACCESS_KEY_ID", "WRONG_ENVIRONMENT_KEY")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "wrong-secret")
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		switch r.URL.Path {
		case "/latest/api/token":
			fmt.Fprint(w, "metadata-token")
		case "/latest/meta-data/iam/security-credentials/":
			fmt.Fprint(w, "node-role")
		case "/latest/meta-data/iam/security-credentials/node-role":
			fmt.Fprintf(w, `{"Code":"Success","AccessKeyId":"NODE_KEY","SecretAccessKey":"node-secret","Token":"node-token","Expiration":"%s"}`, time.Now().Add(time.Hour).UTC().Format(time.RFC3339))
		default:
			t.Errorf("unexpected metadata path: %s", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	t.Setenv("AWS_EC2_METADATA_SERVICE_ENDPOINT", server.URL)
	// The explicit Pod mode must fail without ever consulting the available node role.
	pod, err := NewAWSCredentials("web_identity", "us-east-1", "")
	require.NoError(t, err)
	_, err = pod.Get()
	require.Error(t, err)
	require.Zero(t, calls)
	node, err := NewAWSCredentials("instance_profile", "us-east-1", "")
	require.NoError(t, err)
	value, err := node.Get()
	require.NoError(t, err)
	require.Equal(t, "NODE_KEY", value.AccessKeyID)
	require.Greater(t, calls, 0)
}

func TestIAMCredentialSourceWireCompatibility(t *testing.T) {
	for _, source := range []string{"", "default", "web_identity", "instance_profile"} {
		cfg := &Config{}
		IAMCredentialSource(source)(cfg)
		require.Equal(t, source, cfg.IAMCredentialSource)
		wire, err := proto.Marshal(&indexpb.StorageConfig{UseIAM: true, IAMCredentialSource: source})
		require.NoError(t, err)
		index := &indexcgopb.StorageConfig{}
		cluster := &clusteringpb.StorageConfig{}
		require.NoError(t, proto.Unmarshal(wire, index))
		require.NoError(t, proto.Unmarshal(wire, cluster))
		require.Equal(t, source, index.GetIAMCredentialSource())
		require.Equal(t, source, cluster.GetIAMCredentialSource())
		require.True(t, index.GetUseIAM())
		require.True(t, cluster.GetUseIAM())
	}
}

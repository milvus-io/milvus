package tencent

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	minioCred "github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common"
	tchttp "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/http"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/profile"

	"github.com/milvus-io/milvus/pkg/v3/objectstorage/credentialcache"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// NewMinioClient returns a MinIO client compatible with Tencent COS.
func NewMinioClient(address string, opts *minio.Options) (*minio.Client, error) {
	if opts == nil {
		opts = &minio.Options{}
	}
	if opts.Creds == nil {
		provider, err := NewCredentialProvider()
		if err != nil {
			return nil, merr.Wrap(err, "create Tencent credential provider")
		}
		opts.Creds = minioCred.New(provider)
	}
	if address == "" {
		address = fmt.Sprintf("cos.%s.myqcloud.com", opts.Region)
		opts.Secure = true
	}
	return minio.New(address, opts)
}

// CredentialProvider keeps the STS response's actual expiration. The SDK's
// RoleArnCredential hides expiration and returns old values after refresh
// errors, so using its field getters cannot enforce the cache lifetime.
type CredentialProvider struct{ *credentialcache.Provider }

type oidcSource struct {
	roleARN, providerID, tokenFile string
	send                           func(*tchttp.CommonRequest) ([]byte, error)
}

func NewCredentialProvider() (minioCred.Provider, error) {
	region, providerID := os.Getenv("TKE_REGION"), os.Getenv("TKE_PROVIDER_ID")
	roleARN, tokenFile := os.Getenv("TKE_ROLE_ARN"), os.Getenv("TKE_WEB_IDENTITY_TOKEN_FILE")
	if strings.TrimSpace(region) == "" || strings.TrimSpace(providerID) == "" || strings.TrimSpace(roleARN) == "" || strings.TrimSpace(tokenFile) == "" {
		return nil, merr.WrapErrServiceUnavailableMsg("Tencent OIDC credentials require TKE_REGION, TKE_PROVIDER_ID, TKE_ROLE_ARN and TKE_WEB_IDENTITY_TOKEN_FILE")
	}
	clientProfile := profile.NewClientProfile()
	clientProfile.HttpProfile.Endpoint = "sts.tencentcloudapi.com"
	clientProfile.HttpProfile.ReqMethod = "POST"
	client := common.NewCommonClient(nil, region, clientProfile)
	source := &oidcSource{
		roleARN: roleARN, providerID: providerID, tokenFile: tokenFile,
		send: func(request *tchttp.CommonRequest) ([]byte, error) {
			response := tchttp.NewCommonResponse()
			if err := client.Send(request, response); err != nil {
				return nil, err
			}
			return response.GetBody(), nil
		},
	}
	provider := &CredentialProvider{credentialcache.New(source.retrieve, 3*time.Minute, true, false)}
	if _, err := provider.Retrieve(); err != nil {
		return nil, err
	}
	return provider, nil
}

func (s *oidcSource) retrieve() (minioCred.Value, error) {
	// Fix the role/provider identity for this instance; re-read the projected
	// token file on refresh so workload-identity token rotation still works.
	token, err := os.ReadFile(s.tokenFile)
	if err != nil {
		return minioCred.Value{}, merr.Wrap(err, "read Tencent OIDC token file")
	}
	if strings.TrimSpace(string(token)) == "" {
		return minioCred.Value{}, merr.WrapErrServiceUnavailableMsg("Tencent OIDC token file is empty")
	}
	request := tchttp.NewCommonRequest("sts", "2018-08-13", "AssumeRoleWithWebIdentity")
	request.SetSkipSign(true)
	if err := request.SetActionParameters(map[string]interface{}{
		"ProviderId": s.providerID, "WebIdentityToken": string(token), "RoleArn": s.roleARN,
		"RoleSessionName": fmt.Sprintf("tencentcloud-go-sdk-%d", time.Now().UnixMicro()), "DurationSeconds": int64(7200),
	}); err != nil {
		return minioCred.Value{}, merr.Wrap(err, "create Tencent OIDC request")
	}
	body, err := s.send(request)
	if err != nil {
		return minioCred.Value{}, merr.Wrap(err, "retrieve Tencent OIDC credentials")
	}
	var response struct {
		Response struct {
			Credentials struct {
				TmpSecretID  string `json:"TmpSecretId"`
				TmpSecretKey string `json:"TmpSecretKey"`
				Token        string `json:"Token"`
			} `json:"Credentials"`
			ExpiredTime int64 `json:"ExpiredTime"`
		} `json:"Response"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return minioCred.Value{}, merr.Wrap(err, "decode Tencent OIDC credentials")
	}
	return minioCred.Value{
		AccessKeyID: response.Response.Credentials.TmpSecretID, SecretAccessKey: response.Response.Credentials.TmpSecretKey,
		SessionToken: response.Response.Credentials.Token, Expiration: time.Unix(response.Response.ExpiredTime, 0),
		SignerType: minioCred.SignatureV4,
	}, nil
}

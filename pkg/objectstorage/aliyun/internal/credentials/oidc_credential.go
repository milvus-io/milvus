// Derived from github.com/aliyun/credentials-go v1.2.7 (Apache-2.0).
// Local maintenance and modifications are documented in UPSTREAM.md.

package credentials

import (
	"encoding/json"
	"os"
	"strings"

	"github.com/alibabacloud-go/tea/tea"

	"github.com/milvus-io/milvus/pkg/v3/objectstorage/aliyun/internal/credentials/request"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage/aliyun/internal/credentials/utils"
)

const defaultOIDCDurationSeconds = 3600

// OIDCCredential is a kind of credentials
type OIDCCredential struct {
	*credentialUpdater
	AccessKeyId           string
	AccessKeySecret       string
	RoleArn               string
	OIDCProviderArn       string
	OIDCTokenFilePath     string
	Policy                string
	RoleSessionName       string
	RoleSessionExpiration int
	sessionCredential     *sessionCredential
	runtime               *utils.Runtime
}

type OIDCResponse struct {
	Credentials *credentialsInResponse `json:"Credentials" xml:"Credentials"`
}

type OIDCcredentialsInResponse struct {
	AccessKeyId     string `json:"AccessKeyId" xml:"AccessKeyId"`
	AccessKeySecret string `json:"AccessKeySecret" xml:"AccessKeySecret"`
	SecurityToken   string `json:"SecurityToken" xml:"SecurityToken"`
	Expiration      string `json:"Expiration" xml:"Expiration"`
}

func newOIDCRoleArnCredential(accessKeyId, accessKeySecret, roleArn, OIDCProviderArn, OIDCTokenFilePath, RoleSessionName, policy string, RoleSessionExpiration int, runtime *utils.Runtime) *OIDCCredential {
	if OIDCTokenFilePath == "" {
		OIDCTokenFilePath = os.Getenv("ALIBABA_CLOUD_OIDC_TOKEN_FILE")
	}
	return &OIDCCredential{
		AccessKeyId:           accessKeyId,
		AccessKeySecret:       accessKeySecret,
		RoleArn:               roleArn,
		OIDCProviderArn:       OIDCProviderArn,
		OIDCTokenFilePath:     OIDCTokenFilePath,
		RoleSessionName:       RoleSessionName,
		Policy:                policy,
		RoleSessionExpiration: RoleSessionExpiration,
		credentialUpdater:     new(credentialUpdater),
		runtime:               runtime,
	}
}

// GetAccessKeyId reutrns OIDCCredential's AccessKeyId
// if AccessKeyId is not exist or out of date, the function will update it.
func (r *OIDCCredential) GetAccessKeyId() (*string, error) {
	snapshot, err := RetrieveSnapshot(r)
	if err != nil {
		return tea.String(""), err
	}
	return tea.String(snapshot.AccessKeyId), nil
}

// GetAccessSecret reutrns OIDCCredential's AccessKeySecret
// if AccessKeySecret is not exist or out of date, the function will update it.
func (r *OIDCCredential) GetAccessKeySecret() (*string, error) {
	snapshot, err := RetrieveSnapshot(r)
	if err != nil {
		return tea.String(""), err
	}
	return tea.String(snapshot.AccessKeySecret), nil
}

// GetSecurityToken reutrns OIDCCredential's SecurityToken
// if SecurityToken is not exist or out of date, the function will update it.
func (r *OIDCCredential) GetSecurityToken() (*string, error) {
	snapshot, err := RetrieveSnapshot(r)
	if err != nil {
		return tea.String(""), err
	}
	return tea.String(snapshot.SecurityToken), nil
}

// GetBearerToken is useless OIDCCredential
func (r *OIDCCredential) GetBearerToken() *string {
	return tea.String("")
}

// GetType reutrns OIDCCredential's type
func (r *OIDCCredential) GetType() *string {
	return tea.String("oidc_role_arn")
}

func (r *OIDCCredential) GetOIDCToken(tokenPath string) *string {
	// The selected token path is part of this provider's identity. A refresh
	// failure must not fall back to a different path from changed environment.
	token, err := os.ReadFile(tokenPath)
	if err != nil {
		return nil
	}
	return tea.String(string(token))
}

func (r *OIDCCredential) updateCredential() (err error) {
	if r.runtime == nil {
		r.runtime = new(utils.Runtime)
	}
	request := request.NewCommonRequest()
	request.Domain = "sts.aliyuncs.com"
	if r.runtime.STSEndpoint != "" {
		request.Domain = r.runtime.STSEndpoint
	}
	request.Scheme = "HTTPS"
	request.Method = "POST"
	request.QueryParams["Timestamp"] = utils.GetTimeInFormatISO8601()
	request.QueryParams["Action"] = "AssumeRoleWithOIDC"
	request.QueryParams["Format"] = "JSON"
	request.BodyParams["RoleArn"] = r.RoleArn
	request.BodyParams["OIDCProviderArn"] = r.OIDCProviderArn
	token := r.GetOIDCToken(r.OIDCTokenFilePath)
	if token == nil || strings.TrimSpace(*token) == "" {
		return newCredentialError("cannot read a nonempty OIDC token from the selected file")
	}
	request.BodyParams["OIDCToken"] = tea.StringValue(token)
	if r.Policy != "" {
		request.QueryParams["Policy"] = r.Policy
	}
	request.QueryParams["RoleSessionName"] = r.RoleSessionName
	request.QueryParams["Version"] = "2015-04-01"
	request.QueryParams["SignatureNonce"] = utils.GetUUID()
	request.Headers["Host"] = request.Domain
	request.Headers["Accept-Encoding"] = "identity"
	request.Headers["content-type"] = "application/x-www-form-urlencoded"
	request.URL = request.BuildURL()
	content, err := doAction(request, r.runtime)
	if err != nil {
		return wrapCredentialError(err, "credential refresh failed")
	}
	var resp *OIDCResponse
	err = json.Unmarshal(content, &resp)
	if err != nil {
		return newCredentialError("credential endpoint returned invalid JSON")
	}
	if resp == nil || resp.Credentials == nil {
		return credentialErrorf("refresh RoleArn sts token err: Credentials is empty")
	}
	respCredentials := resp.Credentials
	if respCredentials.AccessKeyId == "" || respCredentials.AccessKeySecret == "" || respCredentials.SecurityToken == "" || respCredentials.Expiration == "" {
		return credentialErrorf("credential endpoint returned incomplete credentials")
	}

	candidate := &sessionCredential{
		AccessKeyId:     respCredentials.AccessKeyId,
		AccessKeySecret: respCredentials.AccessKeySecret,
		SecurityToken:   respCredentials.SecurityToken,
	}
	expirationTime, err := validateSession(candidate, respCredentials.Expiration, true, r.timeNow())
	if err != nil {
		return err
	}
	r.lastUpdateTimestamp = r.timeNow().Unix()
	r.credentialExpiration = int(expirationTime.Unix() - r.lastUpdateTimestamp)
	r.expiresAt = expirationTime
	r.sessionCredential = candidate

	return
}

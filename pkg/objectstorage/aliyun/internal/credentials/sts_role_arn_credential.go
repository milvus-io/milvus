// Derived from github.com/aliyun/credentials-go v1.2.7 (Apache-2.0).
// Local maintenance and modifications are documented in UPSTREAM.md.

package credentials

import (
	"encoding/json"
	"strconv"

	"github.com/alibabacloud-go/tea/tea"

	"github.com/milvus-io/milvus/pkg/v3/objectstorage/aliyun/internal/credentials/request"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage/aliyun/internal/credentials/utils"
)

const defaultDurationSeconds = 3600

// RAMRoleArnCredential is a kind of credentials
type RAMRoleArnCredential struct {
	*credentialUpdater
	AccessKeyId           string
	AccessKeySecret       string
	RoleArn               string
	RoleSessionName       string
	RoleSessionExpiration int
	Policy                string
	sessionCredential     *sessionCredential
	runtime               *utils.Runtime
}

type ramRoleArnResponse struct {
	Credentials *credentialsInResponse `json:"Credentials" xml:"Credentials"`
}

type credentialsInResponse struct {
	AccessKeyId     string `json:"AccessKeyId" xml:"AccessKeyId"`
	AccessKeySecret string `json:"AccessKeySecret" xml:"AccessKeySecret"`
	SecurityToken   string `json:"SecurityToken" xml:"SecurityToken"`
	Expiration      string `json:"Expiration" xml:"Expiration"`
}

func newRAMRoleArnCredential(accessKeyId, accessKeySecret, roleArn, roleSessionName, policy string, roleSessionExpiration int, runtime *utils.Runtime) *RAMRoleArnCredential {
	return &RAMRoleArnCredential{
		AccessKeyId:           accessKeyId,
		AccessKeySecret:       accessKeySecret,
		RoleArn:               roleArn,
		RoleSessionName:       roleSessionName,
		RoleSessionExpiration: roleSessionExpiration,
		Policy:                policy,
		credentialUpdater:     new(credentialUpdater),
		runtime:               runtime,
	}
}

// GetAccessKeyId reutrns RamRoleArnCredential's AccessKeyId
// if AccessKeyId is not exist or out of date, the function will update it.
func (r *RAMRoleArnCredential) GetAccessKeyId() (*string, error) {
	snapshot, err := RetrieveSnapshot(r)
	if err != nil {
		return tea.String(""), err
	}
	return tea.String(snapshot.AccessKeyId), nil
}

// GetAccessSecret reutrns RamRoleArnCredential's AccessKeySecret
// if AccessKeySecret is not exist or out of date, the function will update it.
func (r *RAMRoleArnCredential) GetAccessKeySecret() (*string, error) {
	snapshot, err := RetrieveSnapshot(r)
	if err != nil {
		return tea.String(""), err
	}
	return tea.String(snapshot.AccessKeySecret), nil
}

// GetSecurityToken reutrns RamRoleArnCredential's SecurityToken
// if SecurityToken is not exist or out of date, the function will update it.
func (r *RAMRoleArnCredential) GetSecurityToken() (*string, error) {
	snapshot, err := RetrieveSnapshot(r)
	if err != nil {
		return tea.String(""), err
	}
	return tea.String(snapshot.SecurityToken), nil
}

// GetBearerToken is useless RamRoleArnCredential
func (r *RAMRoleArnCredential) GetBearerToken() *string {
	return tea.String("")
}

// GetType reutrns RamRoleArnCredential's type
func (r *RAMRoleArnCredential) GetType() *string {
	return tea.String("ram_role_arn")
}

func (r *RAMRoleArnCredential) updateCredential() (err error) {
	if r.runtime == nil {
		r.runtime = new(utils.Runtime)
	}
	request := request.NewCommonRequest()
	request.Domain = "sts.aliyuncs.com"
	if r.runtime.STSEndpoint != "" {
		request.Domain = r.runtime.STSEndpoint
	}
	request.Scheme = "HTTPS"
	request.Method = "GET"
	request.QueryParams["AccessKeyId"] = r.AccessKeyId
	request.QueryParams["Action"] = "AssumeRole"
	request.QueryParams["Format"] = "JSON"
	if r.RoleSessionExpiration > 0 {
		if r.RoleSessionExpiration >= 900 && r.RoleSessionExpiration <= 3600 {
			request.QueryParams["DurationSeconds"] = strconv.Itoa(r.RoleSessionExpiration)
		} else {
			err = newCredentialError("[InvalidParam]:Assume Role session duration should be in the range of 15min - 1Hr")
			return
		}
	} else {
		request.QueryParams["DurationSeconds"] = strconv.Itoa(defaultDurationSeconds)
	}
	request.QueryParams["RoleArn"] = r.RoleArn
	if r.Policy != "" {
		request.QueryParams["Policy"] = r.Policy
	}
	request.QueryParams["RoleSessionName"] = r.RoleSessionName
	request.QueryParams["SignatureMethod"] = "HMAC-SHA1"
	request.QueryParams["SignatureVersion"] = "1.0"
	request.QueryParams["Version"] = "2015-04-01"
	request.QueryParams["Timestamp"] = utils.GetTimeInFormatISO8601()
	request.QueryParams["SignatureNonce"] = utils.GetUUID()
	signature := utils.ShaHmac1(request.BuildStringToSign(), r.AccessKeySecret+"&")
	request.QueryParams["Signature"] = signature
	request.Headers["Host"] = request.Domain
	request.Headers["Accept-Encoding"] = "identity"
	request.URL = request.BuildURL()
	content, err := doAction(request, r.runtime)
	if err != nil {
		return wrapCredentialError(err, "credential refresh failed")
	}
	var resp *ramRoleArnResponse
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

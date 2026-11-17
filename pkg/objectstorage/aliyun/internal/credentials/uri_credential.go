// Derived from github.com/aliyun/credentials-go v1.2.7 (Apache-2.0).
// Local maintenance and modifications are documented in UPSTREAM.md.

package credentials

import (
	"encoding/json"
	"os"

	"github.com/alibabacloud-go/tea/tea"

	"github.com/milvus-io/milvus/pkg/v3/objectstorage/aliyun/internal/credentials/request"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage/aliyun/internal/credentials/utils"
)

// URLCredential is a kind of credential
type URLCredential struct {
	URL string
	*credentialUpdater
	*sessionCredential
	runtime *utils.Runtime
}

type URLResponse struct {
	AccessKeyId     string `json:"AccessKeyId" xml:"AccessKeyId"`
	AccessKeySecret string `json:"AccessKeySecret" xml:"AccessKeySecret"`
	SecurityToken   string `json:"SecurityToken" xml:"SecurityToken"`
	Expiration      string `json:"Expiration" xml:"Expiration"`
}

func newURLCredential(URL string) *URLCredential {
	credentialUpdater := new(credentialUpdater)
	if URL == "" {
		URL = os.Getenv("ALIBABA_CLOUD_CREDENTIALS_URI")
	}
	return &URLCredential{
		URL:               URL,
		credentialUpdater: credentialUpdater,
	}
}

// GetAccessKeyId reutrns  URLCredential's AccessKeyId
// if AccessKeyId is not exist or out of date, the function will update it.
func (e *URLCredential) GetAccessKeyId() (*string, error) {
	snapshot, err := RetrieveSnapshot(e)
	if err != nil {
		return tea.String(""), err
	}
	return tea.String(snapshot.AccessKeyId), nil
}

// GetAccessSecret reutrns  URLCredential's AccessKeySecret
// if AccessKeySecret is not exist or out of date, the function will update it.
func (e *URLCredential) GetAccessKeySecret() (*string, error) {
	snapshot, err := RetrieveSnapshot(e)
	if err != nil {
		return tea.String(""), err
	}
	return tea.String(snapshot.AccessKeySecret), nil
}

// GetSecurityToken reutrns  URLCredential's SecurityToken
// if SecurityToken is not exist or out of date, the function will update it.
func (e *URLCredential) GetSecurityToken() (*string, error) {
	snapshot, err := RetrieveSnapshot(e)
	if err != nil {
		return tea.String(""), err
	}
	return tea.String(snapshot.SecurityToken), nil
}

// GetBearerToken is useless for URLCredential
func (e *URLCredential) GetBearerToken() *string {
	return tea.String("")
}

// GetType reutrns  URLCredential's type
func (e *URLCredential) GetType() *string {
	return tea.String("credential_uri")
}

func (e *URLCredential) updateCredential() (err error) {
	if e.runtime == nil {
		e.runtime = new(utils.Runtime)
	}
	request := request.NewCommonRequest()
	request.URL = e.URL
	request.Method = "GET"
	content, err := doAction(request, e.runtime)
	if err != nil {
		return wrapCredentialError(err, "credential refresh failed")
	}
	var resp *URLResponse
	err = json.Unmarshal(content, &resp)
	if err != nil {
		return newCredentialError("credential endpoint returned invalid JSON")
	}
	if resp == nil {
		return credentialErrorf("credential endpoint returned an empty response")
	}
	if resp.AccessKeyId == "" || resp.AccessKeySecret == "" || resp.SecurityToken == "" || resp.Expiration == "" {
		return credentialErrorf("credential endpoint returned incomplete credentials")
	}

	candidate := &sessionCredential{
		AccessKeyId:     resp.AccessKeyId,
		AccessKeySecret: resp.AccessKeySecret,
		SecurityToken:   resp.SecurityToken,
	}
	expirationTime, err := validateSession(candidate, resp.Expiration, true, e.timeNow())
	if err != nil {
		return err
	}
	e.lastUpdateTimestamp = e.timeNow().Unix()
	e.credentialExpiration = int(expirationTime.Unix() - e.lastUpdateTimestamp)
	e.expiresAt = expirationTime
	e.sessionCredential = candidate

	return
}

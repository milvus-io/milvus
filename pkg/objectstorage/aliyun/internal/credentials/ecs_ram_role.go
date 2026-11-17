// Derived from github.com/aliyun/credentials-go v1.2.7 (Apache-2.0).
// Local maintenance and modifications are documented in UPSTREAM.md.

package credentials

import (
	"encoding/json"

	"github.com/alibabacloud-go/tea/tea"

	"github.com/milvus-io/milvus/pkg/v3/objectstorage/aliyun/internal/credentials/request"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage/aliyun/internal/credentials/utils"
)

var securityCredURL = "http://100.100.100.200/latest/meta-data/ram/security-credentials/"

// EcsRAMRoleCredential is a kind of credential
type EcsRAMRoleCredential struct {
	*credentialUpdater
	RoleName          string
	sessionCredential *sessionCredential
	runtime           *utils.Runtime
}

type ecsRAMRoleResponse struct {
	Code            string `json:"Code" xml:"Code"`
	AccessKeyId     string `json:"AccessKeyId" xml:"AccessKeyId"`
	AccessKeySecret string `json:"AccessKeySecret" xml:"AccessKeySecret"`
	SecurityToken   string `json:"SecurityToken" xml:"SecurityToken"`
	Expiration      string `json:"Expiration" xml:"Expiration"`
}

func newEcsRAMRoleCredential(roleName string, inAdvanceScale float64, runtime *utils.Runtime) *EcsRAMRoleCredential {
	credentialUpdater := new(credentialUpdater)
	if inAdvanceScale < 1 && inAdvanceScale > 0 {
		credentialUpdater.inAdvanceScale = inAdvanceScale
	}
	return &EcsRAMRoleCredential{
		RoleName:          roleName,
		credentialUpdater: credentialUpdater,
		runtime:           runtime,
	}
}

// GetAccessKeyId reutrns  EcsRAMRoleCredential's AccessKeyId
// if AccessKeyId is not exist or out of date, the function will update it.
func (e *EcsRAMRoleCredential) GetAccessKeyId() (*string, error) {
	snapshot, err := RetrieveSnapshot(e)
	if err != nil {
		return tea.String(""), err
	}
	return tea.String(snapshot.AccessKeyId), nil
}

// GetAccessSecret reutrns  EcsRAMRoleCredential's AccessKeySecret
// if AccessKeySecret is not exist or out of date, the function will update it.
func (e *EcsRAMRoleCredential) GetAccessKeySecret() (*string, error) {
	snapshot, err := RetrieveSnapshot(e)
	if err != nil {
		return tea.String(""), err
	}
	return tea.String(snapshot.AccessKeySecret), nil
}

// GetSecurityToken reutrns  EcsRAMRoleCredential's SecurityToken
// if SecurityToken is not exist or out of date, the function will update it.
func (e *EcsRAMRoleCredential) GetSecurityToken() (*string, error) {
	snapshot, err := RetrieveSnapshot(e)
	if err != nil {
		return tea.String(""), err
	}
	return tea.String(snapshot.SecurityToken), nil
}

// GetBearerToken is useless for EcsRAMRoleCredential
func (e *EcsRAMRoleCredential) GetBearerToken() *string {
	return tea.String("")
}

// GetType reutrns  EcsRAMRoleCredential's type
func (e *EcsRAMRoleCredential) GetType() *string {
	return tea.String("ecs_ram_role")
}

func getRoleName() (string, error) {
	runtime := utils.NewRuntime(1, 1, "", "")
	request := request.NewCommonRequest()
	request.URL = securityCredURL
	request.Method = "GET"
	content, err := doAction(request, runtime)
	if err != nil {
		return "", err
	}
	return string(content), nil
}

func (e *EcsRAMRoleCredential) updateCredential() (err error) {
	if e.runtime == nil {
		e.runtime = new(utils.Runtime)
	}
	request := request.NewCommonRequest()
	if e.RoleName == "" {
		e.RoleName, err = getRoleName()
		if err != nil {
			return wrapCredentialError(err, "credential refresh failed")
		}
	}
	request.URL = securityCredURL + e.RoleName
	request.Method = "GET"
	content, err := doAction(request, e.runtime)
	if err != nil {
		return wrapCredentialError(err, "credential refresh failed")
	}
	var resp *ecsRAMRoleResponse
	err = json.Unmarshal(content, &resp)
	if err != nil {
		return newCredentialError("credential endpoint returned invalid JSON")
	}
	if resp == nil {
		return credentialErrorf("credential endpoint returned an empty response")
	}
	if resp.Code != "Success" {
		return credentialErrorf("refresh Ecs sts token err: Code is not Success")
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

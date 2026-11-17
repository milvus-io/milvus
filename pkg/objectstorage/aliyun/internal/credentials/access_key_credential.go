// Derived from github.com/aliyun/credentials-go v1.2.7 (Apache-2.0).
// Local maintenance and modifications are documented in UPSTREAM.md.

package credentials

import "github.com/alibabacloud-go/tea/tea"

// AccessKeyCredential is a kind of credential
type AccessKeyCredential struct {
	AccessKeyId     string
	AccessKeySecret string
}

func newAccessKeyCredential(accessKeyId, accessKeySecret string) *AccessKeyCredential {
	return &AccessKeyCredential{
		AccessKeyId:     accessKeyId,
		AccessKeySecret: accessKeySecret,
	}
}

// GetAccessKeyId reutrns  AccessKeyCreential's AccessKeyId
func (a *AccessKeyCredential) GetAccessKeyId() (*string, error) {
	return tea.String(a.AccessKeyId), nil
}

// GetAccessSecret reutrns  AccessKeyCreential's AccessKeySecret
func (a *AccessKeyCredential) GetAccessKeySecret() (*string, error) {
	return tea.String(a.AccessKeySecret), nil
}

// GetSecurityToken is useless for AccessKeyCreential
func (a *AccessKeyCredential) GetSecurityToken() (*string, error) {
	return tea.String(""), nil
}

// GetBearerToken is useless for AccessKeyCreential
func (a *AccessKeyCredential) GetBearerToken() *string {
	return tea.String("")
}

// GetType reutrns  AccessKeyCreential's type
func (a *AccessKeyCredential) GetType() *string {
	return tea.String("access_key")
}

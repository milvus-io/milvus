// Derived from github.com/aliyun/credentials-go v1.2.7 (Apache-2.0).
// Local maintenance and modifications are documented in UPSTREAM.md.

package credentials

import "github.com/alibabacloud-go/tea/tea"

// StsTokenCredential is a kind of credentials
type StsTokenCredential struct {
	AccessKeyId     string
	AccessKeySecret string
	SecurityToken   string
}

func newStsTokenCredential(accessKeyId, accessKeySecret, securityToken string) *StsTokenCredential {
	return &StsTokenCredential{
		AccessKeyId:     accessKeyId,
		AccessKeySecret: accessKeySecret,
		SecurityToken:   securityToken,
	}
}

// GetAccessKeyId reutrns  StsTokenCredential's AccessKeyId
func (s *StsTokenCredential) GetAccessKeyId() (*string, error) {
	return tea.String(s.AccessKeyId), nil
}

// GetAccessSecret reutrns  StsTokenCredential's AccessKeySecret
func (s *StsTokenCredential) GetAccessKeySecret() (*string, error) {
	return tea.String(s.AccessKeySecret), nil
}

// GetSecurityToken reutrns  StsTokenCredential's SecurityToken
func (s *StsTokenCredential) GetSecurityToken() (*string, error) {
	return tea.String(s.SecurityToken), nil
}

// GetBearerToken is useless StsTokenCredential
func (s *StsTokenCredential) GetBearerToken() *string {
	return tea.String("")
}

// GetType reutrns  StsTokenCredential's type
func (s *StsTokenCredential) GetType() *string {
	return tea.String("sts")
}

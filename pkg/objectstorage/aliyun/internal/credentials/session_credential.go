// Derived from github.com/aliyun/credentials-go v1.2.7 (Apache-2.0).
// Local maintenance and modifications are documented in UPSTREAM.md.

package credentials

type sessionCredential struct {
	AccessKeyId     string
	AccessKeySecret string
	SecurityToken   string
}

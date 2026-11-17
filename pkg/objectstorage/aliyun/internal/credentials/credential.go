// Derived from github.com/aliyun/credentials-go v1.2.7 (Apache-2.0).
// Local maintenance and modifications are documented in UPSTREAM.md.

package credentials

import (
	"bufio"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/alibabacloud-go/tea/tea"

	"github.com/milvus-io/milvus/pkg/v3/objectstorage/aliyun/internal/credentials/request"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage/aliyun/internal/credentials/response"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage/aliyun/internal/credentials/utils"
)

var hookParse = func(err error) error {
	return err
}

// Credential is an interface for getting actual credential
type Credential interface {
	GetAccessKeyId() (*string, error)
	GetAccessKeySecret() (*string, error)
	GetSecurityToken() (*string, error)
	GetBearerToken() *string
	GetType() *string
}

// Config is important when call NewCredential
type Config struct {
	Type                  *string  `json:"type"`
	AccessKeyId           *string  `json:"access_key_id"`
	AccessKeySecret       *string  `json:"access_key_secret"`
	OIDCProviderArn       *string  `json:"oidc_provider_arn"`
	OIDCTokenFilePath     *string  `json:"oidc_token"`
	RoleArn               *string  `json:"role_arn"`
	RoleSessionName       *string  `json:"role_session_name"`
	PublicKeyId           *string  `json:"public_key_id"`
	RoleName              *string  `json:"role_name"`
	SessionExpiration     *int     `json:"session_expiration"`
	PrivateKeyFile        *string  `json:"private_key_file"`
	BearerToken           *string  `json:"bearer_token"`
	SecurityToken         *string  `json:"security_token"`
	RoleSessionExpiration *int     `json:"role_session_expiratioon"`
	Policy                *string  `json:"policy"`
	Host                  *string  `json:"host"`
	Timeout               *int     `json:"timeout"`
	ConnectTimeout        *int     `json:"connect_timeout"`
	Proxy                 *string  `json:"proxy"`
	InAdvanceScale        *float64 `json:"inAdvanceScale"`
	Url                   *string  `json:"url"`
	STSEndpoint           *string  `json:"sts_endpoint"`
}

func (s Config) String() string {
	return tea.Prettify(s)
}

func (s Config) GoString() string {
	return s.String()
}

func (s *Config) SetAccessKeyId(v string) *Config {
	s.AccessKeyId = &v
	return s
}

func (s *Config) SetAccessKeySecret(v string) *Config {
	s.AccessKeySecret = &v
	return s
}

func (s *Config) SetSecurityToken(v string) *Config {
	s.SecurityToken = &v
	return s
}

func (s *Config) SetRoleArn(v string) *Config {
	s.RoleArn = &v
	return s
}

func (s *Config) SetRoleSessionName(v string) *Config {
	s.RoleSessionName = &v
	return s
}

func (s *Config) SetPublicKeyId(v string) *Config {
	s.PublicKeyId = &v
	return s
}

func (s *Config) SetRoleName(v string) *Config {
	s.RoleName = &v
	return s
}

func (s *Config) SetSessionExpiration(v int) *Config {
	s.SessionExpiration = &v
	return s
}

func (s *Config) SetPrivateKeyFile(v string) *Config {
	s.PrivateKeyFile = &v
	return s
}

func (s *Config) SetBearerToken(v string) *Config {
	s.BearerToken = &v
	return s
}

func (s *Config) SetRoleSessionExpiration(v int) *Config {
	s.RoleSessionExpiration = &v
	return s
}

func (s *Config) SetPolicy(v string) *Config {
	s.Policy = &v
	return s
}

func (s *Config) SetHost(v string) *Config {
	s.Host = &v
	return s
}

func (s *Config) SetTimeout(v int) *Config {
	s.Timeout = &v
	return s
}

func (s *Config) SetConnectTimeout(v int) *Config {
	s.ConnectTimeout = &v
	return s
}

func (s *Config) SetProxy(v string) *Config {
	s.Proxy = &v
	return s
}

func (s *Config) SetType(v string) *Config {
	s.Type = &v
	return s
}

func (s *Config) SetOIDCTokenFilePath(v string) *Config {
	s.OIDCTokenFilePath = &v
	return s
}

func (s *Config) SetOIDCProviderArn(v string) *Config {
	s.OIDCProviderArn = &v
	return s
}

func (s *Config) SetURLCredential(v string) *Config {
	if v == "" {
		v = os.Getenv("ALIBABA_CLOUD_CREDENTIALS_URI")
	}
	s.Url = &v
	return s
}

func (s *Config) SetSTSEndpoint(v string) *Config {
	s.STSEndpoint = &v
	return s
}

// NewCredential return a credential according to the type in config.
// if config is nil, the function will use default provider chain to get credential.
// please see README.md for detail.
func NewCredential(config *Config) (credential Credential, err error) {
	if config == nil {
		config, err = defaultChain.resolve()
		if err != nil {
			return
		}
		return NewCredential(config)
	}
	switch tea.StringValue(config.Type) {
	case "credentials_uri":
		credential = newURLCredential(tea.StringValue(config.Url))
	case "oidc_role_arn":
		err = checkoutAssumeRamoidc(config)
		if err != nil {
			return
		}
		runtime := &utils.Runtime{
			Host:           tea.StringValue(config.Host),
			Proxy:          tea.StringValue(config.Proxy),
			ReadTimeout:    tea.IntValue(config.Timeout),
			ConnectTimeout: tea.IntValue(config.ConnectTimeout),
			STSEndpoint:    tea.StringValue(config.STSEndpoint),
		}
		credential = newOIDCRoleArnCredential(tea.StringValue(config.AccessKeyId), tea.StringValue(config.AccessKeySecret), tea.StringValue(config.RoleArn), tea.StringValue(config.OIDCProviderArn), tea.StringValue(config.OIDCTokenFilePath), tea.StringValue(config.RoleSessionName), tea.StringValue(config.Policy), tea.IntValue(config.RoleSessionExpiration), runtime)
	case "access_key":
		err = checkAccessKey(config)
		if err != nil {
			return
		}
		credential = newAccessKeyCredential(tea.StringValue(config.AccessKeyId), tea.StringValue(config.AccessKeySecret))
	case "sts":
		err = checkSTS(config)
		if err != nil {
			return
		}
		credential = newStsTokenCredential(tea.StringValue(config.AccessKeyId), tea.StringValue(config.AccessKeySecret), tea.StringValue(config.SecurityToken))
	case "ecs_ram_role":
		checkEcsRAMRole(config)
		runtime := &utils.Runtime{
			Host:           tea.StringValue(config.Host),
			Proxy:          tea.StringValue(config.Proxy),
			ReadTimeout:    tea.IntValue(config.Timeout),
			ConnectTimeout: tea.IntValue(config.ConnectTimeout),
		}
		credential = newEcsRAMRoleCredential(tea.StringValue(config.RoleName), tea.Float64Value(config.InAdvanceScale), runtime)
	case "ram_role_arn":
		err = checkRAMRoleArn(config)
		if err != nil {
			return
		}
		runtime := &utils.Runtime{
			Host:           tea.StringValue(config.Host),
			Proxy:          tea.StringValue(config.Proxy),
			ReadTimeout:    tea.IntValue(config.Timeout),
			ConnectTimeout: tea.IntValue(config.ConnectTimeout),
			STSEndpoint:    tea.StringValue(config.STSEndpoint),
		}
		credential = newRAMRoleArnCredential(tea.StringValue(config.AccessKeyId), tea.StringValue(config.AccessKeySecret), tea.StringValue(config.RoleArn), tea.StringValue(config.RoleSessionName), tea.StringValue(config.Policy), tea.IntValue(config.RoleSessionExpiration), runtime)
	case "rsa_key_pair":
		err = checkRSAKeyPair(config)
		if err != nil {
			return
		}
		file, err1 := os.Open(tea.StringValue(config.PrivateKeyFile))
		if err1 != nil {
			err = newCredentialError("cannot open configured private key file")
			return
		}
		defer file.Close()
		var privateKey string
		scan := bufio.NewScanner(file)
		for scan.Scan() {
			if strings.HasPrefix(scan.Text(), "----") {
				continue
			}
			privateKey += scan.Text() + "\n"
		}
		runtime := &utils.Runtime{
			Host:           tea.StringValue(config.Host),
			Proxy:          tea.StringValue(config.Proxy),
			ReadTimeout:    tea.IntValue(config.Timeout),
			ConnectTimeout: tea.IntValue(config.ConnectTimeout),
			STSEndpoint:    tea.StringValue(config.STSEndpoint),
		}
		credential = newRsaKeyPairCredential(privateKey, tea.StringValue(config.PublicKeyId), tea.IntValue(config.SessionExpiration), runtime)
	case "bearer":
		if tea.StringValue(config.BearerToken) == "" {
			err = newCredentialError("BearerToken cannot be empty")
			return
		}
		credential = newBearerTokenCredential(tea.StringValue(config.BearerToken))
	default:
		err = newCredentialError("Invalid type option, support: access_key, sts, ecs_ram_role, ram_role_arn, rsa_key_pair")
		return
	}
	return credential, nil
}

func checkRSAKeyPair(config *Config) (err error) {
	if tea.StringValue(config.PrivateKeyFile) == "" {
		err = newCredentialError("PrivateKeyFile cannot be empty")
		return
	}
	if tea.StringValue(config.PublicKeyId) == "" {
		err = newCredentialError("PublicKeyId cannot be empty")
		return
	}
	return
}

func checkoutAssumeRamoidc(config *Config) (err error) {
	if tea.StringValue(config.RoleArn) == "" {
		err = newCredentialError("RoleArn cannot be empty")
		return
	}
	if tea.StringValue(config.OIDCProviderArn) == "" {
		err = newCredentialError("OIDCProviderArn cannot be empty")
		return
	}
	return
}

func checkRAMRoleArn(config *Config) (err error) {
	if tea.StringValue(config.AccessKeySecret) == "" {
		err = newCredentialError("AccessKeySecret cannot be empty")
		return
	}
	if tea.StringValue(config.RoleArn) == "" {
		err = newCredentialError("RoleArn cannot be empty")
		return
	}
	if tea.StringValue(config.RoleSessionName) == "" {
		err = newCredentialError("RoleSessionName cannot be empty")
		return
	}
	if tea.StringValue(config.AccessKeyId) == "" {
		err = newCredentialError("AccessKeyId cannot be empty")
		return
	}
	return
}

func checkEcsRAMRole(config *Config) (err error) {
	return
}

func checkSTS(config *Config) (err error) {
	if tea.StringValue(config.AccessKeyId) == "" {
		err = newCredentialError("AccessKeyId cannot be empty")
		return
	}
	if tea.StringValue(config.AccessKeySecret) == "" {
		err = newCredentialError("AccessKeySecret cannot be empty")
		return
	}
	if tea.StringValue(config.SecurityToken) == "" {
		err = newCredentialError("SecurityToken cannot be empty")
		return
	}
	return
}

func checkAccessKey(config *Config) (err error) {
	if tea.StringValue(config.AccessKeyId) == "" {
		err = newCredentialError("AccessKeyId cannot be empty")
		return
	}
	if tea.StringValue(config.AccessKeySecret) == "" {
		err = newCredentialError("AccessKeySecret cannot be empty")
		return
	}
	return
}

func doAction(request *request.CommonRequest, runtime *utils.Runtime) (content []byte, err error) {
	var urlEncoded string
	if request.BodyParams != nil {
		urlEncoded = utils.GetURLFormedMap(request.BodyParams)
	}
	httpRequest, err := http.NewRequest(request.Method, request.URL, strings.NewReader(urlEncoded))
	if err != nil {
		return
	}
	httpRequest.Proto = "HTTP/1.1"
	httpRequest.Host = request.Domain
	for key, value := range request.Headers {
		if value != "" {
			httpRequest.Header[key] = []string{value}
		}
	}
	httpClient := &http.Client{}
	httpClient.Timeout = time.Duration(runtime.ReadTimeout) * time.Second
	proxy := &url.URL{}
	if runtime.Proxy != "" {
		proxy, err = url.Parse(runtime.Proxy)
		if err != nil {
			return
		}
	}
	trans := &http.Transport{}
	if proxy != nil && runtime.Proxy != "" {
		trans.Proxy = http.ProxyURL(proxy)
	}
	trans.DialContext = utils.Timeout(time.Duration(runtime.ConnectTimeout) * time.Second)
	httpClient.Transport = trans
	httpResponse, err := hookDo(httpClient.Do)(httpRequest)
	if err != nil {
		return nil, newCredentialError("Aliyun credential endpoint request failed")
	}

	resp := &response.CommonResponse{}
	err = hookParse(resp.ParseFromHTTPResponse(httpResponse))
	if err != nil {
		return
	}
	if resp.GetHTTPStatus() != http.StatusOK {
		err = credentialErrorf("credential endpoint returned HTTP status %d", resp.GetHTTPStatus())
		return
	}
	return resp.GetHTTPContentBytes(), nil
}

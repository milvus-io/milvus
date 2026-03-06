// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package huawei

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/huaweicloud/huaweicloud-sdk-go-v3/core/auth"
	"github.com/huaweicloud/huaweicloud-sdk-go-v3/core/auth/provider"
	"github.com/huaweicloud/huaweicloud-sdk-go-v3/core/config"
	"github.com/huaweicloud/huaweicloud-sdk-go-v3/core/region"
	iam "github.com/huaweicloud/huaweicloud-sdk-go-v3/services/iam/v3"
	"github.com/huaweicloud/huaweicloud-sdk-go-v3/services/iam/v3/model"
	iamRegion "github.com/huaweicloud/huaweicloud-sdk-go-v3/services/iam/v3/region"
	"github.com/minio/minio-go/v7"
	minioCred "github.com/minio/minio-go/v7/pkg/credentials"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
)

func NewMinioClient(address string, opts *minio.Options) (*minio.Client, error) {
	if opts == nil {
		opts = &minio.Options{}
	}
	if opts.Creds == nil {
		credProvider := NewCredentialProvider()
		opts.Creds = minioCred.New(credProvider)
	}
	if address == "" {
		address = fmt.Sprintf("obs.%s.myhuaweicloud.com", opts.Region)
		opts.Secure = true
	}
	return minio.New(address, opts)
}

var (
	globalCredProvider   *HuaweiCredentialProvider
	globalCredProviderMu sync.Mutex
)

func NewCredentialProvider() minioCred.Provider {
	globalCredProviderMu.Lock()
	defer globalCredProviderMu.Unlock()
	if globalCredProvider == nil {
		globalCredProvider = &HuaweiCredentialProvider{}
	}
	return globalCredProvider
}

type HuaweiCredentialProvider struct {
	credentials minioCred.Value
	expiration  time.Time

	basicCred auth.ICredential
	regionObj *region.Region
	iamClient *iam.IamClient

	mu     sync.Mutex
	inited bool

	refreshMu sync.Mutex // serializes STS refresh calls across all minio client wrappers
}

func (p *HuaweiCredentialProvider) initClients() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.inited {
		return nil
	}

	basicChain := provider.BasicCredentialProviderChain()
	basicCred, err := basicChain.GetCredentials()
	if err != nil {
		log.Warn("HuaweiCloud credential provider: failed to get basic credentials", zap.Error(err))
		return errors.Wrap(err, "failed to get basic credentials")
	}
	p.basicCred = basicCred

	regionName := os.Getenv("HUAWEICLOUD_SDK_REGION")
	if regionName == "" {
		regionName = "cn-east-3"
	}

	regionObj, err := iamRegion.SafeValueOf(regionName)
	if err != nil {
		log.Warn("HuaweiCloud credential provider: invalid region, falling back to cn-east-3",
			zap.String("configured_region", regionName))
		regionObj, _ = iamRegion.SafeValueOf("cn-east-3")
	}
	p.regionObj = regionObj

	hcClient, err := iam.IamClientBuilder().
		WithRegion(p.regionObj).
		WithCredential(p.basicCred).
		WithHttpConfig(config.DefaultHttpConfig()).
		SafeBuild()
	if err != nil {
		log.Warn("HuaweiCloud credential provider: failed to build IAM client", zap.Error(err))
		return errors.Wrap(err, "failed to build IAM client")
	}
	p.iamClient = iam.NewIamClient(hcClient)
	p.inited = true
	log.Info("HuaweiCloud credential provider: IAM client initialized successfully",
		zap.String("region", regionName))
	return nil
}

func (p *HuaweiCredentialProvider) Retrieve() (minioCred.Value, error) {
	if err := p.initClients(); err != nil {
		return minioCred.Value{}, err
	}

	// Multiple minio Credentials wrappers share this singleton provider.
	// Each wrapper independently calls Retrieve() when its own cache expires.
	// Use refreshMu + cached check to deduplicate concurrent STS calls.
	p.refreshMu.Lock()
	defer p.refreshMu.Unlock()

	if !p.expiration.IsZero() && time.Now().UTC().Before(p.expiration.Add(-5*time.Minute)) {
		return p.credentials, nil
	}

	durationSeconds := int32(2 * 60 * 60) // 2 hours, matching C++ layer's duration
	request := &model.CreateTemporaryAccessKeyByTokenRequest{
		Body: &model.CreateTemporaryAccessKeyByTokenRequestBody{
			Auth: &model.TokenAuth{
				Identity: &model.TokenAuthIdentity{
					Methods: []model.TokenAuthIdentityMethods{model.GetTokenAuthIdentityMethodsEnum().TOKEN},
					Token: &model.IdentityToken{
						DurationSeconds: &durationSeconds,
					},
				},
			},
		},
	}

	response, err := p.iamClient.CreateTemporaryAccessKeyByToken(request)
	if err != nil {
		log.Warn("HuaweiCloud credential provider: failed to create temporary access key", zap.Error(err))
		return minioCred.Value{}, errors.Wrap(err, "failed to create temporary access key")
	}

	if response.Credential == nil ||
		response.Credential.Access == "" || response.Credential.Secret == "" || response.Credential.Securitytoken == "" {
		log.Warn("HuaweiCloud credential provider: STS returned nil or incomplete credentials")
		return minioCred.Value{}, errors.New("incomplete credential returned from Huawei Cloud (missing ak/sk/token)")
	}

	expiration, err := time.Parse("2006-01-02T15:04:05Z", response.Credential.ExpiresAt)
	if err != nil {
		log.Warn("HuaweiCloud credential provider: failed to parse expiration time",
			zap.String("expires_at", response.Credential.ExpiresAt), zap.Error(err))
		return minioCred.Value{}, errors.Wrap(err, "failed to parse expiration time")
	}

	credentials := minioCred.Value{
		AccessKeyID:     response.Credential.Access,
		SecretAccessKey: response.Credential.Secret,
		SessionToken:    response.Credential.Securitytoken,
		SignerType:      minioCred.SignatureV4,
	}

	p.credentials = credentials
	p.expiration = expiration

	akPrefix := response.Credential.Access
	if len(akPrefix) > 4 {
		akPrefix = akPrefix[:4] + "***"
	}
	log.Info("HuaweiCloud credential provider: credentials retrieved successfully",
		zap.String("ak_prefix", akPrefix), zap.Time("expiration", expiration))

	return credentials, nil
}

func (p *HuaweiCredentialProvider) IsExpired() bool {
	p.refreshMu.Lock()
	expiration := p.expiration
	p.refreshMu.Unlock()
	return time.Now().UTC().After(expiration.Add(-5 * time.Minute))
}

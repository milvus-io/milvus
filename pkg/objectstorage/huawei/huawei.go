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
	"sync/atomic"
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

	"github.com/milvus-io/milvus/pkg/v2/log"
)

const (
	reloadCooldownNormal  = 30 * time.Second // cooldown when valid credentials exist
	reloadCooldownUrgent  = 5 * time.Second  // cooldown when credentials are empty/expired
	expirationGracePeriod = 3 * time.Minute  // refresh credentials before expiration, matching C++ layer's 180s
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

// NewCredentialProvider returns a process-wide singleton credential provider.
// This assumes a single HuaweiCloud region/configuration per Milvus process,
// which matches the current deployment model where all storage operations
// target the same OBS region.
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

	refreshMu            sync.RWMutex // serializes STS refresh calls; RLock for IsExpired, Lock for Retrieve
	lastReloadFailed     bool
	lastFailedReloadTime time.Time
	stsSuccessCount      atomic.Int64
	stsFailureCount      atomic.Int64
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
		endpoint := fmt.Sprintf("https://iam.%s.myhuaweicloud.com", regionName)
		regionObj = region.NewRegion(regionName, endpoint)
		log.Warn("HuaweiCloud credential provider: region not in SDK, using constructed endpoint",
			zap.String("region", regionName), zap.String("endpoint", endpoint))
	}
	p.regionObj = regionObj

	hcClient, err := iam.IamClientBuilder().
		WithRegion(p.regionObj).
		WithCredential(p.basicCred).
		WithHttpConfig(config.DefaultHttpConfig().WithTimeout(30 * time.Second)).
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

// hasValidCachedCredentials returns true if cached credentials exist and have not
// yet fully expired (past the absolute expiration time, not the grace period).
// Must be called with refreshMu held.
func (p *HuaweiCredentialProvider) hasValidCachedCredentials() bool {
	return p.credentials.AccessKeyID != "" &&
		!p.expiration.IsZero() &&
		time.Now().UTC().Before(p.expiration)
}

// isInCooldown returns true if a recent reload failure occurred and the cooldown
// period has not yet elapsed. Uses shorter cooldown when credentials are
// empty/expired (urgent) vs when valid credentials still exist (normal).
// Must be called with refreshMu held.
func (p *HuaweiCredentialProvider) isInCooldown() bool {
	if !p.lastReloadFailed {
		return false
	}
	cooldown := reloadCooldownNormal
	if p.expiration.IsZero() || time.Now().UTC().After(p.expiration) {
		cooldown = reloadCooldownUrgent
	}
	return time.Since(p.lastFailedReloadTime) < cooldown
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

	if !p.expiration.IsZero() && time.Now().UTC().Before(p.expiration.Add(-expirationGracePeriod)) {
		return p.credentials, nil
	}

	// Throttle retries after STS failures to avoid hammering the service.
	// Only return cached credentials if they haven't fully expired yet;
	// returning already-expired credentials would cause silent auth failures.
	if p.isInCooldown() {
		if p.hasValidCachedCredentials() {
			log.Warn("HuaweiCloud credential provider: in cooldown after failure, returning cached credentials",
				zap.Time("cached_expiration", p.expiration))
			return p.credentials, nil
		}
		log.Warn("HuaweiCloud credential provider: in cooldown after failure, no valid cached credentials available")
		return minioCred.Value{}, errors.New("STS refresh in cooldown, no valid cached credentials available")
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
		p.stsFailureCount.Add(1)
		p.lastReloadFailed = true
		p.lastFailedReloadTime = time.Now()
		if p.hasValidCachedCredentials() {
			log.Warn("HuaweiCloud credential provider: STS refresh failed, falling back to cached credentials",
				zap.Time("cached_expiration", p.expiration),
				zap.Int64("sts_success", p.stsSuccessCount.Load()),
				zap.Int64("sts_failure", p.stsFailureCount.Load()),
				zap.Error(err))
			return p.credentials, nil
		}
		log.Warn("HuaweiCloud credential provider: failed to create temporary access key",
			zap.Int64("sts_success", p.stsSuccessCount.Load()),
			zap.Int64("sts_failure", p.stsFailureCount.Load()),
			zap.Error(err))
		return minioCred.Value{}, errors.Wrap(err, "failed to create temporary access key")
	}

	if response.Credential == nil ||
		response.Credential.Access == "" || response.Credential.Secret == "" || response.Credential.Securitytoken == "" {
		p.stsFailureCount.Add(1)
		p.lastReloadFailed = true
		p.lastFailedReloadTime = time.Now()
		if p.hasValidCachedCredentials() {
			log.Warn("HuaweiCloud credential provider: STS returned incomplete credentials, falling back to cached credentials",
				zap.Time("cached_expiration", p.expiration),
				zap.Int64("sts_success", p.stsSuccessCount.Load()),
				zap.Int64("sts_failure", p.stsFailureCount.Load()))
			return p.credentials, nil
		}
		log.Warn("HuaweiCloud credential provider: STS returned nil or incomplete credentials",
			zap.Int64("sts_success", p.stsSuccessCount.Load()),
			zap.Int64("sts_failure", p.stsFailureCount.Load()))
		return minioCred.Value{}, errors.New("incomplete credential returned from Huawei Cloud (missing ak/sk/token)")
	}

	expiration, err := time.Parse(time.RFC3339, response.Credential.ExpiresAt)
	if err != nil {
		p.stsFailureCount.Add(1)
		log.Warn("HuaweiCloud credential provider: failed to parse expiration time",
			zap.String("expires_at", response.Credential.ExpiresAt),
			zap.Int64("sts_success", p.stsSuccessCount.Load()),
			zap.Int64("sts_failure", p.stsFailureCount.Load()),
			zap.Error(err))
		p.lastReloadFailed = true
		p.lastFailedReloadTime = time.Now()
		if p.hasValidCachedCredentials() {
			return p.credentials, nil
		}
		return minioCred.Value{}, errors.Wrap(err, "failed to parse expiration time")
	}

	p.stsSuccessCount.Add(1)
	credentials := minioCred.Value{
		AccessKeyID:     response.Credential.Access,
		SecretAccessKey: response.Credential.Secret,
		SessionToken:    response.Credential.Securitytoken,
		Expiration:      expiration,
		SignerType:      minioCred.SignatureV4,
	}

	p.credentials = credentials
	p.expiration = expiration
	p.lastReloadFailed = false

	akPrefix := response.Credential.Access
	if len(akPrefix) > 4 {
		akPrefix = akPrefix[:4] + "***"
	}
	log.Info("HuaweiCloud credential provider: credentials retrieved successfully",
		zap.String("ak_prefix", akPrefix), zap.Time("expiration", expiration),
		zap.Int64("sts_success", p.stsSuccessCount.Load()),
		zap.Int64("sts_failure", p.stsFailureCount.Load()))

	return credentials, nil
}

func (p *HuaweiCredentialProvider) IsExpired() bool {
	p.refreshMu.RLock()
	expiration := p.expiration
	p.refreshMu.RUnlock()
	return time.Now().UTC().After(expiration.Add(-expirationGracePeriod))
}

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

func NewCredentialProvider() minioCred.Provider {
	return &HuaweiCredentialProvider{}
}

type HuaweiCredentialProvider struct {
	credentials minioCred.Value
	expiration  time.Time

	basicCred auth.ICredential
	regionObj *region.Region
	iamClient *iam.IamClient

	initOnce sync.Once
	initErr  error
}

func (p *HuaweiCredentialProvider) initClients() {
	p.initOnce.Do(func() {
		basicChain := provider.BasicCredentialProviderChain()
		basicCred, err := basicChain.GetCredentials()
		if err != nil {
			p.initErr = errors.Wrap(err, "failed to get basic credentials")
			return
		}
		p.basicCred = basicCred

		regionName := os.Getenv("HUAWEICLOUD_SDK_REGION")
		if regionName == "" {
			regionName = "cn-east-3"
		}

		regionObj, err := iamRegion.SafeValueOf(regionName)
		if err != nil {
			regionObj, _ = iamRegion.SafeValueOf("cn-east-3")
		}
		p.regionObj = regionObj

		hcClient, err := iam.IamClientBuilder().
			WithRegion(p.regionObj).
			WithCredential(p.basicCred).
			WithHttpConfig(config.DefaultHttpConfig()).
			SafeBuild()
		if err != nil {
			p.initErr = errors.Wrap(err, "failed to build IAM client")
			return
		}
		p.iamClient = iam.NewIamClient(hcClient)
	})
}

func (p *HuaweiCredentialProvider) Retrieve() (minioCred.Value, error) {
	p.initClients()
	if p.initErr != nil {
		return minioCred.Value{}, p.initErr
	}

	request := &model.CreateTemporaryAccessKeyByTokenRequest{
		Body: &model.CreateTemporaryAccessKeyByTokenRequestBody{
			Auth: &model.TokenAuth{
				Identity: &model.TokenAuthIdentity{
					Methods: []model.TokenAuthIdentityMethods{model.GetTokenAuthIdentityMethodsEnum().TOKEN},
				},
			},
		},
	}

	response, err := p.iamClient.CreateTemporaryAccessKeyByToken(request)
	if err != nil {
		return minioCred.Value{}, errors.Wrap(err, "failed to create temporary access key")
	}

	if response.Credential == nil {
		return minioCred.Value{}, errors.New("no credential returned from Huawei Cloud")
	}

	expiration, err := time.Parse("2006-01-02T15:04:05Z", response.Credential.ExpiresAt)
	if err != nil {
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

	return credentials, nil
}

func (p *HuaweiCredentialProvider) IsExpired() bool {
	return time.Now().UTC().After(p.expiration.Add(-5 * time.Minute))
}

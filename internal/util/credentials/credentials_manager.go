/*
 * # Licensed to the LF AI & Data foundation under one
 * # or more contributor license agreements. See the NOTICE file
 * # distributed with this work for additional information
 * # regarding copyright ownership. The ASF licenses this file
 * # to you under the Apache License, Version 2.0 (the
 * # "License"); you may not use this file except in compliance
 * # with the License. You may obtain a copy of the License at
 * #
 * #     http://www.apache.org/licenses/LICENSE-2.0
 * #
 * # Unless required by applicable law or agreed to in writing, software
 * # distributed under the License is distributed on an "AS IS" BASIS,
 * # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * # See the License for the specific language governing permissions and
 * # limitations under the License.
 */

package credentials

import (
	"encoding/base64"
	"fmt"
)

const (
	APIKey          string = "apikey"
	AccessKeyId     string = "access_key_id"
	SecretAccessKey string = "secret_access_key"
	// #nosec G101
	CredentialJSON string = "credential_json"
)

// The current version only supports plain text, and cipher text will be supported later.
type CredentialsManager struct {
	// key formats:
	// {credentialName}.api_key
	// {credentialName}.access_key_id
	// {credentialName}.secret_access_key
	// {credentialName}.credential_json
	confMap map[string]string
}

func NewCredentialsManager(conf map[string]string) *CredentialsManager {
	return &CredentialsManager{conf}
}

func (c *CredentialsManager) GetAPIKeyCredential(name string) (string, error) {
	k := name + "." + APIKey
	apikey, exist := c.confMap[k]
	if !exist {
		return "", fmt.Errorf("%s is not a apikey crediential, can not find key: %s", name, k)
	}
	return apikey, nil
}

func (c *CredentialsManager) GetAKSKCredential(name string) (string, string, error) {
	IdKey := name + "." + AccessKeyId
	accessKeyId, exist := c.confMap[IdKey]
	if !exist {
		return "", "", fmt.Errorf("%s is not a aksk crediential, can not find key: %s", name, IdKey)
	}

	AccessKey := name + "." + SecretAccessKey
	secretAccessKey, exist := c.confMap[AccessKey]
	if !exist {
		return "", "", fmt.Errorf("%s is not a aksk crediential, can not find key: %s", name, AccessKey)
	}
	return accessKeyId, secretAccessKey, nil
}

func (c *CredentialsManager) GetGcpCredential(name string) ([]byte, error) {
	k := name + "." + CredentialJSON
	jsonByte, exist := c.confMap[k]
	if !exist {
		return nil, fmt.Errorf("%s is not a gcp crediential, can not find key: %s ", name, k)
	}

	decode, err := base64.StdEncoding.DecodeString(jsonByte)
	if err != nil {
		return nil, fmt.Errorf("Parse gcp credential:%s faild, err: %s", name, err)
	}
	return decode, nil
}

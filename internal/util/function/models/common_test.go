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

package models

import (
	"os"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/util/credentials"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type CommonSuite struct {
	suite.Suite
}

func TestCommon(t *testing.T) {
	suite.Run(t, new(CommonSuite))
}

func (s *CommonSuite) TestParseAKAndURL() {
	{
		apiKey, _, _ := ParseAKAndURL(&credentials.Credentials{}, []*commonpb.KeyValuePair{}, map[string]string{}, OpenaiAKEnvStr, &ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"})
		s.Equal(apiKey, "")
	}
	{
		os.Setenv(OpenaiAKEnvStr, "TEST")
		apiKey, _, _ := ParseAKAndURL(&credentials.Credentials{}, []*commonpb.KeyValuePair{}, map[string]string{}, OpenaiAKEnvStr, &ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"})
		s.Equal(apiKey, "TEST")
		os.Unsetenv(OpenaiAKEnvStr)
	}
	{
		os.Setenv("MILVUSAI_OPENAI_API_KEY", "OLD_TEST")
		apiKey, _, _ := ParseAKAndURL(&credentials.Credentials{}, []*commonpb.KeyValuePair{}, map[string]string{}, OpenaiAKEnvStr, &ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"})
		s.Equal(apiKey, "OLD_TEST")
		os.Unsetenv("MILVUSAI_OPENAI_API_KEY")
	}
	{
		os.Setenv(OpenaiAKEnvStr, "TEST")
		os.Setenv("MILVUSAI_OPENAI_API_KEY", "OLD_TEST")
		apiKey, _, _ := ParseAKAndURL(&credentials.Credentials{}, []*commonpb.KeyValuePair{}, map[string]string{}, OpenaiAKEnvStr, &ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"})
		s.Equal(apiKey, "TEST")
		os.Unsetenv("MILVUSAI_OPENAI_API_KEY")
		os.Unsetenv(OpenaiAKEnvStr)
	}
	{
		apiKey, _, _ := ParseAKAndURL(&credentials.Credentials{}, []*commonpb.KeyValuePair{{Key: "integration_id", Value: "test-integration"}}, map[string]string{}, OpenaiAKEnvStr, &ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"})
		s.Equal(apiKey, "test-integration|test-cluster|test-db")
	}
}

func (s *CommonSuite) TestParseTimeoutMs() {
	s.Equal(int64(45), ParseTimeoutMs([]*commonpb.KeyValuePair{}, 45))

	s.Equal(int64(90), ParseTimeoutMs([]*commonpb.KeyValuePair{{Key: TimeoutMsParamKey, Value: "90"}}, 45))

	// an invalid override falls back to the default instead of failing
	s.Equal(int64(45), ParseTimeoutMs([]*commonpb.KeyValuePair{{Key: TimeoutMsParamKey, Value: "invalid"}}, 45))

	// non-positive overrides fall back to the default
	s.Equal(int64(45), ParseTimeoutMs([]*commonpb.KeyValuePair{{Key: TimeoutMsParamKey, Value: "0"}}, 45))
	s.Equal(int64(45), ParseTimeoutMs([]*commonpb.KeyValuePair{{Key: TimeoutMsParamKey, Value: "-1"}}, 45))

	// a non-positive default falls back to 30000
	s.Equal(int64(30000), ParseTimeoutMs([]*commonpb.KeyValuePair{}, 0))

	// param key match is case-insensitive
	s.Equal(int64(120), ParseTimeoutMs([]*commonpb.KeyValuePair{{Key: "Timeout_MS", Value: "120"}}, 45))
}

func (s *CommonSuite) TestResolveTimeoutMs() {
	paramtable.Init()
	params := paramtable.Get()

	// falls back to the global function model timeout when no param is set
	params.Save(params.FunctionCfg.ModelRequestTimeout.Key, "12s")
	defer params.Reset(params.FunctionCfg.ModelRequestTimeout.Key)
	s.Equal(int64(12000), ResolveTimeoutMs([]*commonpb.KeyValuePair{}))

	// per-function param overrides the global default
	s.Equal(int64(777), ResolveTimeoutMs([]*commonpb.KeyValuePair{{Key: TimeoutMsParamKey, Value: "777"}}))
}

func (s *CommonSuite) TestParseAKForEndpoint() {
	creds := credentials.NewCredentials(map[string]string{"named.apikey": "named-key", "conf.apikey": "conf-key"})
	extraInfo := &ModelExtraInfo{}
	namedParams := []*commonpb.KeyValuePair{{Key: CredentialParamKey, Value: "named"}}

	// without a credential there is nothing to protect, any endpoint is accepted
	apiKey, err := ParseAKForEndpoint(creds, nil, map[string]string{}, "", extraInfo, "http://any.host:8080", "test.provider")
	s.NoError(err)
	s.Equal("", apiKey)

	// a credential named in the function params is only resolved for a listed endpoint
	conf := map[string]string{CredentialEndpointsConfKey: "http://tei.internal:8080, https://tei.example.com"}
	apiKey, err = ParseAKForEndpoint(creds, namedParams, conf, "", extraInfo, "http://tei.internal:8080/some/path", "test.provider")
	s.NoError(err)
	s.Equal("named-key", apiKey)
	_, err = ParseAKForEndpoint(creds, namedParams, conf, "", extraInfo, "http://other.host:8080", "test.provider")
	s.ErrorContains(err, CredentialEndpointsConfKey)
	_, err = ParseAKForEndpoint(creds, namedParams, map[string]string{}, "", extraInfo, "http://tei.internal:8080", "test.provider")
	s.ErrorContains(err, CredentialEndpointsConfKey)

	// the provider credential from milvus.yaml follows the same rule
	conf = map[string]string{CredentialParamKey: "conf", CredentialEndpointsConfKey: "https://tei.example.com"}
	apiKey, err = ParseAKForEndpoint(creds, nil, conf, "", extraInfo, "https://tei.example.com", "test.provider")
	s.NoError(err)
	s.Equal("conf-key", apiKey)
	_, err = ParseAKForEndpoint(creds, nil, conf, "", extraInfo, "http://other.host", "test.provider")
	s.ErrorContains(err, CredentialEndpointsConfKey)

	// so does a key taken from the environment
	s.T().Setenv("MILVUS_TEST_ENDPOINT_API_KEY", "env-key")
	_, err = ParseAKForEndpoint(creds, nil, map[string]string{}, "MILVUS_TEST_ENDPOINT_API_KEY", extraInfo, "http://other.host", "test.provider")
	s.ErrorContains(err, CredentialEndpointsConfKey)

	// so does an integration ID
	integrationParams := []*commonpb.KeyValuePair{{Key: IntegrationIDKey, Value: "integration"}}
	_, err = ParseAKForEndpoint(creds, integrationParams, map[string]string{}, "", extraInfo, "http://other.host", "test.provider")
	s.ErrorContains(err, CredentialEndpointsConfKey)

	// the error names the full configuration key
	_, err = ParseAKForEndpoint(creds, namedParams, map[string]string{}, "", extraInfo, "http://other.host", "function.rerank.model.providers.tei")
	s.ErrorContains(err, "function.rerank.model.providers.tei.credential_endpoints")

	// an invalid endpoint is reported as such
	_, err = ParseAKForEndpoint(creds, namedParams, conf, "", extraInfo, "", "test.provider")
	s.ErrorContains(err, "is not a valid http/https link")

	// an unknown credential name is still reported as such
	_, err = ParseAKForEndpoint(creds, []*commonpb.KeyValuePair{{Key: CredentialParamKey, Value: "unknown"}}, conf, "", extraInfo, "https://tei.example.com", "test.provider")
	s.ErrorContains(err, "unknown")
}

func (s *CommonSuite) TestIsEndpointListed() {
	listed := "http://tei.internal:8080,https://Tei.Example.com/ignored/path, http://[::1]:9000"
	for _, endpoint := range []string{
		"http://tei.internal:8080",
		"http://tei.internal:8080/",
		"HTTP://TEI.INTERNAL:8080/rerank?x=1",
		"https://tei.example.com",
		"https://tei.example.com:443/other",
		"http://[::1]:9000",
	} {
		s.True(IsEndpointListed(endpoint, listed), endpoint)
	}
	for _, endpoint := range []string{
		"http://tei.internal",                      // different port
		"https://tei.internal:8080",                // different scheme
		"http://tei.internal:8080.evil.com",        // different host
		"http://tei.internal:8080@evil.com",        // user info, the host is evil.com
		"https://tei.example.com@evil.com",         // user info, the host is evil.com
		"http://evil.com/http://tei.internal:8080", // listed origin only in the path
		"http://evil.com?u=http://tei.internal:8080",
		"ftp://tei.internal:8080",
		"tei.internal:8080",
		"",
	} {
		s.False(IsEndpointListed(endpoint, listed), endpoint)
	}
	// non-ASCII hosts never match: U+0130 lowercases to an ASCII i, but the
	// HTTP client dials the IDNA form of the host, a different domain
	s.False(IsEndpointListed("https://tei.my-a\u0130.com", "https://tei.my-ai.com"))
	s.False(IsEndpointListed("https://\u212Aey.example.com", "https://key.example.com"))
	s.False(IsEndpointListed("https://t\u00e9i.example.com", "https://t\u00e9i.example.com"))
	s.True(IsEndpointListed("https://xn--ti-9ia.example.com", "https://xn--ti-9ia.example.com"))
	s.False(IsEndpointListed("http://tei.internal:8080", ""))
	s.False(IsEndpointListed("http://tei.internal:8080", " , not a url"))
}

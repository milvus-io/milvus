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

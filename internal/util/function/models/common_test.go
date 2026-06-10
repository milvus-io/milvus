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
	timeoutMs, err := ParseTimeoutMs([]*commonpb.KeyValuePair{}, 45)
	s.NoError(err)
	s.Equal(int64(45), timeoutMs)

	timeoutMs, err = ParseTimeoutMs([]*commonpb.KeyValuePair{{Key: TimeoutMsParamKey, Value: "90"}}, 45)
	s.NoError(err)
	s.Equal(int64(90), timeoutMs)

	_, err = ParseTimeoutMs([]*commonpb.KeyValuePair{{Key: TimeoutMsParamKey, Value: "invalid"}}, 45)
	s.ErrorContains(err, "is not a valid number")

	_, err = ParseTimeoutMs([]*commonpb.KeyValuePair{{Key: TimeoutMsParamKey, Value: "0"}}, 45)
	s.ErrorContains(err, "must be greater than 0")

	timeoutMs, err = ParseTimeoutMs([]*commonpb.KeyValuePair{}, 0)
	s.NoError(err)
	s.Equal(int64(30000), timeoutMs)

	// param key match is case-insensitive
	timeoutMs, err = ParseTimeoutMs([]*commonpb.KeyValuePair{{Key: "Timeout_MS", Value: "120"}}, 45)
	s.NoError(err)
	s.Equal(int64(120), timeoutMs)

	// negative override is rejected
	_, err = ParseTimeoutMs([]*commonpb.KeyValuePair{{Key: TimeoutMsParamKey, Value: "-1"}}, 45)
	s.ErrorContains(err, "must be greater than 0")
}

func (s *CommonSuite) TestResolveTimeoutMs() {
	paramtable.Init()
	params := paramtable.Get()

	// falls back to the global function model timeout when no param is set
	params.Save(params.FunctionCfg.ModelRequestTimeoutMs.Key, "12345")
	defer params.Reset(params.FunctionCfg.ModelRequestTimeoutMs.Key)
	timeoutMs, err := ResolveTimeoutMs([]*commonpb.KeyValuePair{})
	s.NoError(err)
	s.Equal(int64(12345), timeoutMs)

	// per-function param overrides the global default
	timeoutMs, err = ResolveTimeoutMs([]*commonpb.KeyValuePair{{Key: TimeoutMsParamKey, Value: "777"}})
	s.NoError(err)
	s.Equal(int64(777), timeoutMs)
}

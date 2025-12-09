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

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/util/credentials"
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

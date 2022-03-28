// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package metricsinfo

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFillDeployMetricsWithEnv(t *testing.T) {
	var err error

	var m DeployMetrics

	commit := "commit"
	originalCommit := os.Getenv(GitCommitEnvKey)
	err = os.Setenv(GitCommitEnvKey, commit)
	assert.NoError(t, err)

	deploy := "deploy"
	originalDeploy := os.Getenv(DeployModeEnvKey)
	err = os.Setenv(DeployModeEnvKey, deploy)
	assert.NoError(t, err)

	version := "version"
	originalVersion := os.Getenv(GitBuildTagsEnvKey)
	err = os.Setenv(GitBuildTagsEnvKey, version)
	assert.NoError(t, err)

	goVersion := "go"
	originalGoVersion := os.Getenv(MilvusUsedGoVersion)
	err = os.Setenv(MilvusUsedGoVersion, goVersion)
	assert.NoError(t, err)

	buildTime := "build"
	originalBuildTime := os.Getenv(MilvusBuildTimeEnvKey)
	err = os.Setenv(MilvusBuildTimeEnvKey, buildTime)
	assert.NoError(t, err)

	FillDeployMetricsWithEnv(&m)
	assert.NotNil(t, m)
	assert.Equal(t, commit, m.SystemVersion)
	assert.Equal(t, deploy, m.DeployMode)
	assert.Equal(t, version, m.BuildVersion)
	assert.Equal(t, goVersion, m.UsedGoVersion)
	assert.Equal(t, buildTime, m.BuildTime)

	err = os.Setenv(GitCommitEnvKey, originalCommit)
	assert.NoError(t, err)

	err = os.Setenv(DeployModeEnvKey, originalDeploy)
	assert.NoError(t, err)

	err = os.Setenv(GitBuildTagsEnvKey, originalVersion)
	assert.NoError(t, err)

	err = os.Setenv(MilvusUsedGoVersion, originalGoVersion)
	assert.NoError(t, err)

	err = os.Setenv(MilvusBuildTimeEnvKey, originalBuildTime)
	assert.NoError(t, err)
}

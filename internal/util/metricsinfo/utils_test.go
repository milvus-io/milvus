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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFillDeployMetricsWithEnv(t *testing.T) {
	var m DeployMetrics

	commit := "commit"
	t.Setenv(GitCommitEnvKey, commit)

	deploy := "deploy"
	t.Setenv(DeployModeEnvKey, deploy)

	version := "version"
	t.Setenv(GitBuildTagsEnvKey, version)

	goVersion := "go"
	t.Setenv(MilvusUsedGoVersion, goVersion)

	buildTime := "build"
	t.Setenv(MilvusBuildTimeEnvKey, buildTime)

	FillDeployMetricsWithEnv(&m)
	assert.NotNil(t, m)
	assert.Equal(t, commit, m.SystemVersion)
	assert.Equal(t, deploy, m.DeployMode)
	assert.Equal(t, version, m.BuildVersion)
	assert.Equal(t, goVersion, m.UsedGoVersion)
	assert.Equal(t, buildTime, m.BuildTime)
}

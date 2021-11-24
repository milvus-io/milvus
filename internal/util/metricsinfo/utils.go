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
)

// FillDeployMetricsWithEnv fill deploy metrics with env.
func FillDeployMetricsWithEnv(m *DeployMetrics) {
	m.SystemVersion = os.Getenv(GitCommitEnvKey)
	m.DeployMode = os.Getenv(DeployModeEnvKey)
	m.BuildVersion = os.Getenv(GitBuildTagsEnvKey)
	m.UsedGoVersion = os.Getenv(MilvusUsedGoVersion)
	m.BuildTime = os.Getenv(MilvusBuildTimeEnvKey)
}

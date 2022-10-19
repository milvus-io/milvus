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

package common

const (
	// GitCommitEnvKey defines the key to retrieve the commit corresponding to the current milvus version
	// from the metrics information
	GitCommitEnvKey = "MILVUS_GIT_COMMIT"

	// DeployModeEnvKey defines the key to retrieve the current milvus deployment mode
	// from the metrics information
	DeployModeEnvKey = "DEPLOY_MODE"

	// ClusterDeployMode represents distributed deployment mode
	ClusterDeployMode = "DISTRIBUTED"

	// StandaloneDeployMode represents the stand-alone deployment mode
	StandaloneDeployMode = "STANDALONE"

	// GitBuildTagsEnvKey build tag
	GitBuildTagsEnvKey = "MILVUS_GIT_BUILD_TAGS"

	// MilvusBuildTimeEnvKey build time
	MilvusBuildTimeEnvKey = "MILVUS_BUILD_TIME"

	// MilvusUsedGoVersion used go version
	MilvusUsedGoVersion = "MILVUS_USED_GO_VERSION"
)
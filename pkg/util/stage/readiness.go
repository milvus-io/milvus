// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package stage

// ReadinessPathKey is the commonpb.Status.ExtraInfo entry in which QueryCoord
// reports the readiness path a DQL caller observed. It is telemetry only and
// never changes errors, retries or routing.
const ReadinessPathKey = "readiness_path"

// Readiness paths a DQL request can observe. They are fixed metric label values.
const (
	PathUnknown  = "unknown"
	PathDisabled = "disabled"
	PathReady    = "ready"
	PathLoading  = "loading"
	PathUnloaded = "unloaded"
)

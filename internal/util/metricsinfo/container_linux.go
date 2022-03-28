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
	"errors"
	"strings"

	"github.com/containerd/cgroups"
)

// inContainer checks if the service is running inside a container.
func inContainer() (bool, error) {
	paths, err := cgroups.ParseCgroupFile("/proc/1/cgroup")
	if err != nil {
		return false, err
	}
	devicePath := strings.TrimPrefix(paths[string(cgroups.Devices)], "/")
	return devicePath != "", nil
}

// getContainerMemLimit returns memory limit and error
func getContainerMemLimit() (uint64, error) {
	control, err := cgroups.Load(cgroups.V1, cgroups.RootPath)
	if err != nil {
		return 0, err
	}
	stats, err := control.Stat(cgroups.IgnoreNotExist)
	if err != nil {
		return 0, err
	}
	if stats.Memory == nil || stats.Memory.Usage == nil {
		return 0, errors.New("cannot find memory usage info from cGroups")
	}
	return stats.Memory.Usage.Limit, nil
}

// getContainerMemUsed returns memory usage and error
func getContainerMemUsed() (uint64, error) {
	control, err := cgroups.Load(cgroups.V1, cgroups.RootPath)
	if err != nil {
		return 0, err
	}
	stats, err := control.Stat(cgroups.IgnoreNotExist)
	if err != nil {
		return 0, err
	}
	if stats.Memory == nil || stats.Memory.Usage == nil {
		return 0, errors.New("cannot find memory usage info from cGroups")
	}
	// ref: <https://github.com/docker/cli/blob/e57b5f78de635e6e2b688686d10b830c4747c4dc/cli/command/container/stats_helpers.go#L239>
	inactiveFile := stats.Memory.TotalInactiveFile
	usage := stats.Memory.Usage.Usage
	if inactiveFile < usage {
		return usage - inactiveFile, nil
	}
	return usage, nil
}

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

package hardware

import (
	"os"

	"github.com/cockroachdb/errors"
	"github.com/containerd/cgroups/v3"
	"github.com/containerd/cgroups/v3/cgroup1"
	statsv1 "github.com/containerd/cgroups/v3/cgroup1/stats"
	"github.com/containerd/cgroups/v3/cgroup2"
	statsv2 "github.com/containerd/cgroups/v3/cgroup2/stats"
	"k8s.io/apimachinery/pkg/api/resource"
)

func getCgroupV1Stats() (*statsv1.Metrics, error) {
	manager, err := cgroup1.Load(cgroup1.StaticPath("/"))
	if err != nil {
		return nil, err
	}
	// Get the memory stats for the specified cgroup
	stats, err := manager.Stat(cgroup1.IgnoreNotExist)
	if err != nil {
		return nil, err
	}

	if stats.GetMemory() == nil || stats.GetMemory().GetUsage() == nil {
		return nil, errors.New("cannot find memory usage info from cGroupsv1")
	}
	return stats, nil
}

func getCgroupV2Stats() (*statsv2.Metrics, error) {
	manager, err := cgroup2.Load("/")
	if err != nil {
		return nil, err
	}
	// Get the memory stats for the specified cgroup
	stats, err := manager.Stat()
	if err != nil {
		return nil, err
	}

	if stats.GetMemory() == nil {
		return nil, errors.New("cannot find memory usage info from cGroupsv2")
	}
	return stats, nil
}

// getContainerMemLimit returns memory limit and error
func getContainerMemLimit() (uint64, error) {
	memoryStr := os.Getenv("MEM_LIMIT")
	if memoryStr != "" {
		memQuantity, err := resource.ParseQuantity(memoryStr)
		if err != nil {
			return 0, err
		}

		memValue := memQuantity.Value()
		return uint64(memValue), nil
	}

	var limit uint64
	// if cgroupv2 is enabled
	if cgroups.Mode() == cgroups.Unified {
		stats, err := getCgroupV2Stats()
		if err != nil {
			return 0, err
		}
		limit = stats.GetMemory().GetUsageLimit()
	} else {
		stats, err := getCgroupV1Stats()
		if err != nil {
			return 0, err
		}
		limit = stats.GetMemory().GetUsage().GetLimit()
	}
	return limit, nil
}

// getContainerMemUsed returns memory usage and error
// On cgroup v1 host, the result is `mem.Usage - mem.Stats["total_inactive_file"]` .
// On cgroup v2 host, the result is `mem.Usage - mem.Stats["inactive_file"] `.
// ref: <https://github.com/docker/cli/blob/e57b5f78de635e6e2b688686d10b830c4747c4dc/cli/command/container/stats_helpers.go#L239>
func getContainerMemUsed() (uint64, error) {
	var used uint64
	// if cgroupv2 is enabled
	if cgroups.Mode() == cgroups.Unified {
		stats, err := getCgroupV2Stats()
		if err != nil {
			return 0, err
		}
		used = stats.GetMemory().GetUsage() - stats.GetMemory().GetInactiveFile()
	} else {
		stats, err := getCgroupV1Stats()
		if err != nil {
			return 0, err
		}
		used = stats.GetMemory().GetUsage().GetUsage() - stats.GetMemory().GetTotalInactiveFile()
	}
	return used, nil
}

// fileExists checks if a file or directory exists at the given path
func fileExists(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}

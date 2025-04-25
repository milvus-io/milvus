// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build !darwin && !openbsd && !freebsd && !windows
// +build !darwin,!openbsd,!freebsd,!windows

package hardware

import (
	"os"

	"github.com/shirou/gopsutil/v3/process"
	"github.com/sirupsen/logrus"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
)

var proc *process.Process

func init() {
	var err error
	proc, err = process.NewProcess(int32(os.Getpid()))
	if err != nil {
		panic(err)
	}

	// avoid to output a lot of error logs from cgroups package
	logrus.SetLevel(logrus.PanicLevel)
}

// GetUsedMemoryCount returns the memory usage in bytes.
func GetUsedMemoryCount() uint64 {
	memInfo, err := proc.MemoryInfoEx()
	if err != nil {
		log.Warn("failed to get memory info", zap.Error(err))
		return 0
	}

	// sub the shared memory to filter out the file-backed map memory usage
	return memInfo.RSS - memInfo.Shared
}

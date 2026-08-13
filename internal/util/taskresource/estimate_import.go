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

package taskresource

import (
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// ImportInput carries everything the import family of estimators needs. It
// covers preimport, import, and their L0 (delete) variants.
type ImportInput struct {
	IsPreImport bool
	IsL0        bool
	FileNum     int
	// VChannelNum and PartitionNum drive the per-file buffer of a real import.
	VChannelNum  int
	PartitionNum int
	// MaxFileMemorySize caps the per-file buffer; a file smaller than the
	// computed buffer cannot fill it.
	MaxFileMemorySize int64
}

// ImportConcurrentFiles returns how many files of one task can be in flight at
// once. Import submits every file to the shared exec pool
// (internal/datanode/importv2/task_import.go), and each in-flight file holds
// its own read buffer via GetMemoryAllocator().BlockingAllocate, so this
// multiplier belongs in the estimate: charging a single buffer per task (the
// old coord formula) undercounts by up to the pool width.
func ImportConcurrentFiles(fileNum int) int {
	cfg := &paramtable.Get().DataNodeCfg

	poolWidth := hardware.GetCPUNum() * cfg.ImportConcurrencyPerCPUCore.GetAsInt()
	if poolWidth < 1 {
		poolWidth = 1
	}
	if fileNum <= 0 {
		return 1
	}
	if fileNum < poolWidth {
		return fileNum
	}
	return poolWidth
}

// EstimateImport covers preimport, import, and their L0 variants.
//
// Two defects in the previous formula are fixed here. It charged a single
// buffer for the whole task even though one is allocated per in-flight file
// (see ImportConcurrentFiles), and it computed
// memoryBasedSlots := taskBufferSize / ImportMemoryLimitPerSlot, an integer
// division that yields zero for the common 32MiB/160MiB case (16MiB base x 2
// vchannels x 1 partition) and so removed the memory constraint altogether.
func EstimateImport(in ImportInput) Requirement {
	cfg := &paramtable.Get().DataNodeCfg

	// ImportBaseBufferSize and ImportDeleteBufferSize have keys ending in
	// "InMB" and a DefaultValue of "16", but both carry a Formatter that
	// converts MB to bytes at load time, so GetAsInt64() below already
	// returns bytes (16777216), not megabytes. Do not multiply by mib here.
	var perFile int64
	switch {
	case in.IsL0:
		perFile = cfg.ImportDeleteBufferSize.GetAsInt64()
	case in.IsPreImport:
		perFile = cfg.ImportBaseBufferSize.GetAsInt64()
	default:
		vch := atLeastInt(in.VChannelNum, 1)
		part := atLeastInt(in.PartitionNum, 1)
		perFile = cfg.ImportBaseBufferSize.GetAsInt64() * int64(vch) * int64(part)
		if in.MaxFileMemorySize > 0 && in.MaxFileMemorySize < perFile {
			perFile = in.MaxFileMemorySize
		}
	}

	concurrent := int64(ImportConcurrentFiles(in.FileNum))
	cpuPerFile := paramtable.Get().DataCoordCfg.ResourceImportCPUPerFile.GetAsFloat()

	return Requirement{
		CPU:    float64(concurrent) * cpuPerFile,
		Memory: atLeast(perFile*concurrent, 64*mib),
	}
}

// EstimateCopySegment is data-independent: copyFile calls ChunkManager.Copy,
// which for RemoteChunkManager resolves to client.CopyObject, a server-side
// object-store copy. No segment bytes pass through this process, and the task
// itself reports GetBufferSize() == 0. CPU still scales with how many copies
// can run at once, bounded by the same exec pool as import.
func EstimateCopySegment(segmentNum int) Requirement {
	cfg := &paramtable.Get().DataCoordCfg

	mem := cfg.ResourceCopySegmentMemory.GetAsInt64()
	concurrent := ImportConcurrentFiles(segmentNum)

	return Requirement{
		CPU:    0.1 * float64(concurrent),
		Memory: atLeast(mem, 64*mib),
	}
}

// EstimateRefreshExternalCollection is a light, flat-cost metadata refresh:
// no segment or file data is read.
func EstimateRefreshExternalCollection() Requirement {
	cfg := &paramtable.Get().DataCoordCfg

	mem := cfg.ResourceRefreshExternalMemory.GetAsInt64()

	return Requirement{
		CPU:    0.1,
		Memory: atLeast(mem, 64*mib),
	}
}

func atLeastInt(v, floor int) int {
	if v < floor {
		return floor
	}
	return v
}

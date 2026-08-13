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
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestImportConcurrentFilesBoundedByPoolAndFileCount(t *testing.T) {
	paramtable.Init()

	mk := mockey.Mock(hardware.GetCPUNum).Return(8).Build()
	defer mk.UnPatch()

	pt := paramtable.Get()
	pt.Save(pt.DataNodeCfg.ImportConcurrencyPerCPUCore.Key, "4")
	defer pt.Reset(pt.DataNodeCfg.ImportConcurrencyPerCPUCore.Key)

	// pool width is 8*4 = 32
	assert.Equal(t, 32, ImportConcurrentFiles(1000))
	// but a task with 3 files can only run 3 at once
	assert.Equal(t, 3, ImportConcurrentFiles(3))
	assert.Equal(t, 1, ImportConcurrentFiles(0))
}

// The current coord formula charges one buffer per task. task_import.go
// allocates one per file inside the exec pool, so the real peak is
// bufferSize * concurrentFiles.
func TestEstimateImportMultipliesByConcurrency(t *testing.T) {
	paramtable.Init()

	mk := mockey.Mock(hardware.GetCPUNum).Return(8).Build()
	defer mk.UnPatch()

	oneFile := EstimateImport(ImportInput{FileNum: 1, VChannelNum: 2, PartitionNum: 1})
	manyFiles := EstimateImport(ImportInput{FileNum: 100, VChannelNum: 2, PartitionNum: 1})

	assert.Greater(t, manyFiles.Memory, oneFile.Memory)
	assert.GreaterOrEqual(t, manyFiles.Memory, 10*oneFile.Memory)
}

// 16MiB / 160MiB integer-divides to zero in the old formula, silently
// dropping the memory constraint entirely.
func TestEstimateImportNeverZero(t *testing.T) {
	paramtable.Init()

	got := EstimateImport(ImportInput{FileNum: 1, VChannelNum: 1, PartitionNum: 1})
	assert.Greater(t, got.Memory, int64(0))
	assert.Greater(t, got.CPU, float64(0))
}

func TestEstimatePreImportUsesBaseBuffer(t *testing.T) {
	paramtable.Init()

	mk := mockey.Mock(hardware.GetCPUNum).Return(8).Build()
	defer mk.UnPatch()

	pre := EstimateImport(ImportInput{IsPreImport: true, FileNum: 4, VChannelNum: 16, PartitionNum: 64})
	imp := EstimateImport(ImportInput{FileNum: 4, VChannelNum: 16, PartitionNum: 64})

	// PreImport uses a fixed buffer, so vchannel/partition fan-out must not
	// inflate it the way it does for import.
	assert.Less(t, pre.Memory, imp.Memory)
}

// cm.Copy resolves to a server-side CopyObject, so no segment bytes ever
// pass through this process.
func TestEstimateCopySegmentIsDataIndependent(t *testing.T) {
	paramtable.Init()

	few := EstimateCopySegment(1)
	many := EstimateCopySegment(1000)

	assert.Equal(t, few.Memory, many.Memory)
	assert.Greater(t, many.CPU, few.CPU)
}

func TestEstimateRefreshExternalCollectionIsLight(t *testing.T) {
	paramtable.Init()

	got := EstimateRefreshExternalCollection()
	assert.Greater(t, got.Memory, int64(0))
	assert.LessOrEqual(t, got.Memory, 256*mib)
	assert.LessOrEqual(t, got.CPU, float64(1))
}

// --- Additional exact-value tests: the loose Greater/Less assertions above
// (as specified by the brief) pass even if the formula scales by the wrong
// factor. These pin concrete numbers so a wrong constant or a dropped
// multiplier actually fails the suite.

func TestImportConcurrentFilesFloorsPoolWidthAtOne(t *testing.T) {
	paramtable.Init()

	// poolWidth = GetCPUNum() * concurrencyPerCPUCore = 0 * 0 = 0, which must
	// be floored to 1 so a task is never estimated as unable to run at all.
	mk := mockey.Mock(hardware.GetCPUNum).Return(0).Build()
	defer mk.UnPatch()

	pt := paramtable.Get()
	pt.Save(pt.DataNodeCfg.ImportConcurrencyPerCPUCore.Key, "0")
	defer pt.Reset(pt.DataNodeCfg.ImportConcurrencyPerCPUCore.Key)

	assert.Equal(t, 1, ImportConcurrentFiles(5))
}

func TestEstimateImportExactValueForCommonCase(t *testing.T) {
	paramtable.Init()

	// This is the exact case that used to break: taskBufferSize (32MiB) /
	// ImportMemoryLimitPerSlot (160MiB) integer-divides to zero. Concurrency
	// is picked so perFile*concurrent clears the 64MiB floor (see the
	// package doc note below), otherwise the assertion would pin the floor
	// value instead of the formula.
	mk := mockey.Mock(hardware.GetCPUNum).Return(1).Build()
	defer mk.UnPatch()

	pt := paramtable.Get()
	pt.Save(pt.DataNodeCfg.ImportConcurrencyPerCPUCore.Key, "4")
	defer pt.Reset(pt.DataNodeCfg.ImportConcurrencyPerCPUCore.Key)

	// poolWidth = 1*4 = 4; fileNum=4 is not < poolWidth, so concurrent = 4.
	// perFile = 16MiB base * 2 vchannels * 1 partition = 32MiB.
	got := EstimateImport(ImportInput{FileNum: 4, VChannelNum: 2, PartitionNum: 1})

	assert.Equal(t, 128*mib, got.Memory)
	assert.Equal(t, 2.0, got.CPU)
}

func TestEstimateImportL0UsesDeleteBufferExactValue(t *testing.T) {
	paramtable.Init()

	mk := mockey.Mock(hardware.GetCPUNum).Return(2).Build()
	defer mk.UnPatch()

	pt := paramtable.Get()
	pt.Save(pt.DataNodeCfg.ImportConcurrencyPerCPUCore.Key, "4")
	defer pt.Reset(pt.DataNodeCfg.ImportConcurrencyPerCPUCore.Key)

	// poolWidth = 2*4 = 8; fileNum=8 is not < poolWidth, so concurrent = 8.
	// L0 uses the flat delete buffer, unaffected by vchannel/partition fanout.
	got := EstimateImport(ImportInput{IsL0: true, FileNum: 8, VChannelNum: 16, PartitionNum: 64})

	deleteBuffer := pt.DataNodeCfg.ImportDeleteBufferSize.GetAsInt64()
	assert.Equal(t, deleteBuffer*8, got.Memory)
	assert.Equal(t, 128*mib, got.Memory)
}

func TestEstimatePreImportExactValue(t *testing.T) {
	paramtable.Init()

	mk := mockey.Mock(hardware.GetCPUNum).Return(2).Build()
	defer mk.UnPatch()

	pt := paramtable.Get()
	pt.Save(pt.DataNodeCfg.ImportConcurrencyPerCPUCore.Key, "4")
	defer pt.Reset(pt.DataNodeCfg.ImportConcurrencyPerCPUCore.Key)

	// poolWidth = 8; fileNum=8 -> concurrent = 8.
	got := EstimateImport(ImportInput{IsPreImport: true, FileNum: 8, VChannelNum: 16, PartitionNum: 64})

	baseBuffer := pt.DataNodeCfg.ImportBaseBufferSize.GetAsInt64()
	assert.Equal(t, baseBuffer*8, got.Memory)
	assert.Equal(t, 128*mib, got.Memory)
}

// A zero VChannelNum/PartitionNum must be floored to 1, not treated as a
// zero-size multiplier that would erase the buffer entirely.
func TestEstimateImportZeroFanoutFloorsToOne(t *testing.T) {
	paramtable.Init()

	mk := mockey.Mock(hardware.GetCPUNum).Return(2).Build()
	defer mk.UnPatch()

	pt := paramtable.Get()
	pt.Save(pt.DataNodeCfg.ImportConcurrencyPerCPUCore.Key, "4")
	defer pt.Reset(pt.DataNodeCfg.ImportConcurrencyPerCPUCore.Key)

	// poolWidth = 8; fileNum=8 -> concurrent = 8. 16MiB*8 = 128MiB clears the
	// 64MiB floor, so this pins the atLeastInt(0, 1) substitution rather than
	// the floor: an un-floored zero multiplier would collapse perFile to 0,
	// which atLeast(0, 64MiB) would then silently round up to 64MiB --
	// different from, not equal to, the one-fanout case below.
	zero := EstimateImport(ImportInput{FileNum: 8, VChannelNum: 0, PartitionNum: 0})
	one := EstimateImport(ImportInput{FileNum: 8, VChannelNum: 1, PartitionNum: 1})

	assert.Equal(t, one.Memory, zero.Memory)
	assert.Equal(t, 128*mib, zero.Memory)
}

// MaxFileMemorySize must cap the per-file buffer before it is multiplied by
// concurrency, not merely bound the final total.
func TestEstimateImportCapsAtMaxFileMemorySize(t *testing.T) {
	paramtable.Init()

	mk := mockey.Mock(hardware.GetCPUNum).Return(1).Build()
	defer mk.UnPatch()

	pt := paramtable.Get()
	pt.Save(pt.DataNodeCfg.ImportConcurrencyPerCPUCore.Key, "4")
	defer pt.Reset(pt.DataNodeCfg.ImportConcurrencyPerCPUCore.Key)

	// poolWidth = 1*4 = 4; fileNum=4 is not < poolWidth, so concurrent = 4.
	// Uncapped perFile = 16MiB*2*1 = 32MiB; MaxFileMemorySize caps it to 20MiB.
	got := EstimateImport(ImportInput{
		FileNum:           4,
		VChannelNum:       2,
		PartitionNum:      1,
		MaxFileMemorySize: 20 * mib,
	})

	assert.Equal(t, 80*mib, got.Memory)
	assert.Equal(t, 2.0, got.CPU)
}

func TestEstimateCopySegmentExactValues(t *testing.T) {
	paramtable.Init()

	mk := mockey.Mock(hardware.GetCPUNum).Return(2).Build()
	defer mk.UnPatch()

	pt := paramtable.Get()
	pt.Save(pt.DataNodeCfg.ImportConcurrencyPerCPUCore.Key, "2")
	defer pt.Reset(pt.DataNodeCfg.ImportConcurrencyPerCPUCore.Key)

	// poolWidth = 2*2 = 4; segmentNum=3 < poolWidth, so concurrent = 3.
	got := EstimateCopySegment(3)

	// ResourceCopySegmentMemory defaults to 67108864, exactly 64MiB, so the
	// atLeast floor is a no-op here and the value is the raw config default.
	assert.Equal(t, int64(67108864), got.Memory)
	assert.InDelta(t, 0.3, got.CPU, 1e-9)
}

func TestEstimateRefreshExternalCollectionExactValues(t *testing.T) {
	paramtable.Init()

	got := EstimateRefreshExternalCollection()

	assert.Equal(t, int64(67108864), got.Memory)
	assert.Equal(t, 0.1, got.CPU)
}

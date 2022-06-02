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

package memoryutil

import (
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"go.uber.org/zap"
	"math"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
)

var defaultGOGC int
var previousGOGC int
var minimumGOGC int
var maximumGOGC int

var memoryPercentage float64
var highWatermarkPercentage float64
var lowWatermarkPercentage float64

var totalMemory uint64
var action func(int)

type finalizer struct {
	ref *finalizerRef
}

type finalizerRef struct {
	parent *finalizer
}

// just
func finalizerHandler(f *finalizerRef) {
	optimizeGOGC()
	runtime.SetFinalizer(f, finalizerHandler)
}

func optimizeGOGC() {
	targetUsage := uint64(float64(totalMemory) * memoryPercentage)
	highUsage := uint64(float64(totalMemory) * highWatermarkPercentage)
	lowUsage := uint64(float64(totalMemory) * lowWatermarkPercentage)
	var m runtime.MemStats
	// This will trigger a STW so be careful
	runtime.ReadMemStats(&m)
	newGoGC := previousGOGC

	// if we don't change go gc, prediction is the estimated usage of next gc(If cgo, stack and the other part of the memory change very little)
	// in Load/Build index this may not be the case because cpp memory cost will increase dramatically

	current := metricsinfo.GetUsedMemoryCount()
	if current <= 0 {
		return
	}
	// non heap memory = all memory - heap memory
	nonHeapMemory := (current - m.HeapAlloc)
	// next gc memory usage = non heap + next expected heap
	prediction := nonHeapMemory + m.NextGC

	// based on nextGC memory and GOGC rate, we can calculate last alive heap
	lastHeap := (m.NextGC * 100) / uint64(100+previousGOGC)

	// calculate the new go gc based on prediction and target usage.
	if current >= highUsage {
		// case 0: gc now, try to avoid OOMing
		// Directly release memory if current usage is already too high
		log.Warn("memory is out of highUsage boundary, force gc", zap.Uint64("current usage", current), zap.Uint64("high usage", highUsage))
		debug.FreeOSMemory()
	} else if prediction >= highUsage {
		// case 1: current message is ok, but we predict our memory usage is dangerous,
		// set GoGC to lower bound
		newGoGC = minimumGOGC
	} else if prediction >= targetUsage && prediction < highUsage {
		// case 2: current prediction is between high watermark and target usage
		// decrease the GoGC and try to start GC when memory exceed targetUsage
		newGoGC = int(((float64(targetUsage-nonHeapMemory) / float64(lastHeap)) - float64(1)) * 100)
	} else if prediction >= lowUsage && prediction < targetUsage {
		// case 3: current prediction is between low watermark and target usage
		// already in a good mode, keep it
		newGoGC = previousGOGC
	} else {
		// case 4: current prediction is large , we can enlarge GoGC for better Throughput
		// we increase GOGC to save memory
		newGoGC = int(((float64(lowUsage-nonHeapMemory) / float64(lastHeap)) - float64(1)) * 100)
	}

	// Protection, we don't want to overflow
	if newGoGC > maximumGOGC {
		newGoGC = maximumGOGC
	} else if newGoGC > minimumGOGC {
		newGoGC = minimumGOGC
	}

	action(newGoGC)

	log.Info("GC Tune done", zap.Int("GOGC set", previousGOGC),
		zap.Uint64("current memory ", current),
		zap.Uint64("current heap", m.HeapAlloc),
		zap.Uint64("previous Target Heap", m.NextGC),
		zap.Int("new GOGC", newGoGC),
	)
}

func NewHelper(targetPercent float64, minimumGOGCConfig int, maximumGOGCConfig int, fn func(int)) *finalizer {
	// initiate GOGC paramete
	if envGOGC := os.Getenv("GOGC"); envGOGC != "" {
		n, err := strconv.Atoi(envGOGC)
		if err == nil {
			defaultGOGC = n
		}
	} else {
		// the default value of GOGC is 100 for now
		defaultGOGC = 100
	}
	action = fn
	memoryPercentage = targetPercent
	// theh high watermark that we want to keep our memory in.
	highWatermarkPercentage = math.Min(targetPercent*1.15, 0.95)
	// the low watermark ensures the memory is safe
	lowWatermarkPercentage = targetPercent * 0.85

	previousGOGC = defaultGOGC
	minimumGOGC = minimumGOGCConfig
	maximumGOGC = maximumGOGCConfig

	totalMemory = metricsinfo.GetMemoryCount()
	if totalMemory == 0 {
		log.Warn("Failed to get memory count, disable gc auto tune", zap.Int("Initial GoGC", defaultGOGC))
		// noop
		action = func(int) {}
		return nil
	}
	log.Info("GC Helper initialized.", zap.Int("Initial GoGC", previousGOGC),
		zap.Float64("target percentage", targetPercent),
		zap.Int("minimum go gc", minimumGOGC),
		zap.Int("maximum go gc", maximumGOGC),
		zap.Uint64("Memory", totalMemory))
	f := &finalizer{}

	f.ref = &finalizerRef{parent: f}
	runtime.SetFinalizer(f.ref, finalizerHandler)
	f.ref = nil
	return f
}

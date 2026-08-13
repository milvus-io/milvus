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
	"fmt"

	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Requirement is the two-dimensional resource footprint of a single task.
//
// CPU is the number of cores the task is expected to keep busy; it is a
// scheduling hint, not a cgroup quota. Memory is the task's expected *peak*
// resident bytes — peak rather than average, because the ledger that consumes
// this value charges the full amount at admission time and holds it for the
// task's whole lifetime.
type Requirement struct {
	CPU    float64
	Memory int64
}

// Capacity is the two-dimensional budget of a node.
type Capacity struct {
	CPU    float64
	Memory int64
}

func (r Requirement) Add(o Requirement) Requirement {
	return Requirement{CPU: r.CPU + o.CPU, Memory: r.Memory + o.Memory}
}

// Sub clamps at zero. A ledger must never go negative: a negative balance
// would silently enlarge the budget and let extra tasks in, which is exactly
// the failure this package exists to prevent.
//
// Clamping is not snapping: subtracting exactly what was added does not
// reliably land on zero, because CPU is a float. Three additions of 0.1
// followed by three subtractions of 0.1 leave 2.7755575615628914e-17 behind,
// and nothing here removes it. A caller that needs an exact zero has to
// establish it some other way -- the DataNode guard's ledger does, by resetting
// the total outright when its last task releases.
func (r Requirement) Sub(o Requirement) Requirement {
	out := Requirement{CPU: r.CPU - o.CPU, Memory: r.Memory - o.Memory}
	if out.CPU < 0 {
		out.CPU = 0
	}
	if out.Memory < 0 {
		out.Memory = 0
	}
	return out
}

func (r Requirement) FitsIn(c Capacity) bool {
	return r.CPU <= c.CPU && r.Memory <= c.Memory
}

func (r Requirement) IsZero() bool {
	return r.CPU == 0 && r.Memory == 0
}

func (r Requirement) String() string {
	return fmt.Sprintf("{cpu=%.2f mem=%dMiB}", r.CPU, r.Memory>>20)
}

// NodeCapacity reports the raw two-dimensional budget of this node, before the
// non-task memory correction applied by the guard.
func NodeCapacity() Capacity {
	cfg := &paramtable.Get().DataNodeCfg
	cpu := float64(hardware.GetCPUNum()) * cfg.ResourceCPURatio.GetAsFloat()
	mem := int64(float64(hardware.GetMemoryCount()) * cfg.ResourceMemoryRatio.GetAsFloat())
	return Capacity{CPU: cpu, Memory: mem}
}

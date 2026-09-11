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
// The two dimensions are not the same kind of quantity, and the difference is
// the reason this type has two fields instead of the single slot it replaces.
// It is the split Borg and Kubernetes make between INCOMPRESSIBLE and
// COMPRESSIBLE resources:
//
//   - Memory is incompressible. A task that exceeds its estimate does not run
//     slower, it kills the process. So Memory is a RESERVATION: the task's
//     expected *peak* resident bytes, charged in full at admission and held for
//     the task's whole lifetime, and the one dimension admission may refuse on.
//   - CPU is compressible. Two tasks on one core both finish, just later. So
//     CPU is a REQUEST, in the Kubernetes sense: nothing enforces it, no task is
//     ever refused for want of it (see MemoryFitsIn), and it exists to place and
//     to spread rather than to admit.
//
// The CPU figure is currently a flat per-family charge and is known to be a poor
// model of the DataNode's actual behavior: a vector index build and an analyze
// task each saturate knowhere's global build thread pool
// (GetCPUNum() x DefaultKnowhereThreadPoolNumRatioInBuild, shared by both), so
// running two does not consume twice the cores -- it halves each one's speed. A
// resource of that shape is not additive and cannot be priced by a per-task
// number at all; it needs a per-node concurrency count. Scalar index and stats
// builds are the opposite: tantivy's writer runs on a single thread
// (DEFAULT_NUM_THREADS = 1), so their CPU really is additive and really is about
// one core. Fixing this is deferred; nothing in the admission path depends on
// the CPU figure, so the flat charge is inaccurate rather than unsafe.
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

// FitsIn reports whether both dimensions fit. Nothing in the admission path
// uses it -- see MemoryFitsIn for why -- but the whole-vector comparison is
// still the right question for a reporting or diagnostic caller asking whether
// a node is over-committed in any dimension at all.
func (r Requirement) FitsIn(c Capacity) bool {
	return r.CPU <= c.CPU && r.Memory <= c.Memory
}

// MemoryFitsIn reports whether the memory dimension fits. It, and not FitsIn,
// is what admission asks.
//
// Refusing a task because the node's CPU requests are already spoken for would
// contradict the point of a request: CPU is compressible, so the consequence of
// running one more task is that everything runs a little slower, not that
// anything dies. Enforcing it would serialize work the node can perfectly well
// run concurrently, and it would do so across task classes that do not even
// contend for the same thread pool -- an L0 compaction blocked behind four
// vector index builds it shares nothing with.
//
// This is also what makes the CPU request safe to set honestly at all. A vector
// index build saturates the whole knowhere build pool; a charge that reflected
// that would refuse half the node's work if admission still treated it as a
// reservation.
func (r Requirement) MemoryFitsIn(c Capacity) bool {
	return r.Memory <= c.Memory
}

func (r Requirement) IsZero() bool {
	return r.CPU == 0 && r.Memory == 0
}

func (r Requirement) String() string {
	return fmt.Sprintf("{cpu=%.2f mem=%dMiB}", r.CPU, r.Memory>>20)
}

func (c Capacity) String() string {
	return fmt.Sprintf("{cpu=%.2f mem=%dMiB}", c.CPU, c.Memory>>20)
}

// NodeCapacity reports the raw two-dimensional budget of this node, before the
// non-task memory correction applied by the guard.
func NodeCapacity() Capacity {
	cfg := &paramtable.Get().DataNodeCfg
	cpu := float64(hardware.GetCPUNum()) * cfg.ResourceCPURatio.GetAsFloat()
	mem := int64(float64(hardware.GetMemoryCount()) * cfg.ResourceMemoryRatio.GetAsFloat())
	return Capacity{CPU: cpu, Memory: mem}
}

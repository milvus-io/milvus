package taskcommon

import "fmt"

// Resource is what one task is expected to occupy on a worker for its whole
// run, or what a worker has in total / has left. CPU is whole cores; Memory is
// bytes. It is estimated only by DataCoord; a worker only adds and subtracts it.
type Resource struct {
	CPU    int64
	Memory int64
}

func (r Resource) IsZero() bool {
	return r.CPU == 0 && r.Memory == 0
}

func (r Resource) Add(o Resource) Resource {
	return Resource{CPU: r.CPU + o.CPU, Memory: r.Memory + o.Memory}
}

// Sub subtracts o and clamps each dimension at zero, so a release that exceeds
// what was booked (a request that changed mid-flight) cannot drive the ledger
// negative.
func (r Resource) Sub(o Resource) Resource {
	return Resource{CPU: max(r.CPU-o.CPU, 0), Memory: max(r.Memory-o.Memory, 0)}
}

func (r Resource) String() string {
	return fmt.Sprintf("cpu=%d memory=%d", r.CPU, r.Memory)
}

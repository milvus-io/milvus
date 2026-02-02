//go:build !cuda
// +build !cuda

package hardware

import (
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

// GPUMemoryInfo holds information about a GPU's memory
type GPUMemoryInfo struct {
	TotalMemory uint64 // Total memory available on the GPU
	FreeMemory  uint64 // Free memory available on the GPU
}

// GetAllGPUMemoryInfo returns mock GPU memory information for non-CUDA builds
func GetAllGPUMemoryInfo() ([]GPUMemoryInfo, error) {
	// Mock error to indicate no CUDA support
	return nil, merr.WrapErrServiceInternalMsg("CUDA not supported: failed to retrieve GPU memory info or no GPUs found")
}

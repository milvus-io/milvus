//go:build cuda
// +build cuda

package hardware

/*
#cgo CFLAGS: -I/usr/local/cuda/include
#cgo LDFLAGS: -L/usr/local/cuda/lib64 -lcudart
#include <cuda_runtime.h>
#include <stdlib.h>

// Structure to store GPU memory info
typedef struct {
    size_t totalMemory;
    size_t freeMemory;
} GPUMemoryInfo;

// Function to get memory info for all GPUs
int getAllGPUMemoryInfo(GPUMemoryInfo** infos) {
    int deviceCount = 0;
    cudaError_t err = cudaGetDeviceCount(&deviceCount);
    if (err != cudaSuccess || deviceCount == 0) {
        return 0; // No GPUs found or error occurred
    }

    // Allocate memory for the output array
    *infos = (GPUMemoryInfo*)malloc(deviceCount * sizeof(GPUMemoryInfo));
    if (*infos == NULL) {
        return 0; // Memory allocation failed
    }

    for (int i = 0; i < deviceCount; ++i) {
        if (cudaSetDevice(i) != cudaSuccess) {
            (*infos)[i].totalMemory = 0;
            (*infos)[i].freeMemory = 0;
            continue; // Skip if the device cannot be set
        }

        size_t freeMem = 0, totalMem = 0;
        if (cudaMemGetInfo(&freeMem, &totalMem) != cudaSuccess) {
            (*infos)[i].totalMemory = 0;
            (*infos)[i].freeMemory = 0;
            continue; // Skip if memory info cannot be fetched
        }

        (*infos)[i].totalMemory = totalMem;
        (*infos)[i].freeMemory = freeMem;
    }

    return deviceCount; // Return the number of devices processed
}
*/
import "C"

import (
	"unsafe"

	"github.com/cockroachdb/errors"
)

// GPUMemoryInfo represents a single GPU's memory information.
type GPUMemoryInfo struct {
	TotalMemory uint64 // Total memory in bytes
	FreeMemory  uint64 // Free memory in bytes
}

// GetAllGPUMemoryInfo retrieves the memory information for all available GPUs.
// It returns a slice of GPUMemoryInfo and an error if no GPUs are found or retrieval fails.
func GetAllGPUMemoryInfo() ([]GPUMemoryInfo, error) {
	var infos *C.GPUMemoryInfo

	// Call the C function to retrieve GPU memory info
	deviceCount := int(C.getAllGPUMemoryInfo(&infos))
	if deviceCount == 0 {
		return nil, errors.New("failed to retrieve GPU memory info or no GPUs found")
	}
	defer C.free(unsafe.Pointer(infos)) // Free the allocated memory

	// Convert C array to Go slice
	gpuInfos := make([]GPUMemoryInfo, 0, deviceCount)
	infoArray := (*[1 << 30]C.GPUMemoryInfo)(unsafe.Pointer(infos))[:deviceCount:deviceCount]

	for i := 0; i < deviceCount; i++ {
		info := infoArray[i]
		gpuInfos = append(gpuInfos, GPUMemoryInfo{
			TotalMemory: uint64(info.totalMemory),
			FreeMemory:  uint64(info.freeMemory),
		})
	}

	return gpuInfos, nil
}

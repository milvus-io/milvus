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

#ifdef MILVUS_GPU_VERSION
#include "server/init/GpuChecker.h"

#include <iostream>
#include <set>
#include <vector>

#include <fiu-local.h>

#include "config/Config.h"
#include "utils/Log.h"

namespace milvus {
namespace server {

namespace {
std::string
ConvertCudaVersion(int version) {
    return std::to_string(version / 1000) + "." + std::to_string((version % 100) / 10);
}
}  // namespace

const int CUDA_MIN_VERSION = 10000;  // 10.0
const float GPU_MIN_COMPUTE_CAPACITY = 6.0;
const char* NVIDIA_MIN_DRIVER_VERSION = "418.00";

std::string
GpuChecker::NvmlErrorString(nvmlReturn_t error_no) {
    return "code: " + std::to_string(error_no) + ", message: " + nvmlErrorString(error_no);
}

std::string
GpuChecker::CudaErrorString(cudaError_t error_no) {
    return "code: " + std::to_string(error_no) + ", message: " + cudaGetErrorString(error_no);
}

Status
GpuChecker::GetGpuComputeCapacity(nvmlDevice_t device, int& major, int& minor) {
    nvmlReturn_t code = nvmlDeviceGetCudaComputeCapability(device, &major, &minor);
    if (NVML_SUCCESS != code) {
        return Status(SERVER_UNEXPECTED_ERROR, NvmlErrorString(code));
    }

    return Status::OK();
}

Status
GpuChecker::GetGpuNvidiaDriverVersion(std::string& version) {
    char driver_version[NVML_SYSTEM_DRIVER_VERSION_BUFFER_SIZE];
    memset(driver_version, 0, NVML_SYSTEM_DRIVER_VERSION_BUFFER_SIZE);
    auto nvml_code = nvmlSystemGetDriverVersion(driver_version, NVML_SYSTEM_DRIVER_VERSION_BUFFER_SIZE);
    if (NVML_SUCCESS != nvml_code) {
        return Status(SERVER_UNEXPECTED_ERROR, NvmlErrorString(nvml_code));
    }

    version = std::string(driver_version);
    return Status::OK();
}

Status
GpuChecker::GetGpuCudaDriverVersion(int& version) {
    auto cuda_code = cudaDriverGetVersion(&version);
    if (cudaSuccess != cuda_code) {
        std::string error_msg = "Check cuda driver version failed. " + CudaErrorString(cuda_code);
        return Status(SERVER_UNEXPECTED_ERROR, error_msg);
    }
    return Status::OK();
}

Status
GpuChecker::GetGpuCudaRuntimeVersion(int& version) {
    auto cuda_code = cudaRuntimeGetVersion(&version);
    if (cudaSuccess != cuda_code) {
        std::string error_msg = "Check cuda runtime version failed. " + CudaErrorString(cuda_code);
        return Status(SERVER_UNEXPECTED_ERROR, error_msg);
    }
    return Status::OK();
}

Status
GpuChecker::CheckGpuEnvironment() {
    std::string err_msg;

    auto& config = Config::GetInstance();
    bool gpu_enable = true;
    auto status = config.GetGpuResourceConfigEnable(gpu_enable);
    if (!status.ok()) {
        err_msg = "Cannot check if GPUs are enable from configuration. " + status.message();
        LOG_SERVER_FATAL_ << err_msg;
        return Status(SERVER_UNEXPECTED_ERROR, err_msg);
    }
    if (!gpu_enable) {
        return Status::OK();
    }

    std::vector<int64_t> build_gpus;
    status = config.GetGpuResourceConfigBuildIndexResources(build_gpus);
    if (!status.ok()) {
        err_msg = "Get GPU resources of building index failed. " + status.message();
        LOG_SERVER_FATAL_ << err_msg;
        return Status(SERVER_UNEXPECTED_ERROR, err_msg);
    }

    std::vector<int64_t> search_gpus;
    status = config.GetGpuResourceConfigSearchResources(search_gpus);
    if (!status.ok()) {
        err_msg = "Get GPU resources of search failed. " + status.message();
        LOG_SERVER_FATAL_ << err_msg;
        return Status(SERVER_UNEXPECTED_ERROR, err_msg);
    }

    std::set<int64_t> gpu_sets(build_gpus.begin(), build_gpus.end());
    gpu_sets.insert(search_gpus.begin(), search_gpus.end());

    nvmlReturn_t nvmlresult = nvmlInit();
    fiu_do_on("GpuChecker.CheckGpuEnvironment.nvml_init_fail", nvmlresult = NVML_ERROR_UNKNOWN);
    if (NVML_SUCCESS != nvmlresult) {
        err_msg = "nvml initialize failed. " + NvmlErrorString(nvmlresult);
        LOG_SERVER_FATAL_ << err_msg;
        std::cerr << err_msg << std::endl;
        return Status(SERVER_UNEXPECTED_ERROR, err_msg);
    }

    /* Check nvidia driver version */
    std::string nvidia_version;
    status = GetGpuNvidiaDriverVersion(nvidia_version);
    fiu_do_on("GpuChecker.CheckGpuEnvironment.get_nvidia_driver_fail", status = Status(SERVER_UNEXPECTED_ERROR, ""));
    if (!status.ok()) {
        err_msg = " Check nvidia driver failed. " + status.message();
        LOG_SERVER_FATAL_ << err_msg;
        return Status(SERVER_UNEXPECTED_ERROR, err_msg);
    }

    fiu_do_on("GpuChecker.CheckGpuEnvironment.nvidia_driver_too_slow",
              nvidia_version = std::to_string(std::stof(NVIDIA_MIN_DRIVER_VERSION) - 1));
    if (nvidia_version.compare(NVIDIA_MIN_DRIVER_VERSION) < 0) {
        err_msg = "Nvidia driver version " + std::string(nvidia_version) + " is slower than " +
                  std::string(NVIDIA_MIN_DRIVER_VERSION);
        LOG_SERVER_FATAL_ << err_msg;
        std::cerr << err_msg << std::endl;
        return Status(SERVER_UNEXPECTED_ERROR, err_msg);
    }

    /* Check Cuda version */
    int cuda_driver_version = 0;
    status = GetGpuCudaDriverVersion(cuda_driver_version);
    fiu_do_on("GpuChecker.CheckGpuEnvironment.cuda_driver_fail", status = Status(SERVER_UNEXPECTED_ERROR, ""));
    if (!status.ok()) {
        err_msg = " Check Cuda driver failed. " + status.message();
        LOG_SERVER_FATAL_ << err_msg;
        return Status(SERVER_UNEXPECTED_ERROR, err_msg);
    }
    fiu_do_on("GpuChecker.CheckGpuEnvironment.cuda_driver_too_slow", cuda_driver_version = CUDA_MIN_VERSION - 1);
    if (cuda_driver_version < CUDA_MIN_VERSION) {
        err_msg = "Cuda driver version is " + ConvertCudaVersion(cuda_driver_version) +
                  ", slower than minimum required version " + ConvertCudaVersion(CUDA_MIN_VERSION);
        LOG_SERVER_FATAL_ << err_msg;
        std::cerr << err_msg << std::endl;
        return Status(SERVER_UNEXPECTED_ERROR, err_msg);
    }

    int cuda_runtime_version = 0;
    status = GetGpuCudaRuntimeVersion(cuda_runtime_version);
    fiu_do_on("GpuChecker.CheckGpuEnvironment.cuda_runtime_driver_fail", status = Status(SERVER_UNEXPECTED_ERROR, ""));
    if (!status.ok()) {
        err_msg = " Check Cuda runtime driver failed. " + status.message();
        LOG_SERVER_FATAL_ << err_msg;
        return Status(SERVER_UNEXPECTED_ERROR, err_msg);
    }
    fiu_do_on("GpuChecker.CheckGpuEnvironment.cuda_runtime_driver_too_slow",
              cuda_runtime_version = CUDA_MIN_VERSION - 1);
    if (cuda_runtime_version < CUDA_MIN_VERSION) {
        err_msg = "Cuda runtime version is " + ConvertCudaVersion(cuda_runtime_version) +
                  ", slow than minimum required version " + ConvertCudaVersion(CUDA_MIN_VERSION);
        LOG_SERVER_FATAL_ << err_msg;
        std::cerr << err_msg << std::endl;
        return Status(SERVER_UNEXPECTED_ERROR, err_msg);
    }

    /* Compute capacity */
    uint32_t device_count = 0;
    nvmlresult = nvmlDeviceGetCount(&device_count);
    fiu_do_on("GpuChecker.CheckGpuEnvironment.nvml_get_device_count_fail", nvmlresult = NVML_ERROR_UNKNOWN);
    if (NVML_SUCCESS != nvmlresult) {
        err_msg = "Obtain GPU count failed. " + NvmlErrorString(nvmlresult);
        LOG_SERVER_FATAL_ << err_msg;
        return Status(SERVER_UNEXPECTED_ERROR, err_msg);
    }

    fiu_do_on("GpuChecker.CheckGpuEnvironment.nvml_device_count_zero", device_count = 0);
    if (device_count == 0) {
        err_msg = "GPU count is zero. Make sure there are available GPUs in host machine";
        LOG_SERVER_FATAL_ << err_msg;
        return Status(SERVER_UNEXPECTED_ERROR, err_msg);
    }

    char device_name[NVML_DEVICE_NAME_BUFFER_SIZE];
    int major, minor;
    for (uint32_t i = 0; i < device_count; i++) {
        if (gpu_sets.find(i) == gpu_sets.end()) {
            continue;
        }

        nvmlDevice_t device;
        nvmlresult = nvmlDeviceGetHandleByIndex(i, &device);
        fiu_do_on("GpuChecker.CheckGpuEnvironment.nvml_get_device_handle_fail", nvmlresult = NVML_ERROR_UNKNOWN);
        if (NVML_SUCCESS != nvmlresult) {
            err_msg = "Obtain GPU " + std::to_string(i) + " handle failed. " + NvmlErrorString(nvmlresult);
            LOG_SERVER_FATAL_ << err_msg;
            return Status(SERVER_UNEXPECTED_ERROR, err_msg);
        }
        memset(device_name, 0, NVML_DEVICE_NAME_BUFFER_SIZE);
        nvmlresult = nvmlDeviceGetName(device, device_name, NVML_DEVICE_NAME_BUFFER_SIZE);
        fiu_do_on("GpuChecker.CheckGpuEnvironment.nvml_get_device_name_fail", nvmlresult = NVML_ERROR_UNKNOWN);
        if (NVML_SUCCESS != nvmlresult) {
            err_msg = "Obtain GPU " + std::to_string(i) + " name failed. " + NvmlErrorString(nvmlresult);
            LOG_SERVER_FATAL_ << err_msg;
            return Status(SERVER_UNEXPECTED_ERROR, err_msg);
        }

        major = 0;
        minor = 0;
        status = GetGpuComputeCapacity(device, major, minor);
        fiu_do_on("GpuChecker.CheckGpuEnvironment.device_compute_capacity_fail",
                  status = Status(SERVER_UNEXPECTED_ERROR, ""));
        if (!status.ok()) {
            err_msg = "Obtain GPU " + std::to_string(i) + " compute capacity failed. " + status.message();
            LOG_SERVER_FATAL_ << err_msg;
            std::cerr << err_msg << std::endl;
            return Status(SERVER_UNEXPECTED_ERROR, err_msg);
        }
        float cc = major + minor / 1.0f;
        fiu_do_on("GpuChecker.CheckGpuEnvironment.device_compute_capacity_too_weak", cc = GPU_MIN_COMPUTE_CAPACITY - 1);
        if (cc < GPU_MIN_COMPUTE_CAPACITY) {
            err_msg = "GPU " + std::to_string(i) + " compute capability " + std::to_string(cc) +
                      " is too weak. Required least GPU compute capability is " +
                      std::to_string(GPU_MIN_COMPUTE_CAPACITY);
            LOG_SERVER_FATAL_ << err_msg;
            std::cerr << err_msg << std::endl;
            return Status(SERVER_UNEXPECTED_ERROR, err_msg);
        }

        LOG_SERVER_INFO_ << "GPU" << i << ": name=" << device_name << ", compute capacity=" << cc;
    }

    nvmlresult = nvmlShutdown();
    fiu_do_on("GpuChecker.CheckGpuEnvironment.nvml_shutdown_fail", nvmlresult = NVML_ERROR_UNKNOWN);
    if (NVML_SUCCESS != nvmlresult) {
        err_msg = "nvml shutdown handle failed. " + NvmlErrorString(nvmlresult);
        LOG_SERVER_FATAL_ << err_msg;
        return Status(SERVER_UNEXPECTED_ERROR, err_msg);
    }

    std::cout << "Nvidia driver version: " << nvidia_version << "\n"
              << "CUDA Driver Version / Runtime Version : " << ConvertCudaVersion(cuda_driver_version) << " / "
              << ConvertCudaVersion(cuda_runtime_version) << std::endl;

    return Status::OK();
}

}  // namespace server
}  // namespace milvus
#endif

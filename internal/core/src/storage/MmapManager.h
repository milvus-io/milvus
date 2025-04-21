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

#pragma once

#include "storage/MmapChunkManager.h"
#include "storage/Types.h"

namespace milvus::storage {
/**
 * @brief MmapManager(singleton)
 * MmapManager holds all mmap components;
 * all mmap components use mmapchunkmanager to allocate mmap space;
 * no thread safe, only one thread init in segcore.
 */
class MmapManager {
 private:
    MmapManager() = default;

 public:
    MmapManager(const MmapManager&) = delete;
    MmapManager&
    operator=(const MmapManager&) = delete;

    static MmapManager&
    GetInstance() {
        static MmapManager instance;
        return instance;
    }
    ~MmapManager() {
        // delete mmap chunk manager at last
        if (mcm_ != nullptr) {
            mcm_ = nullptr;
        }
    }
    void
    Init(const MmapConfig& config) {
        if (init_flag_ == false) {
            std::lock_guard<std::mutex> lock(
                init_mutex_);  // in case many threads call init
            mmap_config_ = config;
            if (mcm_ == nullptr) {
                mcm_ = std::make_shared<MmapChunkManager>(
                    mmap_config_.mmap_path,
                    mmap_config_.disk_limit,
                    mmap_config_.fix_file_size);
            }
            LOG_INFO("Init MmapConfig with MmapConfig: {}",
                     mmap_config_.ToString());
            init_flag_ = true;
        } else {
            LOG_WARN("mmap manager has been inited.");
        }
    }

    MmapChunkManagerPtr
    GetMmapChunkManager() {
        AssertInfo(init_flag_ == true, "Mmap manager has not been init.");
        return mcm_;
    }

    MmapConfig&
    GetMmapConfig() {
        AssertInfo(init_flag_ == true, "Mmap manager has not been init.");
        return mmap_config_;
    }

    size_t
    GetAllocSize() {
        if (mcm_ != nullptr) {
            return mcm_->GetDiskAllocSize();
        } else {
            return 0;
        }
    }

    size_t
    GetDiskUsage() {
        if (mcm_ != nullptr) {
            return mcm_->GetDiskUsage();
        } else {
            return 0;
        }
    }

 private:
    mutable std::mutex init_mutex_;
    MmapConfig mmap_config_;
    MmapChunkManagerPtr mcm_ = nullptr;
    std::atomic<bool> init_flag_ = false;
};

}  // namespace milvus::storage
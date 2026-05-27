// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#pragma once

#include "milvus-storage/filesystem/fs.h"

namespace milvus::segcore {

// Returns the default ArrowFileSystem from FilesystemCache.
// Uses LoonFFIPropertiesSingleton to look up the cached instance.
// Throws if properties singleton is not initialized or cache lookup fails.
milvus_storage::ArrowFileSystemPtr
GetDefaultArrowFileSystem();

}  // namespace milvus::segcore

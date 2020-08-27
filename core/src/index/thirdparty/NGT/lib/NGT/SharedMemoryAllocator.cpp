//
// Copyright (C) 2015-2020 Yahoo Japan Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#include	"NGT/SharedMemoryAllocator.h"



void* operator 
new(size_t size, SharedMemoryAllocator &allocator) 
{
  void *addr = allocator.allocate(size);
#ifdef MEMORY_ALLOCATOR_INFO
  std::cerr << "new:" << size << " " << addr << " " << allocator.getTotalSize() << std::endl;
#endif
  return addr;
}

void* operator 
new[](size_t size, SharedMemoryAllocator &allocator) 
{

  void *addr = allocator.allocate(size);
#ifdef MEMORY_ALLOCATOR_INFO
  std::cerr << "new[]:" << size << " " << addr << " " << allocator.getTotalSize() << std::endl;
#endif
  return addr;
}

// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sourcecontext

/*
#cgo pkg-config: milvus_core
#include "storage/loon_ffi/source_context_c.h"
extern int MilvusOpenSourceReadContext(char*, char*, int64_t, uintptr_t*);
static int open_source(const char* source, const char* spec, int64_t deadline, uintptr_t* output) {
    return MilvusOpenSourceReadContext((char*)source, (char*)spec, deadline, output);
}
static void install_source_factory(void) { MilvusSetSourceContextFactory(open_source); }
*/
import "C"

// Installed in every process that links the source reader package. A missing Go
// provider runtime still fails closed when the native factory resolves a binding.
func init() { C.install_source_factory() }

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

extern "C" void __gcov_dump(void);
extern "C" void __gcov_reset(void);

extern "C" __attribute__((visibility("default"))) void
MilvusDumpCoverage() {
    // GCC merges each interval into the existing .gcda files. Reset re-arms
    // subsequent dumps; keep those files until the process has finished.
    __gcov_dump();
    __gcov_reset();
}

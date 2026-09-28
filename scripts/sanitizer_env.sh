#!/usr/bin/env bash
# Copyright (C) 2026 Zilliz. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Use the installed CMake metadata as well as the build-time switch. Clang may
# link its runtime statically, so looking for libtsan in ldd output is not enough.
milvus_sanitizer_env() {
    local prefix=$1 marker mode="none"
    for marker in "${prefix}/lib/milvus-sanitizer" "${prefix}/lib64/milvus-sanitizer"; do
        if [[ -f "${marker}" ]]; then
            read -r mode < "${marker}"
            break
        fi
    done
    export MILVUS_ENABLE_TSAN=OFF
    if [[ "${USE_TSAN:-OFF}" != "ON" && "${mode}" != "thread" ]]; then
        return 0
    fi
    if [[ "${USE_ASAN:-OFF}" == "ON" ]]; then
        echo "ERROR: ThreadSanitizer cannot be combined with AddressSanitizer" >&2
        return 1
    fi
    case " ${GOFLAGS:-} " in
        *" -race "*|*" -race=true "*)
            echo "ERROR: ThreadSanitizer cannot be combined with Go -race" >&2
            return 1
            ;;
    esac
    export MILVUS_ENABLE_TSAN=ON

    # Preserve unrelated preloads, removing the allocator from earlier builds.
    local preload remaining=""
    local -a preloads
    local IFS=' :'
    read -r -a preloads <<< "${LD_PRELOAD:-}"
    for preload in ${preloads[@]+"${preloads[@]}"}; do
        case "${preload##*/}" in
            *jemalloc*) continue ;;
            *asan*)
                echo "ERROR: ASan runtime in LD_PRELOAD conflicts with ThreadSanitizer" >&2
                return 1
                ;;
        esac
        remaining="${remaining:+${remaining}:}${preload}"
    done
    if [[ -n "${remaining}" ]]; then
        export LD_PRELOAD="${remaining}"
    else
        unset LD_PRELOAD
    fi
    # A detected race must fail the runner; user options may add symbolizer paths.
    export TSAN_OPTIONS="${TSAN_OPTIONS:+${TSAN_OPTIONS}:}halt_on_error=1:exitcode=66"
}

#!/usr/bin/env bash

# Licensed to the LF AI & Data foundation under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e

mkdir -p "$GOPATH/bin" && cp -f "$PWD/bin/milvus" "$GOPATH/bin/milvus"
mkdir -p "$LIBRARY_PATH"
cp $PWD"/internal/core/output/lib/"*.dylib* "$LIBRARY_PATH" 2>/dev/null || true
cp $PWD"/internal/core/output/lib/"*.so* "$LIBRARY_PATH" || true
cp $PWD"/internal/core/output/lib64/"*.so* "$LIBRARY_PATH" 2>/dev/null || true

# Enhanced ASAN library handling
if [ "$USE_ASAN" = "ON" ]; then
    echo "USE_ASAN is enabled, copying ASAN and atomic libraries..."
    
    # First, try to copy libraries found by ldd
    ASAN_LIBS_FOUND=0
    for LIB_PATH in $(ldd ./bin/milvus | grep -E '(asan|atomic)' | awk '{print $3}'); do
        if [ -f "$LIB_PATH" ]; then
            echo "Copying ASAN library: $LIB_PATH"
            cp "$LIB_PATH" "$LIBRARY_PATH" 2>/dev/null && ASAN_LIBS_FOUND=1
        fi
    done
    
    # If no ASAN libs found by ldd, try to find them manually
    if [ $ASAN_LIBS_FOUND -eq 0 ]; then
        echo "No ASAN libraries found by ldd, searching system paths..."
        
        # Search for libasan in common locations
        for SEARCH_PATH in /usr/lib /usr/lib64 /usr/lib/x86_64-linux-gnu /usr/lib/gcc/*/*; do
            if [ -d "$SEARCH_PATH" ]; then
                for ASAN_LIB in $(find "$SEARCH_PATH" -name "libasan.so*" 2>/dev/null); do
                    if [ -f "$ASAN_LIB" ]; then
                        echo "Found ASAN library: $ASAN_LIB"
                        cp "$ASAN_LIB" "$LIBRARY_PATH" 2>/dev/null && ASAN_LIBS_FOUND=1
                    fi
                done
            fi
        done
        
        # Search for libatomic as well
        for SEARCH_PATH in /usr/lib /usr/lib64 /usr/lib/x86_64-linux-gnu /usr/lib/gcc/*/*; do
            if [ -d "$SEARCH_PATH" ]; then
                for ATOMIC_LIB in $(find "$SEARCH_PATH" -name "libatomic.so*" 2>/dev/null); do
                    if [ -f "$ATOMIC_LIB" ]; then
                        echo "Found atomic library: $ATOMIC_LIB"
                        cp "$ATOMIC_LIB" "$LIBRARY_PATH" 2>/dev/null
                    fi
                done
            fi
        done
    fi
    
    # Final check and warning
    if [ $ASAN_LIBS_FOUND -eq 0 ]; then
        echo "WARNING: No ASAN libraries found! You may need to install Address Sanitizer runtime libraries."
        echo "Try: sudo apt-get install libc6-dbg (Ubuntu/Debian) or sudo yum install libasan (CentOS/RHEL)"
    fi
else
    # Original logic for non-ASAN builds
    for LIB_PATH in $(ldd ./bin/milvus | grep -E '(asan|atomic)' | awk '{print $3}'); do
        cp "$LIB_PATH" "$LIBRARY_PATH" 2>/dev/null
    done
fi

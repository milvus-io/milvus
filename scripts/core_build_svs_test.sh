#!/usr/bin/env bash
# Licensed under the Apache License, Version 2.0.
set -euo pipefail

scripts_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd "${scripts_dir}/.." && pwd)
test_root=$(mktemp -d)
trap 'rm -rf "${test_root}"' EXIT
REAL_CMAKE=$(command -v cmake)
export REAL_CMAKE
export SVS_TEST_ROOT="${test_root}"
export SVS_REPO_ROOT="${repo_root}"
mkdir -p "${test_root}/repo/scripts" "${test_root}/bin" "${test_root}/project/knowhere"
cp "${scripts_dir}/core_build.sh" "${test_root}/repo/scripts/"
printf 'set +e\n' > "${test_root}/repo/scripts/setenv.sh"

# Run the real shell entrypoint, replacing only dependency setup and compilation.
# Its actual -DWITH_SVS value configures the real Milvus Knowhere integration.
cat > "${test_root}/bin/cmake" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
svs_args=()
for arg in "$@"; do
    case "$arg" in -DWITH_SVS=*) svs_args+=("$arg");; esac
done
"${REAL_CMAKE}" -S "${SVS_TEST_ROOT}/project" -B "${SVS_CASE_DIR}" \
    -DCMAKE_SYSTEM_NAME="${SVS_TARGET_OS}" -DCMAKE_SYSTEM_PROCESSOR="${SVS_TARGET_ARCH}" \
    -DREPO_ROOT="${SVS_REPO_ROOT}" -DRUNTIME_LAYOUT="${SVS_LAYOUT}" \
    -DCMAKE_INSTALL_PREFIX="${SVS_CASE_DIR}/output" "${svs_args[@]}"
"${REAL_CMAKE}" --install "${SVS_CASE_DIR}"
STUB
printf '#!/usr/bin/env bash\nexit 0\n' > "${test_root}/bin/make"
printf '#!/usr/bin/env bash\nexit 0\n' > "${test_root}/bin/ninja"
chmod +x "${test_root}/bin/"*

cat > "${test_root}/project/CMakeLists.txt" <<'CMAKE'
cmake_minimum_required(VERSION 3.18)
project(svs_build_contract NONE)
set(CMAKE_INSTALL_LIBDIR lib)
set(FETCHCONTENT_BASE_DIR "${CMAKE_BINARY_DIR}/fetched")
# Keep the production option declarations in the path if SVS is declared there.
include("${REPO_ROOT}/internal/core/cmake/DefineOptions.cmake")
function(milvus_add_pkg_config)
endfunction()
function(FetchContent_Declare)
endfunction()
macro(FetchContent_GetProperties)
    set(knowhere_POPULATED FALSE)
    set(knowhere_SOURCE_DIR "${CMAKE_SOURCE_DIR}/knowhere")
    set(knowhere_BINARY_DIR "${CMAKE_BINARY_DIR}/knowhere-fixture")
endmacro()
function(FetchContent_Populate)
endfunction()
add_subdirectory("${REPO_ROOT}/internal/core/thirdparty/knowhere" integration)
CMAKE
cat > "${test_root}/project/knowhere/CMakeLists.txt" <<'CMAKE'
# Model the pinned Knowhere libfaiss.cmake's child-scope find_package calls.
file(WRITE "${CMAKE_BINARY_DIR}/resolved-svs" "${WITH_SVS}")
if(WITH_SVS)
    if(RUNTIME_LAYOUT STREQUAL "unresolved")
        return()
    endif()
    if(RUNTIME_LAYOUT STREQUAL "preinstalled")
        set(runtime_root "${CMAKE_BINARY_DIR}/preinstalled with spaces")
        # A stale fetched copy must not win over the selected package.
        file(MAKE_DIRECTORY "${FETCHCONTENT_BASE_DIR}/svs-src/lib")
        file(WRITE "${FETCHCONTENT_BASE_DIR}/svs-src/lib/libsvs_runtime.so" "wrong runtime\n")
    else()
        set(runtime_root "${FETCHCONTENT_BASE_DIR}/svs-src")
    endif()
    file(MAKE_DIRECTORY "${runtime_root}/lib/cmake/svs_runtime")
    file(WRITE "${runtime_root}/lib/libsvs_runtime.so.0.5.1" "selected runtime\n")
    file(CREATE_LINK libsvs_runtime.so.0.5.1 "${runtime_root}/lib/libsvs_runtime.so.0" SYMBOLIC)
    file(CREATE_LINK libsvs_runtime.so.0 "${runtime_root}/lib/libsvs_runtime.so" SYMBOLIC)
    file(WRITE "${runtime_root}/lib/unrelated.so" "do not package\n")
    file(WRITE "${runtime_root}/lib/cmake/svs_runtime/svs_runtimeConfig.cmake" "set(svs_runtime_FOUND TRUE)\n")
    set(svs_runtime_DIR "${runtime_root}/lib/cmake/svs_runtime")
    find_package(svs_runtime REQUIRED)
endif()
CMAKE

failures=0
check_value() {
    local name=$1 expected=$2 actual=$3
    if [[ "$actual" != "$expected" ]]; then
        echo "FAIL ${name}: expected ${expected}, got ${actual}"
        failures=$((failures + 1))
    else
        echo "PASS ${name}: ${actual}"
    fi
}

run_case() {
    local name=$1 target_os=$2 target_arch=$3 expected=$4 layout=$5
    shift 5
    export SVS_CASE_DIR="${test_root}/${name}" SVS_TARGET_OS="$target_os" \
        SVS_TARGET_ARCH="$target_arch" SVS_LAYOUT="$layout"
    local command=(bash "${test_root}/repo/scripts/core_build.sh")
    if [[ "${SVS_DIRECT_CMAKE:-OFF}" == ON ]]; then
        command=("${test_root}/bin/cmake")
    fi
    if ! PATH="${test_root}/bin:$PATH" CPU_TARGET=avx "${command[@]}" "$@" \
        > "${test_root}/case.log" 2>&1; then
        cat "${test_root}/case.log"
        echo "FAIL ${name}: configure/install failed"
        failures=$((failures + 1))
        return
    fi
    check_value "$name" "$expected" "$(cat "${SVS_CASE_DIR}/resolved-svs")"
    local lib_dir="${SVS_CASE_DIR}/output/lib"
    if [[ "$expected" == ON ]]; then
        if [[ -L "${lib_dir}/libsvs_runtime.so" && -L "${lib_dir}/libsvs_runtime.so.0" &&
              -f "${lib_dir}/libsvs_runtime.so.0.5.1" ]] &&
            [[ "$(cat "${lib_dir}/libsvs_runtime.so")" == 'selected runtime' ]] &&
            [[ ! -e "${lib_dir}/unrelated.so" ]]; then
            echo "PASS ${name}_runtime: library and SONAME symlinks installed"
        else
            echo "FAIL ${name}_runtime: SVS runtime or SONAME symlinks missing"
            failures=$((failures + 1))
        fi
    elif [[ -e "${lib_dir}/libsvs_runtime.so" ]]; then
        echo "FAIL ${name}_runtime: disabled SVS installed a runtime"
        failures=$((failures + 1))
    fi
}

# Use the actual Make recipe (all three entrypoints), not an invented argument list.
for target in build-cpp build-cpp-with-unittest build-cpp-with-coverage; do
    for mode in default OFF ON; do
        make_args=()
        expected=ON
        [[ "$mode" == default ]] || make_args+=("USE_SVS=$mode")
        [[ "$mode" != OFF ]] || expected=OFF
        GIT_CONFIG_GLOBAL=/dev/null make -C "$repo_root" -n GO=true \
            PWD="${test_root}/repo" -o generated-proto -o plan-parser-lib \
            "${make_args[@]}" "$target" > "${test_root}/make.log" 2>&1
        recipe=$(sed -n '/core_build.sh /p' "${test_root}/make.log")
        # Extract the arguments from the real recipe, retaining shell quoting.
        args=${recipe#*core_build.sh }
        args=${args%)}
        # The repository recipes contain shell words and quoted empty arguments.
        eval "run_case '${target}_${mode}' Linux x86_64 '$expected' fetched $args"
    done
done
run_case direct_linux_default Linux x86_64 ON fetched
run_case direct_linux_auto Linux x86_64 ON fetched -S AUTO
run_case direct_linux_arm Linux aarch64 OFF fetched
run_case direct_linux_arm_auto Linux aarch64 OFF fetched -S AUTO
run_case direct_darwin_x86 Darwin x86_64 OFF fetched
run_case direct_darwin_arm Darwin arm64 OFF fetched -S AUTO
run_case direct_linux_off Linux x86_64 OFF fetched -S OFF
run_case preinstalled_runtime Linux x86_64 ON preinstalled -S ON

SVS_DIRECT_CMAKE=ON run_case cmake_linux_default Linux x86_64 ON fetched
SVS_DIRECT_CMAKE=ON run_case cmake_linux_arm Linux aarch64 OFF fetched
SVS_DIRECT_CMAKE=ON run_case cmake_linux_amd64 Linux AMD64 ON fetched
SVS_DIRECT_CMAKE=ON run_case cmake_linux_amd64_lower Linux amd64 ON fetched
SVS_DIRECT_CMAKE=ON run_case cmake_linux_32bit Linux i686 OFF fetched
SVS_DIRECT_CMAKE=ON run_case cmake_windows Windows AMD64 OFF fetched
SVS_DIRECT_CMAKE=ON run_case cmake_linux_off Linux x86_64 OFF fetched -DWITH_SVS=OFF
SVS_DIRECT_CMAKE=ON run_case cmake_linux_auto Linux x86_64 ON fetched -DWITH_SVS=AUTO

# Fail configuration clearly when an enabled dependency did not resolve a package.
export SVS_CASE_DIR="${test_root}/unresolved_runtime" SVS_TARGET_OS=Linux \
    SVS_TARGET_ARCH=x86_64 SVS_LAYOUT=unresolved
if "${test_root}/bin/cmake" -DWITH_SVS=ON > "${test_root}/unresolved.log" 2>&1; then
    echo 'FAIL unresolved_runtime: configuration unexpectedly succeeded'
    failures=$((failures + 1))
elif grep -q 'SVS enabled but Knowhere did not resolve svs_runtime' "${test_root}/unresolved.log"; then
    echo 'PASS unresolved_runtime: missing package diagnosed'
else
    cat "${test_root}/unresolved.log"
    echo 'FAIL unresolved_runtime: missing package not diagnosed'
    failures=$((failures + 1))
fi

if [[ "$failures" -ne 0 ]]; then
    echo "SVS build regression: ${failures} failures"
    exit 1
fi
echo 'SVS build regression: all cases passed'

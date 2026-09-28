#!/usr/bin/env bash
# Licensed under the Apache License, Version 2.0.
set -euo pipefail

scripts_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "${scripts_dir}/sanitizer_env.sh"
test_root=$(mktemp -d)
trap 'rm -rf "${test_root}"' EXIT
mkdir -p "${test_root}/plain/lib" "${test_root}/tsan/lib"
printf 'thread\n' > "${test_root}/tsan/lib/milvus-sanitizer"

# The ordinary build must retain the caller's allocator and options.
(
    unset USE_TSAN USE_ASAN GOFLAGS
    export LD_PRELOAD=/tmp/libjemalloc.so TSAN_OPTIONS=verbosity=1
    milvus_sanitizer_env "${test_root}/plain"
    [[ "${MILVUS_ENABLE_TSAN}" == OFF ]]
    [[ "${LD_PRELOAD}" == /tmp/libjemalloc.so ]]
    [[ "${TSAN_OPTIONS}" == verbosity=1 ]]
)

# Installed metadata works without the original build environment. Both loader
# separators are accepted, and unrelated preloads survive allocator removal.
(
    unset USE_TSAN USE_ASAN GOFLAGS
    export LD_PRELOAD='/tmp/libfirst.so:/tmp/libjemalloc.so /tmp/liblast.so'
    export TSAN_OPTIONS=halt_on_error=0:exitcode=0
    milvus_sanitizer_env "${test_root}/tsan"
    [[ "${MILVUS_ENABLE_TSAN}" == ON ]]
    [[ "${LD_PRELOAD}" == /tmp/libfirst.so:/tmp/liblast.so ]]
    [[ "${TSAN_OPTIONS}" == *:halt_on_error=1:exitcode=66 ]]
)

# A first build has no installed metadata yet.
(
    unset USE_ASAN GOFLAGS TSAN_OPTIONS
    export USE_TSAN=ON LD_PRELOAD=/tmp/libjemalloc.so.2
    milvus_sanitizer_env "${test_root}/plain"
    [[ "${MILVUS_ENABLE_TSAN}" == ON ]]
    [[ -z "${LD_PRELOAD:-}" ]]
)

# An empty preload list must also work with Bash 3.2 and nounset enabled.
(
    unset USE_ASAN GOFLAGS LD_PRELOAD
    export USE_TSAN=ON
    milvus_sanitizer_env "${test_root}/plain"
    [[ -z "${LD_PRELOAD:-}" ]]
)

for conflict in asan race preload; do
    (
        unset USE_ASAN GOFLAGS LD_PRELOAD
        export USE_TSAN=ON
        case "${conflict}" in
            asan) export USE_ASAN=ON ;;
            race) export GOFLAGS='-trimpath -race=true' ;;
            preload) export LD_PRELOAD=/tmp/libasan.so.8 ;;
        esac
        if milvus_sanitizer_env "${test_root}/plain"; then
            echo "Expected ${conflict} conflict to fail" >&2
            exit 1
        fi
    )
done

# Installed Archer metadata must activate OMPT and fail if the plugin is missing.
(
    unset USE_ASAN GOFLAGS LD_PRELOAD
    export USE_TSAN=ON OMP_TOOL=disabled OMP_TOOL_LIBRARIES=/wrong/tool.so
    printf 'llvm-20\n' > "${test_root}/tsan/lib/milvus-archer"
    if milvus_sanitizer_env "${test_root}/tsan"; then
        echo 'Expected missing Archer libraries to fail' >&2
        exit 1
    fi
    touch "${test_root}/tsan/lib/libarcher.so" "${test_root}/tsan/lib/libomp.so.5"
    milvus_sanitizer_env "${test_root}/tsan"
    [[ "${OMP_TOOL}" == enabled ]]
    [[ "${OMP_TOOL_LIBRARIES}" == "${test_root}/tsan/lib/libarcher.so" ]]
    [[ "${TSAN_OPTIONS}" == *ignore_noninstrumented_modules=1* ]]
)

# Verify configure-time guards without compiling C/C++ or resolving dependencies.
cmake_module="${scripts_dir}/../internal/core/cmake/Sanitizers.cmake"
printf 'include("%s")\n' "${cmake_module}" > "${test_root}/check.cmake"
for guard in conflict dependency-asan platform gpu dependencies cache; do
    args=(-DUSE_ASAN=OFF -DUSE_TSAN=ON -DCMAKE_SYSTEM_NAME=Linux -DCMAKE_SIZEOF_VOID_P=8)
    case "${guard}" in
        conflict) args+=(-DUSE_ASAN=ON); expected='mutually exclusive' ;;
        dependency-asan) args+=(-DWITH_ASAN=ON); expected='mutually exclusive' ;;
        platform) args+=(-DCMAKE_SYSTEM_NAME=Darwin); expected='64-bit Linux only' ;;
        gpu) args+=(-DMILVUS_GPU_VERSION=ON); expected='GPU builds' ;;
        dependencies) args+=(-DUSE_TSAN=OFF -DMILVUS_CONAN_SANITIZER=thread); expected='require USE_TSAN=ON' ;;
        cache) args+=(-DUSE_TSAN=OFF -DMILVUS_CONFIGURED_SANITIZER=thread); expected='fresh build' ;;
    esac
    if cmake "${args[@]}" -P "${test_root}/check.cmake" > "${test_root}/guard.log" 2>&1; then
        echo "Expected ${guard} configuration to fail" >&2
        exit 1
    fi
    grep -q "${expected}" "${test_root}/guard.log"
done

# Reject invalid shell options before the script creates a build directory.
for args in '-T invalid' '-T ON -a ON' '-T ON -g'; do
    if bash "${scripts_dir}/core_build.sh" ${args} > "${test_root}/script.log" 2>&1; then
        echo "Expected core_build.sh ${args} to fail" >&2
        exit 1
    fi
    grep -q 'ERROR:' "${test_root}/script.log"
done

# Exercise the parser's Go and handwritten C++ commands without compiling. This
# also protects the non-TSan path on systems whose /bin/bash is still Bash 3.2.
mkdir -p "${test_root}/repo/scripts" "${test_root}/bin" \
    "${test_root}/repo/internal/parser/planparserv2/cwrapper"
cp "${scripts_dir}/build_plan_parser.sh" "${test_root}/repo/scripts/"
printf 'set +e\n' > "${test_root}/repo/scripts/setenv.sh"
touch "${test_root}/repo/internal/parser/planparserv2/cwrapper/milvus_plan_parser.h"
cat > "${test_root}/bin/go" <<'EOF'
#!/usr/bin/env bash
[[ "$1" == env ]] && exit 0
printf 'CGO_CFLAGS=%s\nCGO_CXXFLAGS=%s\nCGO_LDFLAGS=%s\n' \
    "${CGO_CFLAGS:-}" "${CGO_CXXFLAGS:-}" "${CGO_LDFLAGS:-}" >> "${TEST_LOG}"
while [[ $# -gt 0 ]]; do
    if [[ "$1" == -o ]]; then
        touch "$2" "${2%.*}.h"
        exit 0
    fi
    shift
done
exit 1
EOF
cat > "${test_root}/bin/c++" <<'EOF'
#!/usr/bin/env bash
printf 'CXX=%s\n' "$*" >> "${TEST_LOG}"
while [[ $# -gt 0 ]]; do
    if [[ "$1" == -o ]]; then
        touch "$2"
        exit 0
    fi
    shift
done
exit 1
EOF
printf '#!/usr/bin/env bash\necho Linux\n' > "${test_root}/bin/uname"
chmod +x "${test_root}/bin/"*
for mode in OFF ON; do
    log="${test_root}/parser-${mode}.log"
    env -u LD_PRELOAD -u CGO_CFLAGS -u CGO_CXXFLAGS -u CGO_LDFLAGS \
        USE_ASAN=OFF USE_TSAN="${mode}" GOFLAGS= \
        PATH="${test_root}/bin:${PATH}" CXX="${test_root}/bin/c++" TEST_LOG="${log}" \
        bash "${test_root}/repo/scripts/build_plan_parser.sh"
    if [[ "${mode}" == ON ]]; then
        for stage in CGO_CFLAGS CGO_CXXFLAGS CGO_LDFLAGS CXX; do
            grep -q "^${stage}=.*-fsanitize=thread" "${log}"
        done
    elif grep -q -- '-fsanitize=thread' "${log}"; then
        echo 'The ordinary parser build unexpectedly enables TSan' >&2
        exit 1
    fi
done

# Image packaging must honor installed metadata even when USE_TSAN is unset.
mkdir -p "${test_root}/repo/build" "${test_root}/repo/lib"
cp "${scripts_dir}/../build/build_image.sh" "${test_root}/repo/build/"
cp "${scripts_dir}/sanitizer_env.sh" "${test_root}/repo/scripts/"
cat > "${test_root}/bin/docker" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >> "${TEST_LOG}"
[[ "$1" != inspect ]] || printf '1024\n'
EOF
chmod +x "${test_root}/bin/docker"
for mode in none thread; do
    printf '%s\n' "${mode}" > "${test_root}/repo/lib/milvus-sanitizer"
    log="${test_root}/image-${mode}.log"
    env -u USE_TSAN -u USE_ASAN -u LD_PRELOAD GOFLAGS= \
        IMAGE_ARCH=amd64 BUILD_ARGS='--build-arg TARGETARCH=amd64' \
        PATH="${test_root}/bin:${PATH}" TEST_LOG="${log}" \
        bash "${test_root}/repo/build/build_image.sh" > "${test_root}/image-output.log" 2>&1
    if [[ "${mode}" == thread ]]; then
        grep -q -- '--build-arg MILVUS_JEMALLOC_LIB=' "${log}"
    elif grep -q -- 'MILVUS_JEMALLOC_LIB=' "${log}"; then
        echo 'The ordinary image unexpectedly disables jemalloc' >&2
        exit 1
    fi
done

echo 'Sanitizer environment, parser commands and configuration guards passed'

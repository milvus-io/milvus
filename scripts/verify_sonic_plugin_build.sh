#!/usr/bin/env bash

set -euo pipefail

readonly REQUIRED_LDFLAG="-checklinkname=0"
readonly -a REQUIRED_TAGS=(dynamic sonic with_jemalloc bytedance_tango)
readonly -a SONIC_MODULES=(
    github.com/bytedance/sonic
    github.com/bytedance/sonic/loader
)

usage() {
    echo "Usage: $0 <milvus-binary> <go-plugin.so> [go-plugin.so ...]" >&2
}

if [[ $# -lt 2 ]]; then
    usage
    exit 2
fi

host=$1
shift

declare -A build_info

read_build_info() {
    local artifact=$1

    if [[ ! -f ${artifact} ]]; then
        echo "missing artifact: ${artifact}" >&2
        return 1
    fi

    local info
    if ! info=$(go version -m "${artifact}" 2>&1); then
        echo "cannot read Go build info from ${artifact}: ${info}" >&2
        return 1
    fi
    build_info["${artifact}"]=${info}
}

build_setting() {
    local artifact=$1
    local key=$2

    printf '%s\n' "${build_info[${artifact}]}" |
        awk -F '\t' -v wanted="${key}" '$2 == "build" && index($3, wanted "=") == 1 {sub(wanted "=", "", $3); print $3; exit}'
}

dependency_version() {
    local artifact=$1
    local module=$2

    printf '%s\n' "${build_info[${artifact}]}" |
        awk -F '\t' -v wanted="${module}" '$2 == "dep" && $3 == wanted {print $4; exit}'
}

go_version() {
    local artifact=$1
    printf '%s\n' "${build_info[${artifact}]}" | awk 'NR == 1 {print $2}'
}

has_csv_value() {
    local csv=$1
    local wanted=$2
    local item
    local -a items

    IFS=',' read -r -a items <<< "${csv}"
    for item in "${items[@]}"; do
        if [[ ${item} == "${wanted}" ]]; then
            return 0
        fi
    done
    return 1
}

validate_common_settings() {
    local artifact=$1
    local tags
    local ldflags
    local required_tag

    tags=$(build_setting "${artifact}" "-tags")
    for required_tag in "${REQUIRED_TAGS[@]}"; do
        if ! has_csv_value "${tags}" "${required_tag}"; then
            echo "${artifact}: missing build tag ${required_tag}; tags=${tags:-<empty>}" >&2
            return 1
        fi
    done

    ldflags=$(build_setting "${artifact}" "-ldflags")
    if [[ ${ldflags} != *"${REQUIRED_LDFLAG}"* ]]; then
        echo "${artifact}: missing linker flag ${REQUIRED_LDFLAG}; ldflags=${ldflags:-<empty>}" >&2
        return 1
    fi
}

read_build_info "${host}"
for plugin in "$@"; do
    read_build_info "${plugin}"
done

if [[ $(build_setting "${host}" "-buildmode") != "exe" ]]; then
    echo "${host}: expected -buildmode=exe" >&2
    exit 1
fi
validate_common_settings "${host}"

host_go_version=$(go_version "${host}")
host_goos=$(build_setting "${host}" "GOOS")
host_goarch=$(build_setting "${host}" "GOARCH")
host_goamd64=$(build_setting "${host}" "GOAMD64")
readonly host_go_version host_goos host_goarch host_goamd64

declare module host_module_version plugin_module_version

for plugin in "$@"; do
    if [[ $(build_setting "${plugin}" "-buildmode") != "plugin" ]]; then
        echo "${plugin}: expected -buildmode=plugin" >&2
        exit 1
    fi
    validate_common_settings "${plugin}"

    if [[ $(go_version "${plugin}") != "${host_go_version}" ]]; then
        echo "${plugin}: Go version differs from host ${host_go_version}" >&2
        exit 1
    fi
    if [[ $(build_setting "${plugin}" "GOOS") != "${host_goos}" ]]; then
        echo "${plugin}: GOOS differs from host ${host_goos}" >&2
        exit 1
    fi
    if [[ $(build_setting "${plugin}" "GOARCH") != "${host_goarch}" ]]; then
        echo "${plugin}: GOARCH differs from host ${host_goarch}" >&2
        exit 1
    fi
    if [[ $(build_setting "${plugin}" "GOAMD64") != "${host_goamd64}" ]]; then
        echo "${plugin}: GOAMD64 differs from host ${host_goamd64}" >&2
        exit 1
    fi

    for module in "${SONIC_MODULES[@]}"; do
        host_module_version=$(dependency_version "${host}" "${module}")
        plugin_module_version=$(dependency_version "${plugin}" "${module}")
        if [[ -n ${plugin_module_version} && ${plugin_module_version} != "${host_module_version}" ]]; then
            echo "${plugin}: ${module}=${plugin_module_version} differs from host ${host_module_version:-<missing>}" >&2
            exit 1
        fi
    done
done

echo "Sonic/Go-plugin synchronization build settings are consistent."

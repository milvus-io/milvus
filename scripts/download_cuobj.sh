#!/usr/bin/env bash

set -euo pipefail

if [[ ${1:-} == "-h" || ${1:-} == "--help" ]]; then
  echo "Usage: $0 [DESTINATION]"
  echo "Extract CUDA 13.4 headers and libraries (development and runtime) without installing packages."
  echo "Default destination: internal/core/thirdparty/cuobj (relative to the repository)."
  echo "Set CUDA_REPO_URL to override the NVIDIA Ubuntu 22.04 repository URL."
  echo "System libraries (libibverbs, librdmacm, libmlx5, libnuma) must be installed separately."
  exit 0
fi

if [[ $# -gt 1 ]]; then
  echo "Usage: $0 [DESTINATION]" >&2
  exit 1
fi

for tool in curl gzip awk dpkg dpkg-deb sha256sum; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "Required tool not found: $tool" >&2
    exit 1
  fi
done

case "$(uname -m)" in
  x86_64) repo_arch=x86_64; target_arch=x86_64-linux ;;
  aarch64|arm64) repo_arch=sbsa; target_arch=sbsa-linux ;;
  *) echo "Unsupported architecture: $(uname -m)" >&2; exit 1 ;;
esac

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
destination="${1:-$root_dir/internal/core/thirdparty/cuobj}"
repo_url="${CUDA_REPO_URL:-https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2204/$repo_arch}"
repo_url="${repo_url%/}"
work_dir="$(mktemp -d)"
trap 'rm -rf "$work_dir"' EXIT

curl --fail --location --retry 3 --connect-timeout 30 \
  "$repo_url/Packages.gz" -o "$work_dir/Packages.gz"
gzip -dc "$work_dir/Packages.gz" > "$work_dir/Packages"

# The -dev packages only carry headers and unversioned .so links; the runtime
# packages carry the versioned libraries those links point to.
packages=(
  libcuobjclient-13-4 libcuobjclient-dev-13-4
  libcufile-13-4 libcufile-dev-13-4
  cuda-cudart-13-4 cuda-cudart-dev-13-4
)
for package in "${packages[@]}"; do
  awk -v package="$package" '
    BEGIN { RS = ""; FS = "\n" }
    {
      name = version = filename = checksum = ""
      for (field = 1; field <= NF; field++) {
        if ($field ~ /^Package: /) name = substr($field, 10)
        if ($field ~ /^Version: /) version = substr($field, 10)
        if ($field ~ /^Filename: /) filename = substr($field, 11)
        if ($field ~ /^SHA256: /) checksum = substr($field, 9)
      }
      if (name == package) print version, filename, checksum
    }
  ' "$work_dir/Packages" > "$work_dir/candidates"

  selected_version=""
  selected_filename=""
  selected_checksum=""
  while read -r version filename checksum; do
    if [[ -z "$selected_version" ]] || dpkg --compare-versions "$version" gt "$selected_version"; then
      selected_version="$version"
      selected_filename="$filename"
      selected_checksum="$checksum"
    fi
  done < "$work_dir/candidates"

  if [[ -z "$selected_filename" || ! "$selected_checksum" =~ ^[[:xdigit:]]{64}$ ]]; then
    echo "Package missing or SHA256 unavailable: $package in $repo_url" >&2
    exit 1
  fi

  echo "Downloading $package ($selected_version)"
  archive="$work_dir/$package.deb"
  curl --fail --location --retry 3 --connect-timeout 30 \
    "$repo_url/${selected_filename#./}" -o "$archive"
  printf '%s  %s\n' "$selected_checksum" "$archive" | sha256sum --check --status
  dpkg-deb --extract "$archive" "$work_dir/extracted"
done

payload="$work_dir/extracted/usr/local/cuda-13.4/targets/$target_arch"
if [[ ! -d "$payload/include" || ! -d "$payload/lib" ]]; then
  echo "Expected CUDA include/lib directories missing: $payload" >&2
  exit 1
fi

mkdir -p "$destination/include" "$destination/lib"
cp -a "$payload/include/." "$destination/include/"
cp -a "$payload/lib/." "$destination/lib/"
echo "Extracted headers and libraries to $destination"

dangling=0
while IFS= read -r -d '' library; do
  if [[ ! -e "$library" ]]; then
    echo "Unresolved library link: $library -> $(readlink "$library")" >&2
    dangling=1
  fi
done < <(find "$destination/lib" -type l -print0)
if [[ $dangling -ne 0 ]]; then
  echo "Extraction is missing runtime libraries required by the links above." >&2
  exit 1
fi
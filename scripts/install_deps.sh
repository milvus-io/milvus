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

# Milvus Development Dependencies Installation Script
#
# Supported platforms:
#   - macOS 12, 13, 14, 15 (Intel and Apple Silicon)
#   - Ubuntu 20.04, 22.04, 24.04
#   - Rocky Linux 8, 9
#   - Amazon Linux 2023
#
# Compiler requirements:
#   - macOS: LLVM/Clang 14-18
#   - Linux: GCC 9-14
#
# Usage:
#   ./scripts/install_deps.sh
#
# After installation, build with:
#   make

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Minimum version requirements
MIN_CMAKE_VERSION="3.26"
MIN_GO_VERSION="1.21"
CONAN_VERSION="1.64.1"
RUST_VERSION="1.89"

#######################################
# Check if a command exists
#######################################
command_exists() {
    command -v "$1" &> /dev/null
}

#######################################
# Compare version strings
# Returns 0 if $1 >= $2
#######################################
version_ge() {
    [ "$(printf '%s\n' "$2" "$1" | sort -V | head -n1)" = "$2" ]
}

#######################################
# Install Rust
#######################################
install_rust() {
    if command_exists cargo; then
        print_info "Rust already installed, ensuring version ${RUST_VERSION}..."
        rustup install ${RUST_VERSION}
        rustup default ${RUST_VERSION}
    else
        print_info "Installing Rust ${RUST_VERSION}..."
        curl https://sh.rustup.rs -sSf | sh -s -- --default-toolchain=${RUST_VERSION} -y || {
            print_error "Rust installation failed"
            exit 1
        }
        source "$HOME/.cargo/env"
    fi
}

#######################################
# Install CMake if version is too old
#######################################
install_cmake_linux() {
    local current_version=""
    if command_exists cmake; then
        current_version=$(cmake --version | head -1 | grep -oE '[0-9]+\.[0-9]+')
    fi

    if [ -z "$current_version" ] || ! version_ge "$current_version" "$MIN_CMAKE_VERSION"; then
        print_info "Installing CMake >= ${MIN_CMAKE_VERSION}..."
        local arch=$(uname -m)
        wget -qO- "https://cmake.org/files/v3.26/cmake-3.26.5-linux-${arch}.tar.gz" | \
            sudo tar --strip-components=1 -xz -C /usr/local
    else
        print_info "CMake ${current_version} already installed"
    fi
}

#######################################
# Install Conan package manager
#######################################
install_conan() {
    print_info "Installing Conan ${CONAN_VERSION}..."
    if command_exists pip3; then
        pip3 install --user conan==${CONAN_VERSION}
    elif command_exists pip; then
        pip install --user conan==${CONAN_VERSION}
    else
        print_error "pip not found. Please install Python 3 with pip."
        exit 1
    fi

    # Add local bin to PATH if not already there
    if [[ ":$PATH:" != *":$HOME/.local/bin:"* ]]; then
        export PATH="$HOME/.local/bin:$PATH"
        print_info "Added ~/.local/bin to PATH"
    fi
}

#######################################
# macOS: Detect best available LLVM version
#######################################
detect_llvm_version() {
    # Try versions from newest to oldest
    for version in 17 16 15 14; do
        if brew list llvm@${version} &>/dev/null; then
            echo $version
            return 0
        fi
    done
    echo ""
}

#######################################
# macOS: Install dependencies
#######################################
install_mac_deps() {
    print_info "Installing macOS dependencies..."

    # Check for Homebrew
    if ! command_exists brew; then
        print_error "Homebrew not found. Please install from https://brew.sh"
        exit 1
    fi

    # Install Xcode command line tools if needed
    if ! xcode-select -p &>/dev/null; then
        print_info "Installing Xcode command line tools..."
        xcode-select --install
        print_warn "Please complete Xcode tools installation and re-run this script"
        exit 0
    fi

    # Detect architecture
    local arch=$(uname -m)
    print_info "Detected architecture: ${arch}"

    # Detect or install LLVM
    local llvm_version=$(detect_llvm_version)
    if [ -z "$llvm_version" ]; then
        # Install LLVM 17 as default (good balance of features and stability)
        print_info "Installing LLVM 17..."
        brew install llvm@17
        llvm_version=17
    else
        print_info "Using existing LLVM ${llvm_version}"
    fi

    # Core build dependencies
    print_info "Installing core dependencies..."
    brew install --quiet \
        libomp \
        cmake \
        ninja \
        ccache \
        pkg-config \
        zip \
        unzip \
        grep

    # Architecture-specific dependencies
    if [[ "$arch" == "arm64" ]]; then
        print_info "Installing Apple Silicon specific dependencies..."
        brew install --quiet openssl librdkafka
        # AWS SDK for S3 support on macOS (needed for milvus-storage)
        brew install --quiet aws-sdk-cpp
    else
        # Intel Mac
        brew install --quiet aws-sdk-cpp
    fi

    # Create symlink for LLVM (for scripts that expect /usr/local/opt/llvm)
    # Note: setenv.sh detects LLVM version directly, so symlink is optional
    local llvm_prefix=$(brew --prefix llvm@${llvm_version})
    local target_link="/usr/local/opt/llvm"
    if [[ "$arch" == "arm64" ]]; then
        target_link="/opt/homebrew/opt/llvm"
    fi

    if [ ! -L "$target_link" ] || [ "$(readlink "$target_link")" != "$llvm_prefix" ]; then
        print_info "Creating LLVM symlink (may require sudo)..."
        if sudo -n true 2>/dev/null; then
            sudo rm -f "$target_link" 2>/dev/null || true
            sudo ln -sf "$llvm_prefix" "$target_link"
        else
            print_warn "Skipping LLVM symlink creation (no sudo access). Build will still work."
        fi
    fi

    # Install Conan
    install_conan

    # Install Rust
    install_rust

    print_info "macOS dependencies installed successfully!"
    print_info "LLVM version: ${llvm_version}"
    print_info ""
    print_info "To build Milvus, run: make"
}

#######################################
# Ubuntu: Detect version
#######################################
detect_ubuntu_version() {
    if [ -f /etc/os-release ]; then
        . /etc/os-release
        echo "$VERSION_ID"
    else
        echo ""
    fi
}

#######################################
# Ubuntu: Install dependencies
#######################################
install_ubuntu_deps() {
    local ubuntu_version=$(detect_ubuntu_version)
    print_info "Detected Ubuntu ${ubuntu_version}"

    # Update package lists
    sudo apt-get update

    # Base packages for all Ubuntu versions
    local base_packages=(
        wget curl ca-certificates gnupg2
        git make ninja-build ccache
        libssl-dev zlib1g-dev zip unzip
        lcov libtool m4 autoconf automake
        python3 python3-pip python3-venv
        pkg-config uuid-dev libaio-dev
        libopenblas-dev libgoogle-perftools-dev
    )

    # Version-specific GCC and clang-format
    local gcc_packages=()
    local clang_format_package=""

    case "$ubuntu_version" in
        20.04)
            gcc_packages=(g++ gcc gfortran)
            clang_format_package="clang-format-12 clang-tidy-12"
            ;;
        22.04)
            gcc_packages=(g++ gcc gfortran g++-11 gcc-11)
            clang_format_package="clang-format-14 clang-tidy-14"
            ;;
        24.04)
            gcc_packages=(g++ gcc gfortran g++-13 gcc-13)
            clang_format_package="clang-format-17 clang-tidy-17"
            ;;
        *)
            print_warn "Ubuntu ${ubuntu_version} not explicitly supported, using default packages"
            gcc_packages=(g++ gcc gfortran)
            clang_format_package="clang-format clang-tidy"
            ;;
    esac

    print_info "Installing packages..."
    sudo apt-get install -y "${base_packages[@]}" "${gcc_packages[@]}" $clang_format_package

    # Install CMake if needed
    install_cmake_linux

    # Install Conan
    install_conan

    # Install Rust
    install_rust

    print_info "Ubuntu dependencies installed successfully!"
    print_info ""
    print_info "To build Milvus, run: make"
}

#######################################
# Rocky Linux: Detect version
#######################################
detect_rocky_version() {
    if [ -f /etc/os-release ]; then
        . /etc/os-release
        echo "${VERSION_ID%%.*}"
    else
        echo ""
    fi
}

#######################################
# Rocky Linux: Install dependencies
#######################################
install_rocky_deps() {
    local rocky_version=$(detect_rocky_version)
    print_info "Detected Rocky Linux ${rocky_version}"

    # Enable EPEL repository
    sudo dnf install -y epel-release

    # Base packages
    local base_packages=(
        wget curl which git make ninja-build
        automake python3-devel python3-pip
        openblas-devel libaio libuuid-devel
        zip unzip ccache lcov libtool m4 autoconf
    )

    # Version-specific packages
    case "$rocky_version" in
        8)
            print_info "Installing GCC toolset for Rocky 8..."
            sudo dnf install -y gcc-toolset-12 gcc-toolset-12-libatomic-devel gcc-toolset-12-libstdc++-devel
            sudo dnf install -y "${base_packages[@]}"
            sudo dnf install -y atlas-devel

            # Enable GCC toolset
            echo "source /opt/rh/gcc-toolset-12/enable" | sudo tee /etc/profile.d/gcc-toolset-12.sh
            source /opt/rh/gcc-toolset-12/enable

            # Create libstdc++.a symlink for static linking (needed by conan packages like ninja)
            local gcc12_libstdcxx="/opt/rh/gcc-toolset-12/root/usr/lib/gcc/x86_64-redhat-linux/12/libstdc++.a"
            local system_gcc_dir="/usr/lib/gcc/x86_64-redhat-linux/8"
            if [ -f "$gcc12_libstdcxx" ] && [ -d "$system_gcc_dir" ] && [ ! -f "$system_gcc_dir/libstdc++.a" ]; then
                print_info "Creating libstdc++.a symlink for static linking..."
                sudo ln -s "$gcc12_libstdcxx" "$system_gcc_dir/libstdc++.a"
            fi

            # Install LLVM toolset for clang-format
            sudo dnf install -y llvm-toolset
            echo "export CLANG_TOOLS_PATH=/usr/bin" | sudo tee -a /etc/profile.d/llvm-toolset.sh
            ;;
        9)
            print_info "Installing GCC for Rocky 9..."
            sudo dnf install -y gcc gcc-c++ gcc-gfortran
            sudo dnf install -y "${base_packages[@]}"

            # Optionally install newer GCC toolset
            sudo dnf install -y gcc-toolset-13 || true
            if [ -d /opt/rh/gcc-toolset-13 ]; then
                echo "source /opt/rh/gcc-toolset-13/enable" | sudo tee /etc/profile.d/gcc-toolset-13.sh
            fi

            # Install clang-tools
            sudo dnf install -y clang-tools-extra || sudo dnf install -y clang
            ;;
        *)
            print_warn "Rocky Linux ${rocky_version} not explicitly supported"
            sudo dnf install -y gcc gcc-c++ gcc-gfortran "${base_packages[@]}"
            ;;
    esac

    # Install CMake if needed
    install_cmake_linux

    # Install Conan
    install_conan

    # Configure Conan profile to match the active GCC toolset
    local gcc_ver
    gcc_ver=$(gcc -dumpversion 2>/dev/null | cut -d. -f1)
    if [ -n "$gcc_ver" ]; then
        print_info "Configuring Conan profile for GCC ${gcc_ver}..."
        conan profile new default --detect --force 2>/dev/null || true
        conan profile update settings.compiler.version="$gcc_ver" default
        conan profile update settings.compiler.libcxx=libstdc++11 default
    fi

    # Install Rust
    install_rust

    print_info "Rocky Linux dependencies installed successfully!"
    print_info ""
    print_info "To build Milvus, run: make"
}

#######################################
# Amazon Linux: Install dependencies
#######################################
install_amazon_linux_deps() {
    print_info "Detected Amazon Linux"

    # Install base packages
    sudo dnf install -y \
        wget curl which git make ninja-build \
        gcc gcc-c++ gcc-gfortran \
        automake python3-devel python3-pip \
        openblas-devel libaio libuuid-devel \
        zip unzip ccache libtool m4 autoconf \
        openssl-devel zlib-devel

    # Install CMake if needed
    install_cmake_linux

    # Install Conan
    install_conan

    # Install Rust
    install_rust

    print_info "Amazon Linux dependencies installed successfully!"
    print_info ""
    print_info "To build Milvus, run: make"
}

#######################################
# CentOS: Install dependencies (legacy support)
#######################################
install_centos_deps() {
    print_info "Detected CentOS (legacy support)"
    print_warn "CentOS is EOL. Consider migrating to Rocky Linux."

    # Try to use similar approach to Rocky
    sudo yum install -y epel-release centos-release-scl-rh || true

    sudo yum install -y \
        wget curl which git make \
        automake python3-devel python3-pip \
        devtoolset-11-gcc devtoolset-11-gcc-c++ devtoolset-11-gcc-gfortran devtoolset-11-libatomic-devel \
        llvm-toolset-11.0-clang llvm-toolset-11.0-clang-tools-extra \
        openblas-devel libaio libuuid-devel \
        zip unzip ccache lcov libtool m4 autoconf automake

    # Enable devtoolset
    echo "source scl_source enable devtoolset-11" | sudo tee /etc/profile.d/devtoolset-11.sh
    echo "source scl_source enable llvm-toolset-11.0" | sudo tee /etc/profile.d/llvm-toolset-11.sh
    echo "export CLANG_TOOLS_PATH=/opt/rh/llvm-toolset-11.0/root/usr/bin" | sudo tee -a /etc/profile.d/llvm-toolset-11.sh
    source /etc/profile.d/devtoolset-11.sh
    source /etc/profile.d/llvm-toolset-11.sh

    # Install CMake if needed
    install_cmake_linux

    # Install Conan
    install_conan

    # Install Rust
    install_rust

    print_info "CentOS dependencies installed successfully!"
}

#######################################
# Detect Linux distribution
#######################################
detect_linux_distro() {
    if [ -f /etc/os-release ]; then
        . /etc/os-release
        echo "$ID"
    elif [ -f /etc/centos-release ]; then
        echo "centos"
    else
        echo "unknown"
    fi
}

#######################################
# Main: Install dependencies based on OS
#######################################
main() {
    print_info "Milvus Development Dependencies Installer"
    print_info "=========================================="

    # Check for Go first
    if ! command_exists go; then
        print_error "Go not found. Please install Go >= ${MIN_GO_VERSION} first."
        print_info "Download from: https://go.dev/dl/"
        exit 1
    fi

    # Check Go version
    local go_version=$(go version | grep -oE 'go[0-9]+\.[0-9]+' | sed 's/go//')
    if ! version_ge "$go_version" "$MIN_GO_VERSION"; then
        print_error "Go version ${go_version} is too old. Please install Go >= ${MIN_GO_VERSION}"
        exit 1
    fi
    print_info "Go version: ${go_version}"

    # Detect OS and install dependencies
    local os_type=$(uname -s)
    case "$os_type" in
        Darwin)
            install_mac_deps
            ;;
        Linux)
            local distro=$(detect_linux_distro)
            case "$distro" in
                ubuntu|debian)
                    install_ubuntu_deps
                    ;;
                rocky|almalinux)
                    install_rocky_deps
                    ;;
                amzn)
                    install_amazon_linux_deps
                    ;;
                centos|rhel)
                    install_centos_deps
                    ;;
                *)
                    print_error "Unsupported Linux distribution: ${distro}"
                    print_info "Supported distributions: Ubuntu, Rocky Linux, Amazon Linux, CentOS"
                    exit 1
                    ;;
            esac
            ;;
        *)
            print_error "Unsupported operating system: ${os_type}"
            exit 1
            ;;
    esac
}

main "$@"

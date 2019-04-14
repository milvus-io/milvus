# OpenBLAS

[![Join the chat at https://gitter.im/xianyi/OpenBLAS](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/xianyi/OpenBLAS?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Travis CI: [![Build Status](https://travis-ci.org/xianyi/OpenBLAS.svg?branch=develop)](https://travis-ci.org/xianyi/OpenBLAS)

AppVeyor: [![Build status](https://ci.appveyor.com/api/projects/status/09sohd35n8nkkx64/branch/develop?svg=true)](https://ci.appveyor.com/project/xianyi/openblas/branch/develop)

## Introduction

OpenBLAS is an optimized BLAS library based on GotoBLAS2 1.13 BSD version.

Please read the documentation on the OpenBLAS wiki pages: <http://github.com/xianyi/OpenBLAS/wiki>.

## Binary Packages

We provide official binary packages for the following platform:

  * Windows x86/x86_64

You can download them from [file hosting on sourceforge.net](https://sourceforge.net/projects/openblas/files/).

## Installation from Source

Download from project homepage, http://xianyi.github.com/OpenBLAS/, or check out the code
using Git from https://github.com/xianyi/OpenBLAS.git.

### Dependencies

Building OpenBLAS requires the following to be installed:

* GNU Make
* A C compiler, e.g. GCC or Clang
* A Fortran compiler (optional, for LAPACK)
* IBM MASS (optional, see below)

### Normal compile

Simply invoking `make` (or `gmake` on BSD) will detect the CPU automatically.
To set a specific target CPU, use `make TARGET=xxx`, e.g. `make TARGET=NEHALEM`.
The full target list is in the file `TargetList.txt`.

### Cross compile

Set `CC` and `FC` to point to the cross toolchains, and set `HOSTCC` to your host C compiler.
The target must be specified explicitly when cross compiling.

Examples:

* On an x86 box, compile this library for a loongson3a CPU:
  ```sh
  make BINARY=64 CC=mips64el-unknown-linux-gnu-gcc FC=mips64el-unknown-linux-gnu-gfortran HOSTCC=gcc TARGET=LOONGSON3A
  ```

* On an x86 box, compile this library for a loongson3a CPU with loongcc (based on Open64) compiler:
  ```sh
  make CC=loongcc FC=loongf95 HOSTCC=gcc TARGET=LOONGSON3A CROSS=1 CROSS_SUFFIX=mips64el-st-linux-gnu-   NO_LAPACKE=1 NO_SHARED=1 BINARY=32
  ```

### Debug version

A debug version can be built using `make DEBUG=1`.

### Compile with MASS support on Power CPU (optional)

The [IBM MASS](http://www-01.ibm.com/software/awdtools/mass/linux/mass-linux.html) library
consists of a set of mathematical functions for C, C++, and Fortran applications that are
are tuned for optimum performance on POWER architectures.
OpenBLAS with MASS requires a 64-bit, little-endian OS on POWER.
The library can be installed as shown:

* On Ubuntu:
  ```sh
  wget -q http://public.dhe.ibm.com/software/server/POWER/Linux/xl-compiler/eval/ppc64le/ubuntu/public.gpg -O- | sudo apt-key add -
  echo "deb http://public.dhe.ibm.com/software/server/POWER/Linux/xl-compiler/eval/ppc64le/ubuntu/ trusty main" | sudo tee /etc/apt/sources.list.d/ibm-xl-compiler-eval.list
  sudo apt-get update
  sudo apt-get install libxlmass-devel.8.1.5
  ```

* On RHEL/CentOS:
  ```sh
  wget http://public.dhe.ibm.com/software/server/POWER/Linux/xl-compiler/eval/ppc64le/rhel7/repodata/repomd.xml.key
  sudo rpm --import repomd.xml.key
  wget http://public.dhe.ibm.com/software/server/POWER/Linux/xl-compiler/eval/ppc64le/rhel7/ibm-xl-compiler-eval.repo
  sudo cp ibm-xl-compiler-eval.repo /etc/yum.repos.d/
  sudo yum install libxlmass-devel.8.1.5
  ```

After installing the MASS library, compile OpenBLAS with `USE_MASS=1`.
For example, to compile on Power8 with MASS support: `make USE_MASS=1 TARGET=POWER8`.

### Install to a specific directory (optional)

Use `PREFIX=` when invoking `make`, for example

```sh
make install PREFIX=your_installation_directory
```

The default installation directory is `/opt/OpenBLAS`.

## Supported CPUs and Operating Systems

Please read `GotoBLAS_01Readme.txt`.

### Additional supported CPUs

#### x86/x86-64

- **Intel Xeon 56xx (Westmere)**: Used GotoBLAS2 Nehalem codes.
- **Intel Sandy Bridge**: Optimized Level-3 and Level-2 BLAS with AVX on x86-64.
- **Intel Haswell**: Optimized Level-3 and Level-2 BLAS with AVX2 and FMA  on x86-64.
- **Intel Skylake**: Optimized Level-3 and Level-2 BLAS with AVX512 and FMA  on x86-64.
- **AMD Bobcat**: Used GotoBLAS2 Barcelona codes.
- **AMD Bulldozer**: x86-64 ?GEMM FMA4 kernels. (Thanks to Werner Saar)
- **AMD PILEDRIVER**: Uses Bulldozer codes with some optimizations.
- **AMD STEAMROLLER**: Uses Bulldozer codes with some optimizations.

#### MIPS64

- **ICT Loongson 3A**: Optimized Level-3 BLAS and the part of Level-1,2.
- **ICT Loongson 3B**: Experimental

#### ARM

- **ARMv6**: Optimized BLAS for vfpv2 and vfpv3-d16 (e.g. BCM2835, Cortex M0+)
- **ARMv7**: Optimized BLAS for vfpv3-d32 (e.g. Cortex A8, A9 and A15)

#### ARM64

- **ARMv8**: Experimental
- **ARM Cortex-A57**: Experimental

#### PPC/PPC64

- **POWER8**: Optmized Level-3 BLAS and some Level-1, only with `USE_OPENMP=1`

#### IBM zEnterprise System

- **Z13**: Optimized Level-3 BLAS and Level-1,2 (double precision)

### Supported OS

- **GNU/Linux**
- **MinGW or Visual Studio (CMake)/Windows**: Please read <https://github.com/xianyi/OpenBLAS/wiki/How-to-use-OpenBLAS-in-Microsoft-Visual-Studio>.
- **Darwin/macOS**: Experimental. Although GotoBLAS2 supports Darwin, we are not macOS experts.
- **FreeBSD**: Supported by the community. We don't actively test the library on this OS.
- **OpenBSD**: Supported by the community. We don't actively test the library on this OS.
- **DragonFly BSD**: Supported by the community. We don't actively test the library on this OS.
- **Android**: Supported by the community. Please read <https://github.com/xianyi/OpenBLAS/wiki/How-to-build-OpenBLAS-for-Android>.

## Usage

Statically link with `libopenblas.a` or dynamically link with `-lopenblas` if OpenBLAS was
compiled as a shared library.

### Setting the number of threads using environment variables

Environment variables are used to specify a maximum number of threads.
For example,

```sh
export OPENBLAS_NUM_THREADS=4
export GOTO_NUM_THREADS=4
export OMP_NUM_THREADS=4
```

The priorities are `OPENBLAS_NUM_THREADS` > `GOTO_NUM_THREADS` > `OMP_NUM_THREADS`.

If you compile this library with `USE_OPENMP=1`, you should set the `OMP_NUM_THREADS`
environment variable; OpenBLAS ignores `OPENBLAS_NUM_THREADS` and `GOTO_NUM_THREADS` when
compiled with `USE_OPENMP=1`.

### Setting the number of threads at runtime

We provide the following functions to control the number of threads at runtime:

```c
void goto_set_num_threads(int num_threads);
void openblas_set_num_threads(int num_threads);
```

If you compile this library with `USE_OPENMP=1`, you should use the above functions too.

## Reporting bugs

Please submit an issue in https://github.com/xianyi/OpenBLAS/issues.

## Contact

* OpenBLAS users mailing list: https://groups.google.com/forum/#!forum/openblas-users
* OpenBLAS developers mailing list: https://groups.google.com/forum/#!forum/openblas-dev

## Change log

Please see Changelog.txt to view the differences between OpenBLAS and GotoBLAS2 1.13 BSD version.

## Troubleshooting

* Please read the [FAQ](https://github.com/xianyi/OpenBLAS/wiki/Faq) first.
* Please use GCC version 4.6 and above to compile Sandy Bridge AVX kernels on Linux/MinGW/BSD.
* Please use Clang version 3.1 and above to compile the library on Sandy Bridge microarchitecture.
  Clang 3.0 will generate the wrong AVX binary code.
* Please use GCC version 6 or LLVM version 6 and above to compile Skylake AVX512 kernels.
* The number of CPUs/cores should less than or equal to 256. On Linux `x86_64` (`amd64`),
  there is experimental support for up to 1024 CPUs/cores and 128 numa nodes if you build
  the library with `BIGNUMA=1`.
* OpenBLAS does not set processor affinity by default.
  On Linux, you can enable processor affinity by commenting out the line `NO_AFFINITY=1` in
  Makefile.rule. However, note that this may cause
  [a conflict with R parallel](https://stat.ethz.ch/pipermail/r-sig-hpc/2012-April/001348.html).
* On Loongson 3A, `make test` may fail with a `pthread_create` error (`EAGAIN`).
  However, it will be okay when you run the same test case on the shell.

## Contributing

1. [Check for open issues](https://github.com/xianyi/OpenBLAS/issues) or open a fresh issue
   to start a discussion around a feature idea or a bug.
2. Fork the [OpenBLAS](https://github.com/xianyi/OpenBLAS) repository to start making your changes.
3. Write a test which shows that the bug was fixed or that the feature works as expected.
4. Send a pull request. Make sure to add yourself to `CONTRIBUTORS.md`.

## Donation

Please read [this wiki page](https://github.com/xianyi/OpenBLAS/wiki/Donation).

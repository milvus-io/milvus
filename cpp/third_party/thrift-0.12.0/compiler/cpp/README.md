# Build Thrift IDL compiler using CMake

<!-- TOC -->

- [Build Thrift IDL compiler using CMake](#build-thrift-idl-compiler-using-cmake)
    - [Build on Unix-like System](#build-on-unix-like-system)
        - [Prerequisites](#prerequisites)
        - [Build using CMake](#build-using-cmake)
            - [Build with Eclipse IDE](#build-with-eclipse-ide)
            - [Build with XCode IDE in MacOS](#build-with-xcode-ide-in-macos)
            - [Usage of other IDEs](#usage-of-other-ides)
    - [Build on Windows](#build-on-windows)
        - [Prerequisites](#prerequisites-1)
        - [Build using Git Bash](#build-using-git-bash)
        - [Using Visual Studio and Win flex-bison](#using-visual-studio-and-win-flex-bison)
        - [Cross compile using mingw32 and generate a Windows Installer with CPack](#cross-compile-using-mingw32-and-generate-a-windows-installer-with-cpack)
- [Other cases](#other-cases)
    - [Building the Thrift IDL compiler in Windows without CMake](#building-the-thrift-idl-compiler-in-windows-without-cmake)
- [Unit tests for compiler](#unit-tests-for-compiler)
    - [Using boost test](#using-boost-test)
    - [Using Catch C++ test library](#using-catch-c-test-library)
- [Have a Happy free time and holidays](#have-a-happy-free-time-and-holidays)

<!-- /TOC -->

## Build on Unix-like System

### Prerequisites
- Install CMake
- Install flex and bison

### Build using CMake

- Go to **thrift\compiler\cpp**
- Use the following steps to build using cmake:

```
mkdir cmake-build && cd cmake-build
cmake ..
make
```

#### Build with Eclipse IDE

- Go to **thrift\compiler\cpp**
- Use the following steps to build using cmake:

```
mkdir cmake-ec && cd cmake-ec
cmake -G "Eclipse CDT4 - Unix Makefiles" ..
make
```

Now open the folder cmake-ec using eclipse.

#### Build with XCode IDE in MacOS

- Install/update flex, bison and cmake with brew

```
brew install cmake
brew install bison
```

- Go to **thrift\compiler\cpp**
- Run commands in command line:

```
mkdir cmake-build && cd cmake-build
cmake -G "Xcode" -DWITH_PLUGIN=OFF ..
cmake --build .
```

#### Usage of other IDEs

Please check list of supported IDE 

```
cmake --help
```

## Build on Windows

### Prerequisites
- Install CMake - https://cmake.org/download/
- In case if you want to build without Git Bash - install winflexbison - https://sourceforge.net/projects/winflexbison/
- In case if you want to build with Visual Studio - install Visual Studio 
  - Better to use the latest stable Visual Studio Community Edition - https://www.visualstudio.com/vs/whatsnew/ (ensure that you installed workload "Desktop Development with C++" for VS2017) - Microsoft added some support for CMake and improving it in Visual Studio

### Build using Git Bash

Git Bash provides flex and bison

- Go to **thrift\compiler\cpp**
- Use the following steps to build using cmake:

```
mkdir cmake-vs && cd cmake-vs
cmake -DWITH_SHARED_LIB=off ..
cmake --build .
```

### Using Visual Studio and Win flex-bison

- Generate a Visual Studio project for version of Visual Studio which you have (**cmake --help** can show list of supportable VS versions):
- Run commands in command line:
```
mkdir cmake-vs
cd cmake-vs
cmake -G "Visual Studio 15 2017" -DWITH_PLUGIN=OFF ..
```
- Now open the folder cmake-vs using Visual Studio.

### Cross compile using mingw32 and generate a Windows Installer with CPack

```
mkdir cmake-mingw32 && cd cmake-mingw32
cmake -DCMAKE_TOOLCHAIN_FILE=../build/cmake/mingw32-toolchain.cmake -DBUILD_COMPILER=ON -DBUILD_LIBRARIES=OFF -DBUILD_TESTING=OFF -DBUILD_EXAMPLES=OFF ..
cpack
```

# Other cases

## Building the Thrift IDL compiler in Windows without CMake

If you don't want to use CMake you can use the already available Visual Studio 2010 solution.

The Visual Studio project contains pre-build commands to generate the thriftl.cc, thrifty.cc and thrifty.hh files which are necessary to build the compiler. 

These depend on bison, flex and their dependencies to work properly.

Download flex & bison as described above. 

Place these binaries somewhere in the path and rename win_flex.exe and win_bison.exe to flex.exe and bison.exe respectively.

If this doesn't work on a system, try these manual pre-build steps.

Open compiler.sln and remove the Pre-build commands under the project's: Properties -> Build Events -> Pre-Build Events.

From a command prompt:
```
cd thrift/compiler/cpp
flex -o src\thrift\thriftl.cc src\thrift\thriftl.ll
```
In the generated thriftl.cc, comment out #include <unistd.h>

Place a copy of bison.simple in thrift/compiler/cpp
```
bison -y -o "src/thrift/thrifty.cc" --defines src/thrift/thrifty.yy
move src\thrift\thrifty.cc.hh  src\thrift\thrifty.hh
```

Bison might generate the yacc header file "thrifty.cc.h" with just one h ".h" extension; in this case you'll have to rename to "thrifty.h".

```
move src\thrift\version.h.in src\thrift\version.h
```

Download inttypes.h from the interwebs and place it in an include path
location (e.g. thrift/compiler/cpp/src).

Build the compiler in Visual Studio.

# Unit tests for compiler

## Using boost test
- pls check **test** folder

## Using Catch C++ test library

Added generic way to cover code by tests for many languages (you just need to make a correct header file for generator for your language - example in **netcore** implementation)

- pls check **tests** folder

# Have a Happy free time and holidays 
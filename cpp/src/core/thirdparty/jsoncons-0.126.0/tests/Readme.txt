
Build instructions

Microsoft Visual Studio
=======================
To build the test suite with Visual C++ version 10 SP1 or later:

Set BOOST_ROOT to the root directory containing Boost
Set BOOST_LIB to the directory containing Boost static libraries

CMake
=====
* set current directory to jsonsons/test_suite/build/cmake

* On Windows, set BOOST_ROOT and BOOST_LIBRARYDIR to enable cmake to locate boost stuff.

* Generate build scripts of your choise: cmake -G <generator>
  where <generator> can be (other choises are possible):
  * Windows: "Visual Studio 10" or "NMake Makefiles"
  * Unix: "Ninja" or "Unix Makefiles"

* Compilation: use appropriate actions depending of generated files.

* Tests execution:
  * Windows: ./run_tests.bat
  * Unix: ./run_tests.sh

SCons
=====
WARNING: Only usable on Unix with gcc as compiler

* set current directory to jsonsons/test_suite/build/scons

* compilation: scons

* Tests execution:
  * Unix: ./run_tests.sh

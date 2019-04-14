rm -rf libmyLib.so lib/libmyLib.so lib/mylib.o lib/myLib.a myLib.a myapp logs ## Clean

compiler=g++
standard=c++0x ## If this does not work try c++11 (depends on your compiler)
macros="-DELPP_THREAD_SAFE -DELPP_FEATURE_CRASH_LOG"  ## Macros for library

cd lib/
$compiler --std=$standard -pipe -fPIC -g -O0 $macros -Iinclude  -c mylib.cpp ../../../../src/easylogging++.cc
ar rvs myLib.a mylib.o easylogging++.o
cp myLib.a ..
cd ..
$compiler -g -std=$standard -o myapp myapp.cpp ../../../src/easylogging++.cc -Ilib/include myLib.a

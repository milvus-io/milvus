./configure CPPFLAGS='-DUSE_CPU' CXXFLAGS='-O0 -g -fPIC -Wno-sign-compare -Wall -Wextra' --prefix=$PWD --without-cuda
make install -j4

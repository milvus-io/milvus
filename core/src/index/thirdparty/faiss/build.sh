./configure CPUFLAGS='-mavx -mf16c -msse4 -mpopcnt'   CXXFLAGS='-O0 -g -fPIC -m64 -Wno-sign-compare -Wall -Wextra' --prefix=$PWD --with-cuda-arch=-gencode=arch=compute_75,code=sm_75 --with-cuda=/usr/local/cuda
make install -j

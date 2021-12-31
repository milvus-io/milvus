#./configure CPUFLAGS='-mavx -mf16c -msse4 -mpopcnt'   CXXFLAGS='-O0 -g -fPIC -m64 -Wno-sign-compare -Wall -Wextra' --prefix=$PWD --with-cuda-arch=-gencode=arch=compute_75,code=sm_75 --with-cuda=/usr/local/cuda
./configure --prefix=$PWD CFLAGS='-g -fPIC' CXXFLAGS='-O3 -g -fPIC -DELPP_THREAD_SAFE -fopenmp -g -fPIC -mf16c -O3 -DNDEBUG' --without-python --with-cuda=/usr/local/cuda --with-cuda-arch='-gencode=arch=compute_60,code=sm_60 -gencode=arch=compute_61,code=sm_61 -gencode=arch=compute_70,code=sm_70 -gencode=arch=compute_75,code=sm_75'
make install -j8

# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

CXX          = /usr/bin/c++ -std=c++11
CXXCPP       = /usr/bin/c++ -std=c++11 -E
CPPFLAGS     = -DFINTEGER=int   -I/usr/local/cuda/include 
CXXFLAGS     = -fPIC -m64 -Wno-sign-compare  -O3 -fPIC -DELPP_THREAD_SAFE -fopenmp -Werror -O3 -DNDEBUG -fPIC -mf16c -O3
CPUFLAGS     = -mpopcnt -msse4
LDFLAGS      =  -L/usr/lib/x86_64-linux-gnu -L/usr/local/cuda/lib64 -L/usr/lib/x86_64-linux-gnu
LIBS         = -lopenblas   -lcudart -lcublas 
PYTHONCFLAGS =   -I
SWIGFLAGS    = -DSWIGWORDSIZE64

NVCC         = /usr/bin/ccache /usr/local/cuda/bin/nvcc
CUDA_ROOT    = /usr/local/cuda
CUDA_ARCH    = -gencode=arch=compute_60,code=sm_60 -gencode=arch=compute_61,code=sm_61 -gencode=arch=compute_70,code=sm_70 -gencode=arch=compute_75,code=sm_75
NVCCFLAGS    = -I $(CUDA_ROOT)/targets/x86_64-linux/include/ \
-O3 \
-Xcompiler -fPIC \
-Xcudafe --diag_suppress=unrecognized_attribute \
$(CUDA_ARCH) \
-lineinfo \
-ccbin $(CXX)

OS = $(shell uname -s)

SHAREDEXT   = so
SHAREDFLAGS = -shared

ifeq ($(OS),Darwin)
	SHAREDEXT   = dylib
	SHAREDFLAGS = -dynamiclib -undefined dynamic_lookup
        SWIGFLAGS   =
endif

MKDIR_P      = /bin/mkdir -p
PYTHON       = 
SWIG         = 
AR          ?= ar

prefix      ?= /home/caiyd/vec/milvus/core/cmake_build/src/index/faiss_ep-prefix/src/faiss_ep
exec_prefix ?= ${prefix}
libdir       = ${exec_prefix}/lib
includedir   = ${prefix}/include

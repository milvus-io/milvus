# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD+Patents license found in the
# LICENSE file in the root directory of this source tree.

-include makefile.inc

HEADERS     = $(wildcard *.h)
SRC         = $(wildcard *.cpp)
OBJ         = $(SRC:.cpp=.o)
INSTALLDIRS = $(DESTDIR)$(libdir) $(DESTDIR)$(includedir)/faiss

GPU_HEADERS = $(wildcard gpu/*.h gpu/impl/*.h gpu/utils/*.h)
GPU_CPPSRC  = $(wildcard gpu/*.cpp gpu/impl/*.cpp gpu/utils/*.cpp)
GPU_CUSRC   = $(wildcard gpu/*.cu gpu/impl/*.cu gpu/utils/*.cu \
gpu/utils/nvidia/*.cu gpu/utils/blockselect/*.cu gpu/utils/warpselect/*.cu)
GPU_SRC     = $(GPU_CPPSRC) $(GPU_CUSRC)
GPU_CPPOBJ  = $(GPU_CPPSRC:.cpp=.o)
GPU_CUOBJ   = $(GPU_CUSRC:.cu=.o)
GPU_OBJ     = $(GPU_CPPOBJ) $(GPU_CUOBJ)
GPU_INSTALLDIRS = $(DESTDIR)$(includedir)/faiss/gpu/{impl,utils}

ifneq ($(strip $(NVCC)),)
	OBJ         += $(GPU_OBJ)
	INSTALLDIRS += $(GPU_INSTALLDIRS)
	HEADERS     += $(GPU_HEADERS)
endif


############################
# Building

all: libfaiss.a libfaiss.$(SHAREDEXT)

libfaiss.a: $(OBJ)
	$(AR) r $@ $^

libfaiss.$(SHAREDEXT): $(OBJ)
	$(CXX) $(SHAREDFLAGS) $(LDFLAGS) -o $@ $^ $(LIBS)

%.o: %.cpp
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $(CPUFLAGS) -c $< -o $@

%.o: %.cu
	$(NVCC) $(NVCCFLAGS) -g -O3 -c $< -o $@

clean:
	rm -f libfaiss.{a,$(SHAREDEXT)}
	rm -f $(OBJ)


############################
# Installing

install: libfaiss.a libfaiss.$(SHAREDEXT) installdirs
	cp libfaiss.{a,$(SHAREDEXT)} $(DESTDIR)$(libdir)
	tar cf - $(HEADERS) | tar xf - -C $(DESTDIR)$(includedir)/faiss/

installdirs:
	$(MKDIR_P) $(INSTALLDIRS)

uninstall:
	rm -f $(DESTDIR)$(libdir)/libfaiss.{a,$(SHAREDEXT)}
	rm -rf $(DESTDIR)$(includedir)/faiss


#############################
# Dependencies

-include depend

depend: $(SRC) $(GPU_SRC)
	for i in $^; do \
		$(CXXCPP) $(CPPFLAGS) -x c++ -MM $$i; \
	done > depend


#############################
# Python

py: libfaiss.a
	$(MAKE) -C python


#############################
# Tests

test: libfaiss.a py
	$(MAKE) -C tests run
	PYTHONPATH=./python/build/`ls python/build | grep lib` \
	$(PYTHON) -m unittest discover tests/ -v

test_gpu: libfaiss.a
	$(MAKE) -C gpu/test run
	PYTHONPATH=./python/build/`ls python/build | grep lib` \
	$(PYTHON) -m unittest discover gpu/test/ -v

#############################
# Demos

demos: libfaiss.a
	$(MAKE) -C demos


#############################
# Misc

misc/test_blas: misc/test_blas.cpp
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $(LDFLAGS) -o $@ $^ $(LIBS)


.PHONY: all clean demos install installdirs py test gpu_test uninstall

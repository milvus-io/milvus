# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

-include makefile.inc

HEADERS     = $(wildcard *.h impl/*.h utils/*.h)
SRC         = $(wildcard *.cpp impl/*.cpp utils/*.cpp)
OBJ         = $(SRC:.cpp=.o)
INSTALLDIRS = $(DESTDIR)$(libdir) $(DESTDIR)$(includedir)/faiss

GPU_HEADERS = $(wildcard gpu/*.h gpu/impl/*.h gpu/impl/*.cuh gpu/utils/*.h gpu/utils/*.cuh)
GPU_CPPSRC  = $(wildcard gpu/*.cpp gpu/impl/*.cpp gpu/utils/*.cpp)
GPU_CUSRC   = $(wildcard gpu/*.cu gpu/impl/*.cu gpu/utils/*.cu \
gpu/utils/nvidia/*.cu gpu/utils/blockselect/*.cu gpu/utils/warpselect/*.cu)
GPU_SRC     = $(GPU_CPPSRC) $(GPU_CUSRC)
GPU_CPPOBJ  = $(GPU_CPPSRC:.cpp=.o)
GPU_CUOBJ   = $(GPU_CUSRC:.cu=.o)
GPU_OBJ     = $(GPU_CPPOBJ) $(GPU_CUOBJ)

ifneq ($(strip $(NVCC)),)
	OBJ         += $(GPU_OBJ)
	HEADERS     += $(GPU_HEADERS)
endif

CPPFLAGS += -I.
NVCCFLAGS += -I.

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
	rm -f libfaiss.a libfaiss.$(SHAREDEXT)
	rm -f $(OBJ)


############################
# Installing

install: libfaiss.a libfaiss.$(SHAREDEXT) installdirs
	cp libfaiss.a libfaiss.$(SHAREDEXT) $(DESTDIR)$(libdir)
	tar cf - $(HEADERS) | tar xf - -C $(DESTDIR)$(includedir)/faiss/

installdirs:
	$(MKDIR_P) $(INSTALLDIRS)

uninstall:
	rm -f $(DESTDIR)$(libdir)/libfaiss.a \
	      $(DESTDIR)$(libdir)/libfaiss.$(SHAREDEXT)
	rm -rf $(DESTDIR)$(includedir)/faiss


#############################
# Dependencies

-include depend

depend: $(SRC) $(GPU_SRC)
	for i in $^; do \
		$(CXXCPP) $(CPPFLAGS) -DCUDA_VERSION=7050 -x c++ -MM $$i; \
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


.PHONY: all clean demos install installdirs py test test_gpu uninstall

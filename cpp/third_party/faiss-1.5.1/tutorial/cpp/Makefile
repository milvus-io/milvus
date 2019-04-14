# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD+Patents license found in the
# LICENSE file in the root directory of this source tree.

-include ../../makefile.inc

CPU_TARGETS = 1-Flat 2-IVFFlat 3-IVFPQ
GPU_TARGETS = 4-GPU 5-Multiple-GPUs

default: cpu

all: cpu gpu

cpu: $(CPU_TARGETS)

gpu: $(GPU_TARGETS)

$(CPU_TARGETS): %: %.cpp ../../libfaiss.a
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS) -I../../.. $(LIBS)

$(GPU_TARGETS): %: %.cpp ../../libfaiss.a ../../gpu/libgpufaiss.a
	$(NVCC) $(NVCCFLAGS) -o $@ $^ $(NVCCLDFLAGS) -I../../.. $(NVCCLIBS)

clean:
	rm -f $(CPU_TARGETS) $(GPU_TARGETS)

.PHONY: all cpu default gpu clean

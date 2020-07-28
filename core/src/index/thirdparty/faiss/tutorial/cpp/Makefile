# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

-include ../../makefile.inc

#CPU_TARGETS = 1-Flat 2-IVFFlat 3-IVFPQ 9-BinaryFlat
CPU_TARGETS = hnsw-sq8-milvus hnsw-sq8-faiss
GPU_TARGETS = 4-GPU 5-Multiple-GPUs

default: cpu

all: cpu gpu

cpu: $(CPU_TARGETS)

gpu: $(GPU_TARGETS)

%: %.cpp ../../libfaiss.a
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) -o $@ $^ $(LDFLAGS) -I../../ $(LIBS) -lhdf5_serial

clean:
	rm -f $(CPU_TARGETS) $(GPU_TARGETS)

.PHONY: all cpu default gpu clean

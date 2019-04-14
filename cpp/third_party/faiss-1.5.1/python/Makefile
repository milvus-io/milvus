# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD+Patents license found in the
# LICENSE file in the root directory of this source tree.

-include ../makefile.inc

ifneq ($(strip $(NVCC)),)
	SWIGFLAGS = -DGPU_WRAPPER
endif

all: build

# Also silently generates swigfaiss.py.
swigfaiss.cpp: swigfaiss.swig ../libfaiss.a
	$(SWIG) -python -c++ -Doverride= -I../ $(SWIGFLAGS) -o $@ $<

swigfaiss.o: swigfaiss.cpp
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $(CPUFLAGS) $(PYTHONCFLAGS) \
               -I../ -c $< -o $@

# Extension is .so even on OSX.
_swigfaiss.so: swigfaiss.o ../libfaiss.a
	$(CXX) $(SHAREDFLAGS) $(LDFLAGS) -o $@ $^ $(LIBS)

build: _swigfaiss.so faiss.py
	$(PYTHON) setup.py build

install: build
	$(PYTHON) setup.py install

clean:
	rm -f swigfaiss.{cpp,o,py}
	rm -f _swigfaiss.so
	rm -rf build/

.PHONY: all build clean install

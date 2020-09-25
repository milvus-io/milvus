# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

-include ../makefile.inc

ifneq ($(strip $(NVCC)),)
	SWIGFLAGS += -DGPU_WRAPPER
endif

all: build

# Also silently generates swigfaiss.py.
swigfaiss.cpp: swigfaiss.swig ../libfaiss.a
	$(SWIG) -python -c++ -Doverride= -I../ $(SWIGFLAGS) -o $@ $<

swigfaiss_avx2.cpp: swigfaiss.swig ../libfaiss.a
	$(SWIG) -python -c++ -Doverride= -module swigfaiss_avx2 -I../ $(SWIGFLAGS) -o $@ $<

%.o: %.cpp
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $(CPUFLAGS) $(PYTHONCFLAGS) \
               -I../ -c $< -o $@

# Extension is .so even on OSX.
_%.so: %.o ../libfaiss.a
	$(CXX) $(SHAREDFLAGS) $(LDFLAGS) -o $@ $^ $(LIBS)

build: _swigfaiss.so faiss.py
	$(PYTHON) setup.py build

install: build
	$(PYTHON) setup.py install

clean:
	rm -f swigfaiss*.cpp swigfaiss*.o swigfaiss*.py _swigfaiss*.so
	rm -rf build/

.PHONY: all build clean install

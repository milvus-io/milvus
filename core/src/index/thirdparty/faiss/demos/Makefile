# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

-include ../makefile.inc

DEMOS_SRC=$(wildcard demo_*.cpp)
DEMOS=$(DEMOS_SRC:.cpp=)


all: $(DEMOS)

clean:
	rm -f $(DEMOS)

%: %.cpp
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $(CPUFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS) -lfaiss


.PHONY: all clean

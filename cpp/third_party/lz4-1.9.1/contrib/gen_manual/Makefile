# ################################################################
# Copyright (C) Przemyslaw Skibinski 2016-present
# All rights reserved.
#
# BSD license
# Redistribution and use in source and binary forms, with or without modification,
# are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice, this
#   list of conditions and the following disclaimer in the documentation and/or
#   other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
# ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
# ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
# You can contact the author at :
#  - LZ4 source repository : https://github.com/Cyan4973/lz4
#  - LZ4 forum froup : https://groups.google.com/forum/#!forum/lz4c
# ################################################################


CXXFLAGS ?= -O3
CXXFLAGS += -Wall -Wextra -Wcast-qual -Wcast-align -Wshadow -Wstrict-aliasing=1 -Wswitch-enum -Wno-comment
CXXFLAGS += $(MOREFLAGS)
FLAGS   = $(CPPFLAGS) $(CXXFLAGS) $(LDFLAGS)

LZ4API = ../../lib/lz4.h
LZ4MANUAL = ../../doc/lz4_manual.html
LZ4FAPI = ../../lib/lz4frame.h
LZ4FMANUAL = ../../doc/lz4frame_manual.html
LIBVER_MAJOR_SCRIPT:=`sed -n '/define LZ4_VERSION_MAJOR/s/.*[[:blank:]]\([0-9][0-9]*\).*/\1/p' < $(LZ4API)`
LIBVER_MINOR_SCRIPT:=`sed -n '/define LZ4_VERSION_MINOR/s/.*[[:blank:]]\([0-9][0-9]*\).*/\1/p' < $(LZ4API)`
LIBVER_PATCH_SCRIPT:=`sed -n '/define LZ4_VERSION_RELEASE/s/.*[[:blank:]]\([0-9][0-9]*\).*/\1/p' < $(LZ4API)`
LIBVER_SCRIPT:= $(LIBVER_MAJOR_SCRIPT).$(LIBVER_MINOR_SCRIPT).$(LIBVER_PATCH_SCRIPT)
LZ4VER := $(shell echo $(LIBVER_SCRIPT))

# Define *.exe as extension for Windows systems
ifneq (,$(filter Windows%,$(OS)))
EXT =.exe
else
EXT =
endif


.PHONY: default
default: gen_manual

gen_manual: gen_manual.cpp
	$(CXX) $(FLAGS) $^ -o $@$(EXT)

$(LZ4MANUAL) : gen_manual $(LZ4API)
	echo "Update lz4 manual in /doc"
	./gen_manual $(LZ4VER) $(LZ4API) $@

$(LZ4FMANUAL) : gen_manual $(LZ4FAPI)
	echo "Update lz4frame manual in /doc"
	./gen_manual $(LZ4VER) $(LZ4FAPI) $@

.PHONY: manuals
manuals: gen_manual $(LZ4MANUAL) $(LZ4FMANUAL)

.PHONY: clean
clean:
	@$(RM) gen_manual$(EXT)
	@echo Cleaning completed

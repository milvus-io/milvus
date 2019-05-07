# ################################################################
# LZ4 - Makefile
# Copyright (C) Yann Collet 2011-present
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
#  - LZ4 source repository : https://github.com/lz4/lz4
#  - LZ4 forum froup : https://groups.google.com/forum/#!forum/lz4c
# ################################################################

LZ4DIR  = lib
PRGDIR  = programs
TESTDIR = tests
EXDIR   = examples

include Makefile.inc

.PHONY: default
default: lib-release lz4-release

.PHONY: all
all: allmost examples manuals build_tests

.PHONY: allmost
allmost: lib lz4

.PHONY: lib lib-release liblz4.a
lib: liblz4.a
lib lib-release liblz4.a:
	@$(MAKE) -C $(LZ4DIR) $@

.PHONY: lz4 lz4-release
lz4 : liblz4.a
lz4-release : lib-release
lz4 lz4-release :
	@$(MAKE) -C $(PRGDIR) $@
	@cp $(PRGDIR)/lz4$(EXT) .

.PHONY: examples
examples: liblz4.a
	$(MAKE) -C $(EXDIR) all

.PHONY: manuals
manuals:
	@$(MAKE) -C contrib/gen_manual $@

.PHONY: build_tests
build_tests:
	@$(MAKE) -C $(TESTDIR) all

.PHONY: clean
clean:
	@$(MAKE) -C $(LZ4DIR) $@ > $(VOID)
	@$(MAKE) -C $(PRGDIR) $@ > $(VOID)
	@$(MAKE) -C $(TESTDIR) $@ > $(VOID)
	@$(MAKE) -C $(EXDIR) $@ > $(VOID)
	@$(MAKE) -C contrib/gen_manual $@ > $(VOID)
	@$(RM) lz4$(EXT)
	@echo Cleaning completed


#-----------------------------------------------------------------------------
# make install is validated only for Linux, OSX, BSD, Hurd and Solaris targets
#-----------------------------------------------------------------------------
ifeq ($(POSIX_ENV),Yes)
HOST_OS = POSIX

.PHONY: install uninstall
install uninstall:
	@$(MAKE) -C $(LZ4DIR) $@
	@$(MAKE) -C $(PRGDIR) $@

travis-install:
	$(MAKE) -j1 install DESTDIR=~/install_test_dir

cmake:
	@cd contrib/cmake_unofficial; cmake $(CMAKE_PARAMS) CMakeLists.txt; $(MAKE)

endif


ifneq (,$(filter MSYS%,$(shell uname)))
HOST_OS = MSYS
CMAKE_PARAMS = -G"MSYS Makefiles"
endif


#------------------------------------------------------------------------
#make tests validated only for MSYS, Linux, OSX, kFreeBSD and Hurd targets
#------------------------------------------------------------------------
ifneq (,$(filter $(HOST_OS),MSYS POSIX))

.PHONY: list
list:
	@$(MAKE) -pRrq -f $(lastword $(MAKEFILE_LIST)) : 2>/dev/null | awk -v RS= -F: '/^# File/,/^# Finished Make data base/ {if ($$1 !~ "^[#.]") {print $$1}}' | sort | egrep -v -e '^[^[:alnum:]]' -e '^$@$$' | xargs

.PHONY: check
check:
	$(MAKE) -C $(TESTDIR) test-lz4-essentials

.PHONY: test
test:
	$(MAKE) -C $(TESTDIR) $@
	$(MAKE) -C $(EXDIR) $@

clangtest: clean
	clang -v
	@CFLAGS="-O3 -Werror -Wconversion -Wno-sign-conversion" $(MAKE) -C $(LZ4DIR)  all CC=clang
	@CFLAGS="-O3 -Werror -Wconversion -Wno-sign-conversion" $(MAKE) -C $(PRGDIR)  all CC=clang
	@CFLAGS="-O3 -Werror -Wconversion -Wno-sign-conversion" $(MAKE) -C $(TESTDIR) all CC=clang

clangtest-native: clean
	clang -v
	@CFLAGS="-O3 -Werror -Wconversion -Wno-sign-conversion" $(MAKE) -C $(LZ4DIR)  all    CC=clang
	@CFLAGS="-O3 -Werror -Wconversion -Wno-sign-conversion" $(MAKE) -C $(PRGDIR)  native CC=clang
	@CFLAGS="-O3 -Werror -Wconversion -Wno-sign-conversion" $(MAKE) -C $(TESTDIR) native CC=clang

usan: clean
	CC=clang CFLAGS="-O3 -g -fsanitize=undefined" $(MAKE) test FUZZER_TIME="-T30s" NB_LOOPS=-i1

usan32: clean
	CFLAGS="-m32 -O3 -g -fsanitize=undefined" $(MAKE) test FUZZER_TIME="-T30s" NB_LOOPS=-i1

.PHONY: staticAnalyze
staticAnalyze: clean
	CFLAGS=-g scan-build --status-bugs -v $(MAKE) all

.PHONY: cppcheck
cppcheck:
	cppcheck . --force --enable=warning,portability,performance,style --error-exitcode=1 > /dev/null

platformTest: clean
	@echo "\n ---- test lz4 with $(CC) compiler ----"
	@$(CC) -v
	CFLAGS="-O3 -Werror"         $(MAKE) -C $(LZ4DIR) all
	CFLAGS="-O3 -Werror -static" $(MAKE) -C $(PRGDIR) all
	CFLAGS="-O3 -Werror -static" $(MAKE) -C $(TESTDIR) all
	$(MAKE) -C $(TESTDIR) test-platform

.PHONY: versionsTest
versionsTest: clean
	$(MAKE) -C $(TESTDIR) $@

gpptest gpptest32: CC = "$(CXX) -Wno-deprecated"
gpptest gpptest32: CFLAGS = -O3 -Wall -Wextra -Wundef -Wshadow -Wcast-align -Werror
gpptest32: CFLAGS += -m32
gpptest gpptest32: clean
	$(CXX) -v
	CC=$(CC) $(MAKE) -C $(LZ4DIR)  all CFLAGS="$(CFLAGS)"
	CC=$(CC) $(MAKE) -C $(PRGDIR)  all CFLAGS="$(CFLAGS)"
	CC=$(CC) $(MAKE) -C $(TESTDIR) all CFLAGS="$(CFLAGS)"

cxx17build : CC = "$(CXX) -Wno-deprecated"
cxx17build : CFLAGS = -std=c++17 -Wall -Wextra -Wundef -Wshadow -Wcast-align -Werror -pedantic
cxx17build : clean
	$(CXX) -v
	CC=$(CC) $(MAKE) -C $(LZ4DIR)  all CFLAGS="$(CFLAGS)"
	CC=$(CC) $(MAKE) -C $(PRGDIR)  all CFLAGS="$(CFLAGS)"
	CC=$(CC) $(MAKE) -C $(TESTDIR) all CFLAGS="$(CFLAGS)"

ctocpptest: LIBCC="$(CC)"
ctocpptest: TESTCC="$(CXX)"
ctocpptest: CFLAGS=""
ctocpptest: clean
	CC=$(LIBCC)  $(MAKE) -C $(LZ4DIR)  CFLAGS="$(CFLAGS)" all
	CC=$(LIBCC)  $(MAKE) -C $(TESTDIR) CFLAGS="$(CFLAGS)" lz4.o lz4hc.o lz4frame.o
	CC=$(TESTCC) $(MAKE) -C $(TESTDIR) CFLAGS="$(CFLAGS)" all

c_standards: clean
	$(MAKE) clean; CFLAGS="-std=c90   -Werror -pedantic -Wno-long-long -Wno-variadic-macros" $(MAKE) allmost
	$(MAKE) clean; CFLAGS="-std=gnu90 -Werror -pedantic -Wno-long-long -Wno-variadic-macros" $(MAKE) allmost
	$(MAKE) clean; CFLAGS="-std=c99   -Werror -pedantic" $(MAKE) all
	$(MAKE) clean; CFLAGS="-std=gnu99 -Werror -pedantic" $(MAKE) all
	$(MAKE) clean; CFLAGS="-std=c11   -Werror" $(MAKE) all

endif

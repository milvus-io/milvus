# ################################################################
# LZ4 library - Makefile
# Copyright (C) Yann Collet 2011-2016
# All rights reserved.
#
# This Makefile is validated for Linux, macOS, *BSD, Hurd, Solaris, MSYS2 targets
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

# Version numbers
LIBVER_MAJOR_SCRIPT:=`sed -n '/define LZ4_VERSION_MAJOR/s/.*[[:blank:]]\([0-9][0-9]*\).*/\1/p' < ./lz4.h`
LIBVER_MINOR_SCRIPT:=`sed -n '/define LZ4_VERSION_MINOR/s/.*[[:blank:]]\([0-9][0-9]*\).*/\1/p' < ./lz4.h`
LIBVER_PATCH_SCRIPT:=`sed -n '/define LZ4_VERSION_RELEASE/s/.*[[:blank:]]\([0-9][0-9]*\).*/\1/p' < ./lz4.h`
LIBVER_SCRIPT:= $(LIBVER_MAJOR_SCRIPT).$(LIBVER_MINOR_SCRIPT).$(LIBVER_PATCH_SCRIPT)
LIBVER_MAJOR := $(shell echo $(LIBVER_MAJOR_SCRIPT))
LIBVER_MINOR := $(shell echo $(LIBVER_MINOR_SCRIPT))
LIBVER_PATCH := $(shell echo $(LIBVER_PATCH_SCRIPT))
LIBVER  := $(shell echo $(LIBVER_SCRIPT))

BUILD_SHARED:=yes
BUILD_STATIC:=yes

CPPFLAGS+= -DXXH_NAMESPACE=LZ4_
CFLAGS  ?= -O3
DEBUGFLAGS:= -Wall -Wextra -Wcast-qual -Wcast-align -Wshadow \
             -Wswitch-enum -Wdeclaration-after-statement -Wstrict-prototypes \
             -Wundef -Wpointer-arith -Wstrict-aliasing=1
CFLAGS  += $(DEBUGFLAGS) $(MOREFLAGS)
FLAGS    = $(CPPFLAGS) $(CFLAGS) $(LDFLAGS)

SRCFILES := $(sort $(wildcard *.c))


# OS X linker doesn't support -soname, and use different extension
# see : https://developer.apple.com/library/mac/documentation/DeveloperTools/Conceptual/DynamicLibraries/100-Articles/DynamicLibraryDesignGuidelines.html
ifeq ($(OS), Darwin)
	SHARED_EXT = dylib
	SHARED_EXT_MAJOR = $(LIBVER_MAJOR).$(SHARED_EXT)
	SHARED_EXT_VER = $(LIBVER).$(SHARED_EXT)
	SONAME_FLAGS = -install_name $(libdir)/liblz4.$(SHARED_EXT_MAJOR) -compatibility_version $(LIBVER_MAJOR) -current_version $(LIBVER)
else
	SONAME_FLAGS = -Wl,-soname=liblz4.$(SHARED_EXT).$(LIBVER_MAJOR)
	SHARED_EXT = so
	SHARED_EXT_MAJOR = $(SHARED_EXT).$(LIBVER_MAJOR)
	SHARED_EXT_VER = $(SHARED_EXT).$(LIBVER)
endif

include ../Makefile.inc

.PHONY: default
default: lib-release

lib-release: DEBUGFLAGS :=
lib-release: lib

lib: liblz4.a liblz4

all: lib

all32: CFLAGS+=-m32
all32: all

liblz4.a: $(SRCFILES)
ifeq ($(BUILD_STATIC),yes)  # can be disabled on command line
	@echo compiling static library
	$(Q)$(CC) $(CPPFLAGS) $(CFLAGS) -c $^
	$(Q)$(AR) rcs $@ *.o
endif

ifeq ($(WINBASED),yes)
liblz4-dll.rc: liblz4-dll.rc.in
	@echo creating library resource
	$(Q)sed -e 's|@LIBLZ4@|$(LIBLZ4)|' \
         -e 's|@LIBVER_MAJOR@|$(LIBVER_MAJOR)|g' \
         -e 's|@LIBVER_MINOR@|$(LIBVER_MINOR)|g' \
         -e 's|@LIBVER_PATCH@|$(LIBVER_PATCH)|g' \
          $< >$@

liblz4-dll.o: liblz4-dll.rc
	$(WINDRES) -i liblz4-dll.rc -o liblz4-dll.o

$(LIBLZ4): $(SRCFILES) liblz4-dll.o
else
$(LIBLZ4): $(SRCFILES)
endif
ifeq ($(BUILD_SHARED),yes)  # can be disabled on command line
	@echo compiling dynamic library $(LIBVER)
  ifeq ($(WINBASED),yes)
	$(Q)$(CC) $(FLAGS) -DLZ4_DLL_EXPORT=1 -shared $^ -o dll/$@.dll -Wl,--out-implib,dll/$(LIBLZ4_EXP)
  else
	$(Q)$(CC) $(FLAGS) -shared $^ -fPIC -fvisibility=hidden $(SONAME_FLAGS) -o $@
	@echo creating versioned links
	$(Q)$(LN_SF) $@ liblz4.$(SHARED_EXT_MAJOR)
	$(Q)$(LN_SF) $@ liblz4.$(SHARED_EXT)
  endif
endif

ifeq (,$(filter MINGW%,$(TARGET_OS)))
liblz4: $(LIBLZ4)
endif

clean:
ifeq ($(WINBASED),yes)
	$(Q)$(RM) *.rc
endif
	$(Q)$(RM) core *.o liblz4.pc dll/$(LIBLZ4).dll dll/$(LIBLZ4_EXP)
	$(Q)$(RM) *.a *.$(SHARED_EXT) *.$(SHARED_EXT_MAJOR) *.$(SHARED_EXT_VER)
	@echo Cleaning library completed

#-----------------------------------------------------------------------------
# make install is validated only for Linux, OSX, BSD, Hurd and Solaris targets
#-----------------------------------------------------------------------------
ifeq ($(POSIX_ENV),Yes)

.PHONY: listL120
listL120:  # extract lines >= 120 characters in *.{c,h}, by Takayuki Matsuoka (note : $$, for Makefile compatibility)
	find . -type f -name '*.c' -o -name '*.h' | while read -r filename; do awk 'length > 120 {print FILENAME "(" FNR "): " $$0}' $$filename; done

DESTDIR     ?=
# directory variables : GNU conventions prefer lowercase
# see https://www.gnu.org/prep/standards/html_node/Makefile-Conventions.html
# support both lower and uppercase (BSD), use lower in script
PREFIX      ?= /usr/local
prefix      ?= $(PREFIX)
EXEC_PREFIX ?= $(prefix)
exec_prefix ?= $(EXEC_PREFIX)
BINDIR      ?= $(exec_prefix)/bin
bindir      ?= $(BINDIR)
LIBDIR      ?= $(exec_prefix)/lib
libdir      ?= $(LIBDIR)
INCLUDEDIR  ?= $(prefix)/include
includedir  ?= $(INCLUDEDIR)

  ifneq (,$(filter $(TARGET_OS),OpenBSD FreeBSD NetBSD DragonFly MidnightBSD))
PKGCONFIGDIR ?= $(prefix)/libdata/pkgconfig
  else
PKGCONFIGDIR ?= $(libdir)/pkgconfig
  endif
pkgconfigdir ?= $(PKGCONFIGDIR)

liblz4.pc: liblz4.pc.in Makefile
	@echo creating pkgconfig
	$(Q)sed -e 's|@PREFIX@|$(prefix)|' \
         -e 's|@LIBDIR@|$(libdir)|' \
         -e 's|@INCLUDEDIR@|$(includedir)|' \
         -e 's|@VERSION@|$(LIBVER)|' \
          $< >$@

install: lib liblz4.pc
	$(Q)$(INSTALL_DIR) $(DESTDIR)$(pkgconfigdir)/ $(DESTDIR)$(includedir)/ $(DESTDIR)$(libdir)/ $(DESTDIR)$(bindir)/
	$(Q)$(INSTALL_DATA) liblz4.pc $(DESTDIR)$(pkgconfigdir)/
	@echo Installing libraries
  ifeq ($(BUILD_STATIC),yes)
	$(Q)$(INSTALL_DATA) liblz4.a $(DESTDIR)$(libdir)/liblz4.a
	$(Q)$(INSTALL_DATA) lz4frame_static.h $(DESTDIR)$(includedir)/lz4frame_static.h
  endif
  ifeq ($(BUILD_SHARED),yes)
# Traditionnally, one installs the DLLs in the bin directory as programs
# search them first in their directory. This allows to not pollute system
# directories (like c:/windows/system32), nor modify the PATH variable.
    ifeq ($(WINBASED),yes)
	$(Q)$(INSTALL_PROGRAM) dll/$(LIBLZ4).dll $(DESTDIR)$(bindir)
	$(Q)$(INSTALL_PROGRAM) dll/$(LIBLZ4_EXP) $(DESTDIR)$(libdir)
    else
	$(Q)$(INSTALL_PROGRAM) liblz4.$(SHARED_EXT_VER) $(DESTDIR)$(libdir)
	$(Q)$(LN_SF) liblz4.$(SHARED_EXT_VER) $(DESTDIR)$(libdir)/liblz4.$(SHARED_EXT_MAJOR)
	$(Q)$(LN_SF) liblz4.$(SHARED_EXT_VER) $(DESTDIR)$(libdir)/liblz4.$(SHARED_EXT)
    endif
  endif
	@echo Installing headers in $(includedir)
	$(Q)$(INSTALL_DATA) lz4.h $(DESTDIR)$(includedir)/lz4.h
	$(Q)$(INSTALL_DATA) lz4hc.h $(DESTDIR)$(includedir)/lz4hc.h
	$(Q)$(INSTALL_DATA) lz4frame.h $(DESTDIR)$(includedir)/lz4frame.h
	@echo lz4 libraries installed

uninstall:
	$(Q)$(RM) $(DESTDIR)$(pkgconfigdir)/liblz4.pc
  ifeq (WINBASED,1)
	$(Q)$(RM) $(DESTDIR)$(bindir)/$(LIBLZ4).dll
	$(Q)$(RM) $(DESTDIR)$(libdir)/$(LIBLZ4_EXP)
  else
	$(Q)$(RM) $(DESTDIR)$(libdir)/liblz4.$(SHARED_EXT)
	$(Q)$(RM) $(DESTDIR)$(libdir)/liblz4.$(SHARED_EXT_MAJOR)
	$(Q)$(RM) $(DESTDIR)$(libdir)/liblz4.$(SHARED_EXT_VER)
  endif
	$(Q)$(RM) $(DESTDIR)$(libdir)/liblz4.a
	$(Q)$(RM) $(DESTDIR)$(includedir)/lz4.h
	$(Q)$(RM) $(DESTDIR)$(includedir)/lz4hc.h
	$(Q)$(RM) $(DESTDIR)$(includedir)/lz4frame.h
	$(Q)$(RM) $(DESTDIR)$(includedir)/lz4frame_static.h
	@echo lz4 libraries successfully uninstalled

endif

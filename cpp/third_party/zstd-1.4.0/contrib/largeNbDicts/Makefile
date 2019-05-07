# ################################################################
# Copyright (c) 2018-present, Yann Collet, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under both the BSD-style license (found in the
# LICENSE file in the root directory of this source tree) and the GPLv2 (found
# in the COPYING file in the root directory of this source tree).
# ################################################################

PROGDIR = ../../programs
LIBDIR  = ../../lib

LIBZSTD = $(LIBDIR)/libzstd.a

CPPFLAGS+= -I$(LIBDIR) -I$(LIBDIR)/common -I$(LIBDIR)/dictBuilder -I$(PROGDIR)

CFLAGS  ?= -O3
CFLAGS  += -std=gnu99
DEBUGFLAGS= -Wall -Wextra -Wcast-qual -Wcast-align -Wshadow \
            -Wstrict-aliasing=1 -Wswitch-enum \
            -Wstrict-prototypes -Wundef -Wpointer-arith \
            -Wvla -Wformat=2 -Winit-self -Wfloat-equal -Wwrite-strings \
            -Wredundant-decls
CFLAGS  += $(DEBUGFLAGS) $(MOREFLAGS)


default: largeNbDicts

all : largeNbDicts

largeNbDicts: util.o timefn.o benchfn.o datagen.o xxhash.o largeNbDicts.c $(LIBZSTD)
	$(CC) $(CPPFLAGS) $(CFLAGS) $^ $(LDFLAGS) -o $@

.PHONY: $(LIBZSTD)
$(LIBZSTD):
	$(MAKE) -C $(LIBDIR) libzstd.a CFLAGS="$(CFLAGS)"

benchfn.o: $(PROGDIR)/benchfn.c
	$(CC) $(CPPFLAGS) $(CFLAGS) $^ -c

timefn.o: $(PROGDIR)/timefn.c
	$(CC) $(CPPFLAGS) $(CFLAGS) $^ -c

datagen.o: $(PROGDIR)/datagen.c
	$(CC) $(CPPFLAGS) $(CFLAGS) $^ -c

util.o: $(PROGDIR)/util.c
	$(CC) $(CPPFLAGS) $(CFLAGS) $^ -c


xxhash.o : $(LIBDIR)/common/xxhash.c
	$(CC) $(CPPFLAGS) $(CFLAGS) $^ -c


clean:
	$(RM) *.o
	$(MAKE) -C $(LIBDIR) clean > /dev/null
	$(RM) largeNbDicts


ZSTDDIR = ../../lib
PRGDIR  = ../../programs
ZSTDCOMMON_FILES := $(ZSTDDIR)/common/*.c
ZSTDCOMP_FILES   := $(ZSTDDIR)/compress/*.c
ZSTDDECOMP_FILES := $(ZSTDDIR)/decompress/*.c
ZSTD_FILES  := $(ZSTDDECOMP_FILES) $(ZSTDCOMMON_FILES) $(ZSTDCOMP_FILES)

MULTITHREAD_LDFLAGS = -pthread
DEBUGFLAGS= -g -DZSTD_DEBUG=1
CPPFLAGS += -I$(ZSTDDIR) -I$(ZSTDDIR)/common -I$(ZSTDDIR)/compress \
            -I$(ZSTDDIR)/dictBuilder -I$(ZSTDDIR)/deprecated -I$(PRGDIR)
CFLAGS   ?= -O3
CFLAGS   += -Wall -Wextra -Wcast-qual -Wcast-align -Wshadow                 \
            -Wstrict-aliasing=1 -Wswitch-enum -Wdeclaration-after-statement \
            -Wstrict-prototypes -Wundef                                     \
            -Wvla -Wformat=2 -Winit-self -Wfloat-equal -Wwrite-strings      \
            -Wredundant-decls
CFLAGS   += $(DEBUGFLAGS)
CFLAGS   += $(MOREFLAGS)
FLAGS     = $(CPPFLAGS) $(CFLAGS) $(LDFLAGS) $(MULTITHREAD_LDFLAGS)

all: adapt datagen

adapt: $(ZSTD_FILES) $(PRGDIR)/util.c $(PRGDIR)/timefn.c adapt.c
	$(CC) $(FLAGS) $^ -o $@

adapt-debug: $(ZSTD_FILES) $(PRGDIR)/util.c $(PRGDIR)/timefn.c adapt.c
	$(CC) $(FLAGS) -DDEBUG_MODE=2 $^ -o adapt

datagen : $(PRGDIR)/datagen.c datagencli.c
	$(CC)      $(FLAGS) $^ -o $@

test-adapt-correctness: datagen adapt
	@./test-correctness.sh
	@echo "test correctness complete"

test-adapt-performance: datagen adapt
	@./test-performance.sh
	@echo "test performance complete"

clean:
	@$(RM) -f adapt datagen
	@$(RM) -rf *.dSYM
	@$(RM) -f tmp*
	@$(RM) -f tests/*.zst
	@$(RM) -f tests/tmp*
	@echo "finished cleaning"

#-----------------------------------------------------------------------------
# make install is validated only for Linux, macOS, BSD, Hurd and Solaris targets
#-----------------------------------------------------------------------------
ifneq (,$(filter $(shell uname),Linux Darwin GNU/kFreeBSD GNU OpenBSD FreeBSD NetBSD DragonFly SunOS))

ifneq (,$(filter $(shell uname),SunOS))
INSTALL ?= ginstall
else
INSTALL ?= install
endif

PREFIX  ?= /usr/local
DESTDIR ?=
BINDIR  ?= $(PREFIX)/bin

INSTALL_PROGRAM ?= $(INSTALL) -m 755

install: adapt
	@echo Installing binaries
	@$(INSTALL) -d -m 755 $(DESTDIR)$(BINDIR)/
	@$(INSTALL_PROGRAM) adapt $(DESTDIR)$(BINDIR)/zstd-adaptive
	@echo zstd-adaptive installation completed

uninstall:
	@$(RM) $(DESTDIR)$(BINDIR)/zstd-adaptive
	@echo zstd-adaptive programs successfully uninstalled
endif

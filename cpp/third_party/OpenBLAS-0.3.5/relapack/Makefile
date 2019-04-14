TOPDIR  = ..
include $(TOPDIR)/Makefile.system



SRC = $(wildcard src/*.c)

SRC1 = \
	src/slauum.c src/clauum.c src/dlauum.c src/zlauum.c \
	src/strtri.c src/dtrtri.c src/ctrtri.c src/ztrtri.c \
	src/spotrf.c src/dpotrf.c src/cpotrf.c src/zpotrf.c \
	src/sgetrf.c src/dgetrf.c src/cgetrf.c src/zgetrf.c

SRC2 = \
	src/cgbtrf.c src/cpbtrf.c src/dsytrf_rec2.c src/sgbtrf.c src/ssytrf_rook.c src/zhegst.c src/zsytrf_rec2.c \
	src/cgemmt.c src/dgbtrf.c src/dsytrf_rook.c src/sgemmt.c src/ssytrf_rook_rec2.c src/zhetrf.c src/zsytrf_rook.c \
	src/csytrf.c src/dgemmt.c src/dsytrf_rook_rec2.c src/stgsyl.c src/zhetrf_rec2.c src/zsytrf_rook_rec2.c \
	src/chegst.c src/csytrf_rec2.c src/dtgsyl.c src/strsyl.c src/zhetrf_rook.c src/ztgsyl.c \
	src/chetrf.c src/csytrf_rook.c src/dtrsyl.c src/spbtrf.c src/strsyl_rec2.c src/zhetrf_rook_rec2.c src/ztrsyl.c \
	src/chetrf_rec2.c src/csytrf_rook_rec2.c src/dpbtrf.c  src/dtrsyl_rec2.c  src/ztrsyl_rec2.c \
	src/chetrf_rook.c src/ctgsyl.c src/ssygst.c src/zgbtrf.c src/zpbtrf.c  \
	src/chetrf_rook_rec2.c src/ctrsyl.c src/dsygst.c src/f2c.c src/ssytrf.c src/zgemmt.c \
	src/ctrsyl_rec2.c src/dsytrf.c src/lapack_wrappers.c src/ssytrf_rec2.c src/zsytrf.c

SRCX = \
	src/cgbtrf.c src/cpbtrf.c src/ctrtri.c src/dsytrf_rec2.c src/sgbtrf.c src/ssytrf_rook.c src/zhegst.c src/zsytrf_rec2.c \
	src/cgemmt.c src/cpotrf.c src/dgbtrf.c src/dsytrf_rook.c src/sgemmt.c src/ssytrf_rook_rec2.c src/zhetrf.c src/zsytrf_rook.c \
	src/cgetrf.c src/csytrf.c src/dgemmt.c src/dsytrf_rook_rec2.c src/sgetrf.c src/stgsyl.c src/zhetrf_rec2.c src/zsytrf_rook_rec2.c \
	src/chegst.c src/csytrf_rec2.c src/dgetrf.c src/dtgsyl.c src/slauum.c src/strsyl.c src/zhetrf_rook.c src/ztgsyl.c \
	src/chetrf.c src/csytrf_rook.c src/dlauum.c src/dtrsyl.c src/spbtrf.c src/strsyl_rec2.c src/zhetrf_rook_rec2.c src/ztrsyl.c \
	src/chetrf_rec2.c src/csytrf_rook_rec2.c src/dpbtrf.c  src/dtrsyl_rec2.c src/spotrf.c src/strtri.c src/zlauum.c src/ztrsyl_rec2.c \
	src/chetrf_rook.c src/ctgsyl.c src/dpotrf.c src/dtrtri.c src/ssygst.c src/zgbtrf.c src/zpbtrf.c src/ztrtri.c \
	src/chetrf_rook_rec2.c src/ctrsyl.c src/dsygst.c src/f2c.c src/ssytrf.c src/zgemmt.c src/zpotrf.c \
	src/clauum.c src/ctrsyl_rec2.c src/dsytrf.c src/lapack_wrappers.c src/ssytrf_rec2.c src/zgetrf.c src/zsytrf.c

OBJS1 = $(SRC1:%.c=%.$(SUFFIX))
OBJS2 = $(SRC2:%.c=%.o)
OBJS = $(OBJS1) $(OBJS2)

TEST_SUITS = \
	slauum dlauum clauum zlauum \
	spotrf dpotrf cpotrf zpotrf \
	spbtrf dpbtrf cpbtrf zpbtrf \
	ssygst dsygst chegst zhegst \
	ssytrf dsytrf csytrf chetrf zsytrf zhetrf \
	sgetrf dgetrf cgetrf zgetrf \
	sgbtrf dgbtrf cgbtrf zgbtrf \
	strsyl dtrsyl ctrsyl ztrsyl \
	stgsyl dtgsyl ctgsyl ztgsyl \
	sgemmt dgemmt cgemmt zgemmt
TESTS = $(TEST_SUITS:%=test/%.pass)  # dummies
TEST_EXES = $(TEST_SUITS:%=test/%.x)

LINK_TEST = -L$(TOPDIR) -lopenblas -lgfortran -lm

.SECONDARY: $(TEST_EXES)
.PHONY: test

# ReLAPACK compilation

libs:	$(OBJS)
	@echo "Building ReLAPACK library $(LIBNAME)"
	$(AR) -r  $(TOPDIR)/$(LIBNAME) $(OBJS)
	$(RANLIB) $(TOPDIR)/$(LIBNAME)

%.$(SUFFIX): %.c config.h
	$(CC) $(CFLAGS) -c $< -o $@

%.o: %.c config.h
	$(CC) $(CFLAGS) -c $< -o $@


# ReLAPACK testing

test: $(TEST_EXES) $(TESTS)
	@echo "passed all tests"

test/%.pass: test/%.x
	@echo -n $*:
	@./$< > /dev/null && echo " pass" || (echo " FAIL" && ./$<)

test/s%.x: test/x%.c test/util.$(SUFFIX) $(TOPDIR)/$(LIBNAME) test/config.h test/test.h
	$(CC) $(CFLAGS) -DDT_PREFIX=s $< test/util.$(SUFFIX) -o $@ $(LINK_TEST) $(TOPDIR)/$(LIBNAME) $(LINK_TEST)

test/d%.x: test/x%.c test/util.$(SUFFIX) $(TOPDIR)/$(LIBNAME) test/config.h test/test.h
	$(CC) $(CFLAGS) -DDT_PREFIX=d $< test/util.$(SUFFIX) -o $@ $(LINK_TEST) $(TOPDIR)/$(LIBNAME) $(LINK_TEST)

test/c%.x: test/x%.c test/util.$(SUFFIX) $(TOPDIR)/$(LIBNAME) test/config.h test/test.h
	$(CC) $(CFLAGS) -DDT_PREFIX=c $< test/util.$(SUFFIX) -o $@ $(LINK_TEST) $(TOPDIR)/$(LIBNAME) $(LINK_TEST)

test/z%.x: test/x%.c test/util.$(SUFFIX) $(TOPDIR)/$(LIBNAME) test/config.h test/test.h
	$(CC) $(CFLAGS) -DDT_PREFIX=z $< test/util.$(SUFFIX) -o $@ $(LINK_TEST) $(TOPDIR)/$(LIBNAME) $(LINK_TEST)


# cleaning up

clean:
	rm -f  $(OBJS) test/util.$(SUFFIX) test/*.x

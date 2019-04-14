TOPDIR	= ..

include ../Makefile.system

ifndef EXPRECISION
EXPRECISION	= 0
endif

ifndef NO_CBLAS
NO_CBLAS	= 0
endif

ifndef NO_LAPACK
NO_LAPACK	= 0
endif

ifndef NO_LAPACKE
NO_LAPACKE	= 0
endif

ifndef NEED2UNDERSCORES
NEED2UNDERSCORES=0
endif

ifndef ONLY_CBLAS
ONLY_CBLAS	= 0
endif

ifndef BUILD_LAPACK_DEPRECATED
BUILD_LAPACK_DEPRECATED = 0
endif

ifeq ($(OSNAME), WINNT)
ifeq ($(F_COMPILER), GFORTRAN)
ifndef ONLY_CBLAS
EXTRALIB += -lgfortran
endif
endif
ifeq ($(USE_OPENMP), 1)
ifeq ($(C_COMPILER), GCC)
EXTRALIB += -lgomp
endif
endif
endif

ifeq ($(OSNAME), CYGWIN_NT)
ifeq ($(F_COMPILER), GFORTRAN)
ifndef ONLY_CBLAS
EXTRALIB += -lgfortran
endif
endif
endif

ifneq (,$(filter 1 2,$(NOFORTRAN)))
FEXTRALIB =
endif

all::

libs::

prof::

hpl:: libgoto_hpl.$(LIBSUFFIX)

hpl_p:: libgoto_hpl_p.$(LIBSUFFIX)

libgoto_hpl.$(LIBSUFFIX) : ../$(LIBNAME)
	rm -f $(@F)
	$(LD) -r $(LDFLAGS) -o goto.$(SUFFIX) --whole-archive $< --no-whole-archive
	$(AR) cq $(@F) goto.$(SUFFIX)
	$(RANLIB) libgoto_hpl.$(LIBSUFFIX)

libgoto_hpl_p.$(LIBSUFFIX) :  ../$(LIBNAME_P)
	rm -f $(@F)
	$(LD) -r  $(LDFLAGS) -o goto.$(PSUFFIX)  --whole-archive $< --no-whole-archive
	$(AR) cq $(@F) goto.$(PSUFFIX)
	$(RANLIB) libgoto_hpl_p.$(LIBSUFFIX)

libgoto_hpl.dll : libgoto_hpl.$(LIBSUFFIX) dllinit.$(SUFFIX) libgoto_hpl.def
	$(DLLWRAP) -o $(@F) --def libgoto_hpl.def --entry _dllinit -s dllinit.$(SUFFIX) --dllname libgoto_hpl.dll libgoto_hpl.$(LIBSUFFIX)
	lib /machine:X64 /def:libgoto_hpl.def

dyn : $(LIBDYNNAME)

zip : dll
	zip $(LIBZIPNAME) $(LIBDLLNAME) $(LIBNAME)

dll  : ../$(LIBDLLNAME)

# On Windows, we only generate a DLL without a version suffix.  This is because
# applications which link against the dynamic library reference a fixed DLL name
# in their import table.  By instead using a stable name it is possible to
# upgrade between library versions, without needing to re-link an application.
# For more details see: https://github.com/xianyi/OpenBLAS/issues/127.
../$(LIBDLLNAME) : ../$(LIBNAME) $(LIBPREFIX).def dllinit.$(SUFFIX)
	$(RANLIB) ../$(LIBNAME)
	$(CC) $(CFLAGS) $(LDFLAGS) $(LIBPREFIX).def dllinit.$(SUFFIX) \
	-shared -o ../$(LIBDLLNAME) -Wl,--out-implib,../$(IMPLIBNAME) \
	-Wl,--whole-archive ../$(LIBNAME) -Wl,--no-whole-archive $(FEXTRALIB) $(EXTRALIB)

$(LIBPREFIX).def : gensymbol
	perl ./gensymbol win2k    $(ARCH) dummy $(EXPRECISION) $(NO_CBLAS) $(NO_LAPACK) $(NO_LAPACKE) $(NEED2UNDERSCORES) $(ONLY_CBLAS) "$(SYMBOLPREFIX)" "$(SYMBOLSUFFIX)" $(BUILD_LAPACK_DEPRECATED) > $(@F)

libgoto_hpl.def : gensymbol
	perl ./gensymbol win2khpl $(ARCH) dummy $(EXPRECISION) $(NO_CBLAS) $(NO_LAPACK) $(NO_LAPACKE) $(NEED2UNDERSCORES) $(ONLY_CBLAS) "$(SYMBOLPREFIX)" "$(SYMBOLSUFFIX)" $(BUILD_LAPACK_DEPRECATED) > $(@F)

ifeq (, $(SYMBOLPREFIX)$(SYMBOLSUFFIX))
$(LIBDYNNAME) : ../$(LIBNAME) osx.def
else
../$(LIBNAME).osx.renamed : ../$(LIBNAME) objconv.def
	$(OBJCONV) @objconv.def ../$(LIBNAME) ../$(LIBNAME).osx.renamed
$(LIBDYNNAME) : ../$(LIBNAME).osx.renamed osx.def
endif
ifneq (,$(filter 1 2,$(NOFORTRAN)))
#only build without Fortran
	$(CC) $(CFLAGS) $(LDFLAGS) -all_load -headerpad_max_install_names -install_name "$(CURDIR)/../$(LIBDYNNAME)" -dynamiclib -o ../$(LIBDYNNAME) $< -Wl,-exported_symbols_list,osx.def  $(FEXTRALIB)
else
	$(FC) $(FFLAGS) $(LDFLAGS) -all_load -headerpad_max_install_names -install_name "$(CURDIR)/../$(LIBDYNNAME)" -dynamiclib -o ../$(LIBDYNNAME) $< -Wl,-exported_symbols_list,osx.def  $(FEXTRALIB)
endif

dllinit.$(SUFFIX) : dllinit.c
	$(CC) $(CFLAGS) -c -o $(@F) -s $<

ifeq ($(OSNAME), $(filter $(OSNAME),Linux SunOS Android Haiku))

so : ../$(LIBSONAME)

ifeq ($(OSNAME), Android)
INTERNALNAME = $(LIBPREFIX).so
FEXTRALIB += -lm
EXTRALIB += -lm
else
INTERNALNAME = $(LIBPREFIX).so.$(MAJOR_VERSION)
endif

ifeq (, $(SYMBOLPREFIX)$(SYMBOLSUFFIX))
../$(LIBSONAME) : ../$(LIBNAME) linktest.c
else
../$(LIBNAME).renamed : ../$(LIBNAME) objcopy.def
	$(OBJCOPY) --redefine-syms objcopy.def ../$(LIBNAME) ../$(LIBNAME).renamed
../$(LIBSONAME) : ../$(LIBNAME).renamed linktest.c
endif
ifneq ($(C_COMPILER), LSB)
	$(CC) $(CFLAGS) $(LDFLAGS) -shared -o ../$(LIBSONAME) \
	-Wl,--whole-archive $< -Wl,--no-whole-archive \
	-Wl,-soname,$(INTERNALNAME) $(EXTRALIB)
	$(CC) $(CFLAGS) $(LDFLAGS) -w -o linktest linktest.c ../$(LIBSONAME) $(FEXTRALIB) && echo OK.
else
#for LSB
	env LSBCC_SHAREDLIBS=gfortran $(CC) $(CFLAGS) $(LDFLAGS) -shared -o ../$(LIBSONAME) \
	-Wl,--whole-archive $< -Wl,--no-whole-archive \
	-Wl,-soname,$(INTERNALNAME) $(EXTRALIB)
	$(FC) $(CFLAGS) $(LDFLAGS) -w -o linktest linktest.c ../$(LIBSONAME) $(FEXTRALIB) && echo OK.
endif
	rm -f linktest

endif

#http://stackoverflow.com/questions/7656425/makefile-ifeq-logical-or
ifeq ($(OSNAME), $(filter $(OSNAME),FreeBSD OpenBSD NetBSD DragonFly))

so : ../$(LIBSONAME)

ifeq (, $(SYMBOLPREFIX)$(SYMBOLSUFFIX))
../$(LIBSONAME) : ../$(LIBNAME) linktest.c
else
../$(LIBNAME).renamed : ../$(LIBNAME) objcopy.def
	$(OBJCOPY) --redefine-syms objcopy.def ../$(LIBNAME) ../$(LIBNAME).renamed
../$(LIBSONAME) : ../$(LIBNAME).renamed linktest.c
endif
	$(CC) $(CFLAGS) $(LDFLAGS)  -shared -o ../$(LIBSONAME) \
	-Wl,--whole-archive $< -Wl,--no-whole-archive \
	$(FEXTRALIB) $(EXTRALIB)
	$(CC) $(CFLAGS) $(LDFLAGS) -w -o linktest linktest.c ../$(LIBSONAME) $(FEXTRALIB) && echo OK.
	rm -f linktest

endif

ifeq ($(OSNAME), OSF1)

so : ../$(LIBSONAME)

../$(LIBSONAME) :
	$(CC) $(CFLAGS) $(LDFLAGS) -shared -o ../$(LIBSONAME) ../$(LIBNAME)
endif

ifeq ($(OSNAME), SunOS)

so : ../$(LIBSONAME)
	$(CC) $(CFLAGS) $(LDFLAGS)  -shared -o ../$(LIBSONAME) \
	-Wl,--whole-archive ../$(LIBNAME) -Wl,--no-whole-archive $(EXTRALIB)
	$(CC) $(CFLAGS) $(LDFLAGS) -w -o linktest linktest.c ../$(LIBSONAME) $(FEXTRALIB) && echo OK.
	rm -f linktest

endif

ifeq ($(OSNAME), AIX)

ifeq ($(COMPILER_F77), xlf)

goto32.$(SUFFIX) : ../$(LIBNAME) aix.def
	ld -o $(@F) ../$(LIBNAME) -bE:aix.def -bM:SRE -bnoexpall -bnoentry -L$(HOME)/misc/lib -lxlf90 -lc -lm  -lpthread

goto64.$(SUFFIX) : ../$(LIBNAME) aix.def
	ld -b64 -o $(@F) ../$(LIBNAME) -bE:aix.def -bM:SRE -bnoexpall -bnoentry -L$(HOME)/misc/lib/ppc64 -lxlf90 -lc -lm -lpthread
else
goto32.$(SUFFIX) : ../$(LIBNAME) aix.def
	ld -o $(@F) ../$(LIBNAME) -bE:aix.def -bM:SRE -bnoexpall -bnoentry -L$(HOME)/misc/lib -lg2c -lc -lm

goto64.$(SUFFIX) : ../$(LIBNAME) aix.def
	ld -b64 -o $(@F) ../$(LIBNAME) -bE:aix.def -bM:SRE -bnoexpall -bnoentry -L$(HOME)/misc/lib/ppc64 -lg2c -lc -lm
endif
endif

static : ../$(LIBNAME)
	$(LD) $(LDFLAGS) -r -o goto.$(SUFFIX) \
	--whole-archive ../$(LIBNAME) --no-whole-archive
	rm -f ../$(LIBNAME)
	$(AR) -cq ../$(LIBNAME) goto.$(SUFFIX)
	rm -f goto.$(SUFFIX)

osx.def : gensymbol ../Makefile.system ../getarch.c
	perl ./gensymbol osx $(ARCH) $(BU) $(EXPRECISION) $(NO_CBLAS)  $(NO_LAPACK) $(NO_LAPACKE) $(NEED2UNDERSCORES) $(ONLY_CBLAS) "$(SYMBOLPREFIX)" "$(SYMBOLSUFFIX)" $(BUILD_LAPACK_DEPRECATED) > $(@F)

aix.def : gensymbol ../Makefile.system ../getarch.c
	perl ./gensymbol aix $(ARCH) $(BU) $(EXPRECISION) $(NO_CBLAS)  $(NO_LAPACK) $(NO_LAPACKE) $(NEED2UNDERSCORES) $(ONLY_CBLAS) "$(SYMBOLPREFIX)" "$(SYMBOLSUFFIX)" $(BUILD_LAPACK_DEPRECATED) > $(@F)

objcopy.def : gensymbol ../Makefile.system ../getarch.c
	perl ./gensymbol objcopy $(ARCH) $(BU) $(EXPRECISION) $(NO_CBLAS)  $(NO_LAPACK) $(NO_LAPACKE) $(NEED2UNDERSCORES) $(ONLY_CBLAS) "$(SYMBOLPREFIX)" "$(SYMBOLSUFFIX)" $(BUILD_LAPACK_DEPRECATED) > $(@F)

objconv.def : gensymbol ../Makefile.system ../getarch.c
	perl ./gensymbol objconv $(ARCH) $(BU) $(EXPRECISION) $(NO_CBLAS)  $(NO_LAPACK) $(NO_LAPACKE) $(NEED2UNDERSCORES) $(ONLY_CBLAS) "$(SYMBOLPREFIX)" "$(SYMBOLSUFFIX)" $(BUILD_LAPACK_DEPRECATED) > $(@F)

test : linktest.c
	$(CC) $(CFLAGS) $(LDFLAGS) -w -o linktest linktest.c ../$(LIBSONAME) -lm && echo OK.
	rm -f linktest

linktest.c : gensymbol ../Makefile.system ../getarch.c
	perl ./gensymbol linktest  $(ARCH) $(BU) $(EXPRECISION) $(NO_CBLAS) $(NO_LAPACK) $(NO_LAPACKE) $(NEED2UNDERSCORES) $(ONLY_CBLAS) "$(SYMBOLPREFIX)" "$(SYMBOLSUFFIX)" $(BUILD_LAPACK_DEPRECATED) > linktest.c

clean ::
	@rm -f *.def *.dylib __.SYMDEF* *.renamed

include ../Makefile.tail



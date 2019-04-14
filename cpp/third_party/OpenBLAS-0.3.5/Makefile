TOPDIR	= .
include ./Makefile.system

BLASDIRS = interface driver/level2 driver/level3 driver/others

ifneq ($(DYNAMIC_ARCH), 1)
BLASDIRS += kernel
endif

ifdef SANITY_CHECK
BLASDIRS += reference
endif

SUBDIRS	= $(BLASDIRS)
ifneq ($(NO_LAPACK), 1)
SUBDIRS	+= lapack
endif

RELA =
ifeq ($(BUILD_RELAPACK), 1)
RELA = re_lapack
endif

ifeq ($(NO_FORTRAN), 1)
define NOFORTRAN
1
endef
define NO_LAPACK
1
endef
export NOFORTRAN
export NO_LAPACK
endif

LAPACK_NOOPT := $(filter-out -O0 -O1 -O2 -O3 -Ofast,$(LAPACK_FFLAGS))

SUBDIRS_ALL = $(SUBDIRS) test ctest utest exports benchmark ../laswp ../bench

.PHONY : all libs netlib $(RELA) test ctest shared install
.NOTPARALLEL : all libs $(RELA) prof lapack-test install blas-test

all :: libs netlib $(RELA) tests shared
	@echo
	@echo " OpenBLAS build complete. ($(LIB_COMPONENTS))"
	@echo
	@echo "  OS               ... $(OSNAME)             "
	@echo "  Architecture     ... $(ARCH)               "
ifndef BINARY64
	@echo "  BINARY           ... 32bit                 "
else
	@echo "  BINARY           ... 64bit                 "
endif

ifdef INTERFACE64
ifneq ($(INTERFACE64), 0)
	@echo "  Use 64 bits int    (equivalent to \"-i8\" in Fortran)      "
endif
endif

	@echo "  C compiler       ... $(C_COMPILER)  (command line : $(CC))"
ifeq ($(NOFORTRAN), $(filter 0,$(NOFORTRAN)))
	@echo "  Fortran compiler ... $(F_COMPILER)  (command line : $(FC))"
endif
ifneq ($(OSNAME), AIX)
	@echo -n "  Library Name     ... $(LIBNAME)"
else
	@echo "  Library Name     ... $(LIBNAME)"
endif

ifndef SMP
	@echo " (Single threaded)  "
else
	@echo " (Multi threaded; Max num-threads is $(NUM_THREADS))"
endif

ifeq ($(USE_OPENMP), 1)
	@echo
	@echo " Use OpenMP in the multithreading. Because of ignoring OPENBLAS_NUM_THREADS and GOTO_NUM_THREADS flags, "
	@echo " you should use OMP_NUM_THREADS environment variable to control the number of threads."
	@echo
endif

ifeq ($(OSNAME), Darwin)
	@echo "WARNING: If you plan to use the dynamic library $(LIBDYNNAME), you must run:"
	@echo
	@echo "\"make PREFIX=/your_installation_path/ install\"."
	@echo
	@echo "(or set PREFIX in Makefile.rule and run make install."
	@echo "If you want to move the .dylib to a new location later, make sure you change"
	@echo "the internal name of the dylib with:"
	@echo
	@echo "install_name_tool -id /new/absolute/path/to/$(LIBDYNNAME) $(LIBDYNNAME)"
endif
	@echo
	@echo "To install the library, you can run \"make PREFIX=/path/to/your/installation install\"."
	@echo

shared :
ifndef NO_SHARED
ifeq ($(OSNAME), $(filter $(OSNAME),Linux SunOS Android Haiku))
	@$(MAKE) -C exports so
	@ln -fs $(LIBSONAME) $(LIBPREFIX).so
	@ln -fs $(LIBSONAME) $(LIBPREFIX).so.$(MAJOR_VERSION)
endif
ifeq ($(OSNAME), $(filter $(OSNAME),FreeBSD OpenBSD NetBSD DragonFly))
	@$(MAKE) -C exports so
	@ln -fs $(LIBSONAME) $(LIBPREFIX).so
endif
ifeq ($(OSNAME), Darwin)
	@$(MAKE) -C exports dyn
	@ln -fs $(LIBDYNNAME) $(LIBPREFIX).dylib
endif
ifeq ($(OSNAME), WINNT)
	@$(MAKE) -C exports dll
endif
ifeq ($(OSNAME), CYGWIN_NT)
	@$(MAKE) -C exports dll
endif
endif

tests :
ifeq ($(NOFORTRAN), $(filter 0,$(NOFORTRAN)))
	touch $(LIBNAME)
ifndef NO_FBLAS
	$(MAKE) -C test all
	$(MAKE) -C utest all
endif
ifndef NO_CBLAS
	$(MAKE) -C ctest all
endif
endif

libs :
ifeq ($(CORE), UNKNOWN)
	$(error OpenBLAS: Detecting CPU failed. Please set TARGET explicitly, e.g. make TARGET=your_cpu_target. Please read README for the detail.)
endif
ifeq ($(NOFORTRAN), 1)
	$(info OpenBLAS: Detecting fortran compiler failed. Cannot compile LAPACK. Only compile BLAS.)
endif
ifeq ($(NO_STATIC), 1)
ifeq ($(NO_SHARED), 1)
	$(error OpenBLAS: neither static nor shared are enabled.)
endif
endif
	@-ln -fs $(LIBNAME) $(LIBPREFIX).$(LIBSUFFIX)
	@for d in $(SUBDIRS) ; \
	do if test -d $$d; then \
	  $(MAKE) -C $$d $(@F) || exit 1 ; \
	fi; \
	done
#Save the config files for installation
	@cp Makefile.conf Makefile.conf_last
	@cp config.h config_last.h
ifdef QUAD_PRECISION
	@echo "#define QUAD_PRECISION">> config_last.h
endif
ifeq ($(EXPRECISION), 1)
	@echo "#define EXPRECISION">> config_last.h
endif
##
ifeq ($(DYNAMIC_ARCH), 1)
	@$(MAKE) -C kernel commonlibs || exit 1
	@for d in $(DYNAMIC_CORE) ; \
	do  $(MAKE) GOTOBLAS_MAKEFILE= -C kernel TARGET_CORE=$$d kernel || exit 1 ;\
	done
	@echo DYNAMIC_ARCH=1 >> Makefile.conf_last
ifeq ($(DYNAMIC_OLDER), 1)
	@echo DYNAMIC_OLDER=1 >> Makefile.conf_last
endif	
endif
ifdef USE_THREAD
	@echo USE_THREAD=$(USE_THREAD) >>  Makefile.conf_last
endif
	@touch lib.grd

prof : prof_blas prof_lapack

prof_blas :
	ln -fs $(LIBNAME_P) $(LIBPREFIX)_p.$(LIBSUFFIX)
	for d in $(SUBDIRS) ; \
	do if test -d $$d; then \
	  $(MAKE) -C $$d prof || exit 1 ; \
	fi; \
	done
ifeq ($(DYNAMIC_ARCH), 1)
	  $(MAKE) -C kernel commonprof || exit 1
endif

blas :
	ln -fs $(LIBNAME) $(LIBPREFIX).$(LIBSUFFIX)
	for d in $(BLASDIRS) ; \
	do if test -d $$d; then \
	  $(MAKE) -C $$d libs || exit 1 ; \
	fi; \
	done

hpl :
	ln -fs $(LIBNAME) $(LIBPREFIX).$(LIBSUFFIX)
	for d in $(BLASDIRS) ../laswp exports ; \
	do if test -d $$d; then \
	  $(MAKE) -C $$d $(@F) || exit 1 ; \
	fi; \
	done
ifeq ($(DYNAMIC_ARCH), 1)
	  $(MAKE) -C kernel commonlibs || exit 1
	for d in $(DYNAMIC_CORE) ; \
	do  $(MAKE) GOTOBLAS_MAKEFILE= -C kernel TARGET_CORE=$$d kernel || exit 1 ;\
	done
endif

hpl_p :
	ln -fs $(LIBNAME_P) $(LIBPREFIX)_p.$(LIBSUFFIX)
	for d in $(SUBDIRS) ../laswp exports ; \
	do if test -d $$d; then \
	  $(MAKE) -C $$d $(@F) || exit 1 ; \
	fi; \
	done

ifeq ($(NO_LAPACK), 1)
netlib :

else
netlib : lapack_prebuild
ifeq ($(NOFORTRAN), $(filter 0,$(NOFORTRAN)))
	@$(MAKE) -C $(NETLIB_LAPACK_DIR) lapacklib
	@$(MAKE) -C $(NETLIB_LAPACK_DIR) tmglib
endif
ifndef NO_LAPACKE
	@$(MAKE) -C $(NETLIB_LAPACK_DIR) lapackelib
endif
endif

ifeq ($(NO_LAPACK), 1)
re_lapack :

else
re_lapack :
	@$(MAKE) -C relapack
endif

prof_lapack : lapack_prebuild
	@$(MAKE) -C $(NETLIB_LAPACK_DIR) lapack_prof

lapack_prebuild :
ifeq ($(NOFORTRAN), $(filter 0,$(NOFORTRAN)))
	-@echo "FORTRAN     = $(FC)" > $(NETLIB_LAPACK_DIR)/make.inc
	-@echo "OPTS        = $(LAPACK_FFLAGS)" >> $(NETLIB_LAPACK_DIR)/make.inc
	-@echo "POPTS       = $(LAPACK_FPFLAGS)" >> $(NETLIB_LAPACK_DIR)/make.inc
	-@echo "NOOPT       = -O0 $(LAPACK_NOOPT)" >> $(NETLIB_LAPACK_DIR)/make.inc
	-@echo "PNOOPT      = $(LAPACK_FPFLAGS) -O0" >> $(NETLIB_LAPACK_DIR)/make.inc
	-@echo "LOADOPTS    = $(FFLAGS) $(EXTRALIB)" >> $(NETLIB_LAPACK_DIR)/make.inc
	-@echo "CC          = $(CC)" >> $(NETLIB_LAPACK_DIR)/make.inc
	-@echo "override CFLAGS      = $(LAPACK_CFLAGS)" >> $(NETLIB_LAPACK_DIR)/make.inc
	-@echo "override ARCH        = $(AR)" >> $(NETLIB_LAPACK_DIR)/make.inc
	-@echo "ARCHFLAGS   = $(ARFLAGS) -ru" >> $(NETLIB_LAPACK_DIR)/make.inc
	-@echo "RANLIB      = $(RANLIB)" >> $(NETLIB_LAPACK_DIR)/make.inc
	-@echo "LAPACKLIB   = ../$(LIBNAME)" >> $(NETLIB_LAPACK_DIR)/make.inc
	-@echo "TMGLIB      = ../$(LIBNAME)" >> $(NETLIB_LAPACK_DIR)/make.inc
	-@echo "BLASLIB     = ../../../$(LIBNAME)" >> $(NETLIB_LAPACK_DIR)/make.inc
	-@echo "LAPACKELIB  = ../$(LIBNAME)" >> $(NETLIB_LAPACK_DIR)/make.inc
	-@echo "LAPACKLIB_P = ../$(LIBNAME_P)" >> $(NETLIB_LAPACK_DIR)/make.inc
	-@echo "SUFFIX      = $(SUFFIX)" >> $(NETLIB_LAPACK_DIR)/make.inc
	-@echo "PSUFFIX     = $(PSUFFIX)" >> $(NETLIB_LAPACK_DIR)/make.inc
	-@echo "CEXTRALIB   = $(EXTRALIB)" >> $(NETLIB_LAPACK_DIR)/make.inc
ifeq ($(F_COMPILER), GFORTRAN)
	-@echo "TIMER       = INT_ETIME" >> $(NETLIB_LAPACK_DIR)/make.inc
ifdef SMP
ifeq ($(OSNAME), WINNT)
	-@echo "LOADER      = $(FC)" >> $(NETLIB_LAPACK_DIR)/make.inc
else ifeq ($(OSNAME), Haiku)
	-@echo "LOADER      = $(FC)" >> $(NETLIB_LAPACK_DIR)/make.inc
else
	-@echo "LOADER      = $(FC) -pthread" >> $(NETLIB_LAPACK_DIR)/make.inc
endif
else
	-@echo "LOADER      = $(FC)" >> $(NETLIB_LAPACK_DIR)/make.inc
endif
else
	-@echo "TIMER       = NONE" >> $(NETLIB_LAPACK_DIR)/make.inc
	-@echo "LOADER      = $(FC)" >> $(NETLIB_LAPACK_DIR)/make.inc
endif
ifeq ($(BUILD_LAPACK_DEPRECATED), 1)
	-@echo "BUILD_DEPRECATED      = 1" >> $(NETLIB_LAPACK_DIR)/make.inc
endif
	-@echo "LAPACKE_WITH_TMG      = 1" >> $(NETLIB_LAPACK_DIR)/make.inc
	-@cat  make.inc >> $(NETLIB_LAPACK_DIR)/make.inc
endif

large.tgz :
ifeq ($(NOFORTRAN), $(filter 0,$(NOFORTRAN)))
	if [ ! -a $< ]; then
	-wget http://www.netlib.org/lapack/timing/large.tgz;
	fi
endif

timing.tgz :
ifeq ($(NOFORTRAN), $(filter 0,$(NOFORTRAN)))
	if [ ! -a $< ]; then
	-wget http://www.netlib.org/lapack/timing/timing.tgz;
	fi
endif

lapack-timing : large.tgz timing.tgz
ifeq ($(NOFORTRAN), $(filter 0,$(NOFORTRAN)))
	(cd $(NETLIB_LAPACK_DIR); $(TAR) zxf ../timing.tgz TIMING)
	(cd $(NETLIB_LAPACK_DIR)/TIMING; $(TAR) zxf ../../large.tgz )
	$(MAKE) -C $(NETLIB_LAPACK_DIR)/TIMING
endif


lapack-test :
	(cd $(NETLIB_LAPACK_DIR)/TESTING && rm -f x* *.out)
	$(MAKE) -j 1 -C $(NETLIB_LAPACK_DIR)/TESTING/EIG xeigtstc  xeigtstd  xeigtsts  xeigtstz 
	$(MAKE) -j 1 -C $(NETLIB_LAPACK_DIR)/TESTING/LIN xlintstc  xlintstd  xlintstds  xlintstrfd  xlintstrfz  xlintsts  xlintstz  xlintstzc xlintstrfs xlintstrfc
ifneq ($(CROSS), 1)
	( cd $(NETLIB_LAPACK_DIR)/INSTALL; make all; ./testlsame; ./testslamch; ./testdlamch; \
        ./testsecond; ./testdsecnd; ./testieee; ./testversion )
	(cd $(NETLIB_LAPACK_DIR); ./lapack_testing.py -r )
endif

lapack-runtest:
	( cd $(NETLIB_LAPACK_DIR)/INSTALL; ./testlsame; ./testslamch; ./testdlamch; \
        ./testsecond; ./testdsecnd; ./testieee; ./testversion )
	(cd $(NETLIB_LAPACK_DIR); ./lapack_testing.py -r )


blas-test:
	(cd $(NETLIB_LAPACK_DIR)/BLAS/TESTING && rm -f x* *.out)
	$(MAKE) -j 1 -C $(NETLIB_LAPACK_DIR) blas_testing
	(cd $(NETLIB_LAPACK_DIR)/BLAS/TESTING && cat *.out)


dummy :

install :
	$(MAKE) -f Makefile.install install

clean ::
	@for d in $(SUBDIRS_ALL) ; \
	do if test -d $$d; then \
	  $(MAKE) -C $$d $(@F) || exit 1 ; \
	fi; \
	done
#ifdef DYNAMIC_ARCH
	@$(MAKE) -C kernel clean
#endif
	@$(MAKE) -C reference clean
	@rm -f *.$(LIBSUFFIX) *.so *~ *.exe getarch getarch_2nd *.dll *.lib *.$(SUFFIX) *.dwf $(LIBPREFIX).$(LIBSUFFIX) $(LIBPREFIX)_p.$(LIBSUFFIX) $(LIBPREFIX).so.$(MAJOR_VERSION) *.lnk myconfig.h
ifeq ($(OSNAME), Darwin)
	@rm -rf getarch.dSYM getarch_2nd.dSYM
endif
	@rm -f Makefile.conf config.h Makefile_kernel.conf config_kernel.h st* *.dylib
	@touch $(NETLIB_LAPACK_DIR)/make.inc
	@$(MAKE) -C $(NETLIB_LAPACK_DIR) clean
	@rm -f $(NETLIB_LAPACK_DIR)/make.inc $(NETLIB_LAPACK_DIR)/lapacke/include/lapacke_mangling.h
	@$(MAKE) -C relapack clean
	@rm -f *.grd Makefile.conf_last config_last.h
	@(cd $(NETLIB_LAPACK_DIR)/TESTING && rm -f x* *.out testing_results.txt)
	@echo Done.

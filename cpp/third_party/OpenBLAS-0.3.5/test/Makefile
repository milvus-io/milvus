TOPDIR	= ..
include ../Makefile.system

all :: level1 level2 level3

level1 : sblat1 dblat1 cblat1 zblat1
ifndef CROSS
	OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 ./sblat1
	OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 ./dblat1
	OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 ./cblat1
	OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 ./zblat1
ifdef SMP
ifeq ($(USE_OPENMP), 1)
	OMP_NUM_THREADS=2 ./sblat1
	OMP_NUM_THREADS=2 ./dblat1
	OMP_NUM_THREADS=2 ./cblat1
	OMP_NUM_THREADS=2 ./zblat1
else
	OPENBLAS_NUM_THREADS=2 ./sblat1
	OPENBLAS_NUM_THREADS=2 ./dblat1
	OPENBLAS_NUM_THREADS=2 ./cblat1
	OPENBLAS_NUM_THREADS=2 ./zblat1
endif
endif
endif

level2 : sblat2 dblat2 cblat2 zblat2
ifndef CROSS
	rm -f ?BLAT2.SUMM
	OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 ./sblat2 < ./sblat2.dat
	@$(GREP) -q FATAL SBLAT2.SUMM && cat SBLAT2.SUMM || exit 0
	OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 ./dblat2 < ./dblat2.dat
	@$(GREP) -q FATAL DBLAT2.SUMM && cat DBLAT2.SUMM || exit 0
	OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 ./cblat2 < ./cblat2.dat
	@$(GREP) -q FATAL CBLAT2.SUMM && cat CBLAT2.SUMM || exit 0
	OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 ./zblat2 < ./zblat2.dat
	@$(GREP) -q FATAL ZBLAT2.SUMM && cat ZBLAT2.SUMM || exit 0
ifdef SMP
	rm -f ?BLAT2.SUMM
ifeq ($(USE_OPENMP), 1)
	OMP_NUM_THREADS=2 ./sblat2 < ./sblat2.dat
	@$(GREP) -q FATAL SBLAT2.SUMM && cat SBLAT2.SUMM || exit 0
	OMP_NUM_THREADS=2 ./dblat2 < ./dblat2.dat
	@$(GREP) -q FATAL DBLAT2.SUMM && cat DBLAT2.SUMM || exit 0
	OMP_NUM_THREADS=2 ./cblat2 < ./cblat2.dat
	@$(GREP) -q FATAL CBLAT2.SUMM && cat CBLAT2.SUMM || exit 0
	OMP_NUM_THREADS=2 ./zblat2 < ./zblat2.dat
	@$(GREP) -q FATAL ZBLAT2.SUMM && cat ZBLAT2.SUMM || exit 0
else
	OPENBLAS_NUM_THREADS=2 ./sblat2 < ./sblat2.dat
	@$(GREP) -q FATAL SBLAT2.SUMM && cat SBLAT2.SUMM || exit 0
	OPENBLAS_NUM_THREADS=2 ./dblat2 < ./dblat2.dat
	@$(GREP) -q FATAL DBLAT2.SUMM && cat DBLAT2.SUMM || exit 0
	OPENBLAS_NUM_THREADS=2 ./cblat2 < ./cblat2.dat
	@$(GREP) -q FATAL CBLAT2.SUMM && cat CBLAT2.SUMM || exit 0
	OPENBLAS_NUM_THREADS=2 ./zblat2 < ./zblat2.dat
	@$(GREP) -q FATAL ZBLAT2.SUMM && cat ZBLAT2.SUMM || exit 0
endif
endif
endif

level3 : sblat3 dblat3 cblat3 zblat3
ifndef CROSS
	rm -f ?BLAT3.SUMM
	OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 ./sblat3 < ./sblat3.dat
	@$(GREP) -q FATAL SBLAT3.SUMM && cat SBLAT3.SUMM || exit 0
	OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 ./dblat3 < ./dblat3.dat
	@$(GREP) -q FATAL DBLAT3.SUMM && cat DBLAT3.SUMM || exit 0
	OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 ./cblat3 < ./cblat3.dat
	@$(GREP) -q FATAL CBLAT3.SUMM && cat CBLAT3.SUMM || exit 0
	OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 ./zblat3 < ./zblat3.dat
	@$(GREP) -q FATAL ZBLAT3.SUMM && cat ZBLAT3.SUMM || exit 0
ifdef SMP
	rm -f ?BLAT3.SUMM
ifeq ($(USE_OPENMP), 1)
	OMP_NUM_THREADS=2 ./sblat3 < ./sblat3.dat
	@$(GREP) -q FATAL SBLAT3.SUMM && cat SBLAT3.SUMM || exit 0
	OMP_NUM_THREADS=2 ./dblat3 < ./dblat3.dat
	@$(GREP) -q FATAL DBLAT3.SUMM && cat DBLAT3.SUMM || exit 0
	OMP_NUM_THREADS=2 ./cblat3 < ./cblat3.dat
	@$(GREP) -q FATAL CBLAT3.SUMM && cat CBLAT3.SUMM || exit 0
	OMP_NUM_THREADS=2 ./zblat3 < ./zblat3.dat
	@$(GREP) -q FATAL ZBLAT3.SUMM && cat ZBLAT3.SUMM || exit 0
else
	OPENBLAS_NUM_THREADS=2 ./sblat3 < ./sblat3.dat
	@$(GREP) -q FATAL SBLAT3.SUMM && cat SBLAT3.SUMM || exit 0
	OPENBLAS_NUM_THREADS=2 ./dblat3 < ./dblat3.dat
	@$(GREP) -q FATAL DBLAT3.SUMM && cat DBLAT3.SUMM || exit 0
	OPENBLAS_NUM_THREADS=2 ./cblat3 < ./cblat3.dat
	@$(GREP) -q FATAL CBLAT3.SUMM && cat CBLAT3.SUMM || exit 0
	OPENBLAS_NUM_THREADS=2 ./zblat3 < ./zblat3.dat
	@$(GREP) -q FATAL ZBLAT3.SUMM && cat ZBLAT3.SUMM || exit 0
endif
endif
endif


level3_3m : zblat3_3m cblat3_3m
ifndef CROSS
	rm -f ?BLAT3_3M.SUMM
	OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 ./cblat3_3m < ./cblat3_3m.dat
	@$(GREP) -q FATAL CBLAT3_3M.SUMM && cat CBLAT3_3M.SUMM || exit 0
	OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 ./zblat3_3m < ./zblat3_3m.dat
	@$(GREP) -q FATAL ZBLAT3_3M.SUMM && cat ZBLAT3_3M.SUMM || exit 0
ifdef SMP
	rm -f ?BLAT3_3M.SUMM
ifeq ($(USE_OPENMP), 1)
	OMP_NUM_THREADS=2 ./cblat3_3m < ./cblat3_3m.dat
	@$(GREP) -q FATAL CBLAT3_3M.SUMM && cat CBLAT3_3M.SUMM || exit 0
	OMP_NUM_THREADS=2 ./zblat3_3m < ./zblat3_3m.dat
	@$(GREP) -q FATAL ZBLAT3_3M.SUMM && cat ZBLAT3_3M.SUMM || exit 0
else
	OPENBLAS_NUM_THREADS=2 ./cblat3_3m < ./cblat3_3m.dat
	@$(GREP) -q FATAL CBLAT3_3M.SUMM && cat CBLAT3_3M.SUMM || exit 0
	OPENBLAS_NUM_THREADS=2 ./zblat3_3m < ./zblat3_3m.dat
	@$(GREP) -q FATAL ZBLAT3_3M.SUMM && cat ZBLAT3_3M.SUMM || exit 0
endif
endif
endif




FLDFLAGS = $(FFLAGS:-fPIC=) $(LDFLAGS)
ifeq ($(USE_OPENMP), 1)
ifeq ($(F_COMPILER), GFORTRAN)
ifeq ($(C_COMPILER), CLANG)
CEXTRALIB = -lomp
endif
endif
endif

sblat1 : sblat1.$(SUFFIX) ../$(LIBNAME)
	$(FC) $(FLDFLAGS) -o sblat1 sblat1.$(SUFFIX) ../$(LIBNAME) $(EXTRALIB) $(CEXTRALIB)

dblat1 : dblat1.$(SUFFIX) ../$(LIBNAME)
	$(FC) $(FLDFLAGS) -o dblat1 dblat1.$(SUFFIX) ../$(LIBNAME) $(EXTRALIB) $(CEXTRALIB)

qblat1 : qblat1.$(SUFFIX) ../$(LIBNAME)
	$(FC) $(FLDFLAGS) -o qblat1 qblat1.$(SUFFIX) ../$(LIBNAME) $(EXTRALIB) $(CEXTRALIB)

cblat1 : cblat1.$(SUFFIX) ../$(LIBNAME)
	$(FC) $(FLDFLAGS) -o cblat1 cblat1.$(SUFFIX) ../$(LIBNAME) $(EXTRALIB) $(CEXTRALIB)

zblat1 : zblat1.$(SUFFIX) ../$(LIBNAME)
	$(FC) $(FLDFLAGS) -o zblat1 zblat1.$(SUFFIX) ../$(LIBNAME) $(EXTRALIB) $(CEXTRALIB)

sblat2 : sblat2.$(SUFFIX) ../$(LIBNAME)
	$(FC) $(FLDFLAGS) -o sblat2 sblat2.$(SUFFIX) ../$(LIBNAME) $(EXTRALIB) $(CEXTRALIB)

dblat2 : dblat2.$(SUFFIX) ../$(LIBNAME)
	$(FC) $(FLDFLAGS) -o dblat2 dblat2.$(SUFFIX) ../$(LIBNAME) $(EXTRALIB) $(CEXTRALIB)

cblat2 : cblat2.$(SUFFIX) ../$(LIBNAME)
	$(FC) $(FLDFLAGS) -o cblat2 cblat2.$(SUFFIX) ../$(LIBNAME) $(EXTRALIB) $(CEXTRALIB)

zblat2 : zblat2.$(SUFFIX) ../$(LIBNAME)
	$(FC) $(FLDFLAGS) -o zblat2 zblat2.$(SUFFIX) ../$(LIBNAME) $(EXTRALIB) $(CEXTRALIB)

sblat3 : sblat3.$(SUFFIX) ../$(LIBNAME)
	$(FC) $(FLDFLAGS) -o sblat3 sblat3.$(SUFFIX) ../$(LIBNAME) $(EXTRALIB) $(CEXTRALIB)

dblat3 : dblat3.$(SUFFIX) ../$(LIBNAME)
	$(FC) $(FLDFLAGS) -o dblat3 dblat3.$(SUFFIX) ../$(LIBNAME) $(EXTRALIB) $(CEXTRALIB)

cblat3 : cblat3.$(SUFFIX) ../$(LIBNAME)
	$(FC) $(FLDFLAGS) -o cblat3 cblat3.$(SUFFIX) ../$(LIBNAME) $(EXTRALIB) $(CEXTRALIB)

zblat3 : zblat3.$(SUFFIX) ../$(LIBNAME)
	$(FC) $(FLDFLAGS) -o zblat3 zblat3.$(SUFFIX) ../$(LIBNAME) $(EXTRALIB) $(CEXTRALIB)

cblat3_3m : cblat3_3m.$(SUFFIX) ../$(LIBNAME)
	$(FC) $(FLDFLAGS) -o cblat3_3m cblat3_3m.$(SUFFIX) ../$(LIBNAME) $(EXTRALIB) $(CEXTRALIB)

zblat3_3m : zblat3_3m.$(SUFFIX) ../$(LIBNAME)
	$(FC) $(FLDFLAGS) -o zblat3_3m zblat3_3m.$(SUFFIX) ../$(LIBNAME) $(EXTRALIB) $(CEXTRALIB)




clean:
	@rm -f *.$(SUFFIX) *.$(PSUFFIX) gmon.$(SUFFIX)ut *.SUMM *.cxml *.exe *.pdb *.dwf \
	sblat1 dblat1 cblat1 zblat1 \
	sblat2 dblat2 cblat2 zblat2 \
	sblat3 dblat3 cblat3 zblat3 \
	sblat1p dblat1p cblat1p zblat1p \
	sblat2p dblat2p cblat2p zblat2p \
	sblat3p dblat3p cblat3p zblat3p \
	zblat3_3m zblat3_3mp \
	cblat3_3m cblat3_3mp \
	*.stackdump *.dll

libs:

prof:

quick :
	$(MAKE) -C $(TOPDIR) libs

# include ../Makefile.tail

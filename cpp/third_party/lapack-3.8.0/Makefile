#
#  Top Level Makefile for LAPACK
#  Version 3.4.1
#  April 2012
#

include make.inc

all: lapack_install lib blas_testing lapack_testing

lib: lapacklib tmglib
#lib: blaslib variants lapacklib tmglib

blaslib:
	$(MAKE) -C BLAS

cblaslib:
	$(MAKE) -C CBLAS

lapacklib:
	$(MAKE) -C SRC

lapackelib:
	$(MAKE) -C LAPACKE

tmglib:
	$(MAKE) -C TESTING/MATGEN

variants:
	$(MAKE) -C SRC/VARIANTS

lapack_install:
	$(MAKE) -C INSTALL run

blas_testing: blaslib
	$(MAKE) -C BLAS blas_testing

cblas_testing: cblaslib blaslib
	$(MAKE) -C CBLAS cblas_testing

lapack_testing: tmglib lapacklib blaslib
	$(MAKE) -C TESTING/LIN cleanexe
	$(MAKE) -C TESTING
	./lapack_testing.py

variants_testing: tmglib variants lapacklib blaslib
	$(MAKE) -C TESTING/LIN cleanexe
	$(MAKE) -C TESTING/LIN VARLIB='SRC/VARIANTS/cholrl.a'
	$(MAKE) -C TESTING stest.out && mv TESTING/stest.out TESTING/stest_cholrl.out
	$(MAKE) -C TESTING dtest.out && mv TESTING/dtest.out TESTING/dtest_cholrl.out
	$(MAKE) -C TESTING ctest.out && mv TESTING/ctest.out TESTING/ctest_cholrl.out
	$(MAKE) -C TESTING ztest.out && mv TESTING/ztest.out TESTING/ztest_cholrl.out
	$(MAKE) -C TESTING/LIN cleanexe
	$(MAKE) -C TESTING/LIN VARLIB='SRC/VARIANTS/choltop.a'
	$(MAKE) -C TESTING stest.out && mv TESTING/stest.out TESTING/stest_choltop.out
	$(MAKE) -C TESTING dtest.out && mv TESTING/dtest.out TESTING/dtest_choltop.out
	$(MAKE) -C TESTING ctest.out && mv TESTING/ctest.out TESTING/ctest_choltop.out
	$(MAKE) -C TESTING ztest.out && mv TESTING/ztest.out TESTING/ztest_choltop.out
	$(MAKE) -C TESTING/LIN cleanexe
	$(MAKE) -C TESTING/LIN VARLIB='SRC/VARIANTS/lucr.a'
	$(MAKE) -C TESTING stest.out && mv TESTING/stest.out TESTING/stest_lucr.out
	$(MAKE) -C TESTING dtest.out && mv TESTING/dtest.out TESTING/dtest_lucr.out
	$(MAKE) -C TESTING ctest.out && mv TESTING/ctest.out TESTING/ctest_lucr.out
	$(MAKE) -C TESTING ztest.out && mv TESTING/ztest.out TESTING/ztest_lucr.out
	$(MAKE) -C TESTING/LIN cleanexe
	$(MAKE) -C TESTING/LIN VARLIB='SRC/VARIANTS/lull.a'
	$(MAKE) -C TESTING stest.out && mv TESTING/stest.out TESTING/stest_lull.out
	$(MAKE) -C TESTING dtest.out && mv TESTING/dtest.out TESTING/dtest_lull.out
	$(MAKE) -C TESTING ctest.out && mv TESTING/ctest.out TESTING/ctest_lull.out
	$(MAKE) -C TESTING ztest.out && mv TESTING/ztest.out TESTING/ztest_lull.out
	$(MAKE) -C TESTING/LIN cleanexe
	$(MAKE) -C TESTING/LIN VARLIB='SRC/VARIANTS/lurec.a'
	$(MAKE) -C TESTING stest.out && mv TESTING/stest.out TESTING/stest_lurec.out
	$(MAKE) -C TESTING dtest.out && mv TESTING/dtest.out TESTING/dtest_lurec.out
	$(MAKE) -C TESTING ctest.out && mv TESTING/ctest.out TESTING/ctest_lurec.out
	$(MAKE) -C TESTING ztest.out && mv TESTING/ztest.out TESTING/ztest_lurec.out
	$(MAKE) -C TESTING/LIN cleanexe
	$(MAKE) -C TESTING/LIN VARLIB='SRC/VARIANTS/qrll.a'
	$(MAKE) -C TESTING stest.out && mv TESTING/stest.out TESTING/stest_qrll.out
	$(MAKE) -C TESTING dtest.out && mv TESTING/dtest.out TESTING/dtest_qrll.out
	$(MAKE) -C TESTING ctest.out && mv TESTING/ctest.out TESTING/ctest_qrll.out
	$(MAKE) -C TESTING ztest.out && mv TESTING/ztest.out TESTING/ztest_qrll.out

cblas_example: cblaslib blaslib
	$(MAKE) -C CBLAS cblas_example

lapacke_example: lapackelib lapacklib blaslib
	$(MAKE) -C LAPACKE lapacke_example

html:
	@echo "LAPACK HTML PAGES GENERATION with Doxygen"
	doxygen DOCS/Doxyfile
	@echo "=================="
	@echo "LAPACK HTML PAGES GENERATED in DOCS/explore-html"
	@echo "Usage: open DOCS/explore-html/index.html"
	@echo "Online version available at http://www.netlib.org/lapack/explore-html/"
	@echo "=================="

man:
	@echo "LAPACK MAN PAGES GENERATION with Doxygen"
	doxygen DOCS/Doxyfile_man
	@echo "=================="
	@echo "LAPACK MAN PAGES GENERATED in DOCS/MAN"
	@echo "Set your MANPATH env variable accordingly"
	@echo "Usage: man dgetrf.f"
	@echo "=================="

clean:
	$(MAKE) -C INSTALL clean
	$(MAKE) -C BLAS clean
	$(MAKE) -C CBLAS clean
	$(MAKE) -C SRC clean
	$(MAKE) -C SRC/VARIANTS clean
	$(MAKE) -C TESTING clean
	$(MAKE) -C TESTING/MATGEN clean
	$(MAKE) -C TESTING/LIN clean
	$(MAKE) -C TESTING/EIG clean
	$(MAKE) -C LAPACKE clean
	rm -f *.a
cleanobj:
	$(MAKE) -C INSTALL cleanobj
	$(MAKE) -C BLAS cleanobj
	$(MAKE) -C CBLAS cleanobj
	$(MAKE) -C SRC cleanobj
	$(MAKE) -C SRC/VARIANTS cleanobj
	$(MAKE) -C TESTING/MATGEN cleanobj
	$(MAKE) -C TESTING/LIN cleanobj
	$(MAKE) -C TESTING/EIG cleanobj
	$(MAKE) -C LAPACKE cleanobj
cleanlib:
	$(MAKE) -C BLAS cleanlib
	$(MAKE) -C CBLAS cleanlib
	$(MAKE) -C SRC cleanlib
	$(MAKE) -C SRC/VARIANTS cleanlib
	$(MAKE) -C TESTING/MATGEN cleanlib
	$(MAKE) -C LAPACKE cleanlib
	rm -f *.a
cleanexe:
	$(MAKE) -C INSTALL cleanexe
	$(MAKE) -C BLAS cleanexe
	$(MAKE) -C CBLAS cleanexe
	$(MAKE) -C TESTING/LIN cleanexe
	$(MAKE) -C TESTING/EIG cleanexe
	$(MAKE) -C LAPACKE cleanexe
cleantest:
	$(MAKE) -C INSTALL cleantest
	$(MAKE) -C BLAS cleantest
	$(MAKE) -C CBLAS cleantest
	$(MAKE) -C TESTING cleantest

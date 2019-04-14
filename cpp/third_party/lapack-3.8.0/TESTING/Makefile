#######################################################################
#  This makefile runs the test programs for the linear equation routines
#  and the eigenvalue routines in LAPACK.  The test output files
#  are grouped as follows:
#
#       SLINTST,SEIGTST  -- Single precision real test routines
#       CLINTST,CEIGTST  -- Single precision complex test routines
#       DLINTST,DEIGTST  -- Double precision real test routines
#       ZLINTST,ZEIGTST  -- Double precision complex test routines
#
#  Test programs can be executed for all or some of the four different
#  precisions.  Enter 'make' followed by one or more of the data types
#  desired.
#  Some examples:
#       make single
#       make single complex
#       make single double complex complex16
#  Alternatively, the command
#       make
#  without any arguments runs all eight test programs.
#  The executable files are called:
#       xlintsts, xlintstd, xlintstc, and xlintstz for LIN
#       xeigtsts, xeigtstd, xeigtstc, and xeigtstz for EIG
#
#  To remove the output files after the tests have been run, enter
#       make cleantest
#
#  To re-run specific tests after a make, enter (for example):
#       'rm ssvd.out; make'  or:
#       'make ssvd.out' or:
#       'touch svd.in; make' (to re-run the single precision SVD tests.)
#
#       'rm *svd.out; make'  (to re-run all the SVD tests.)
#
#######################################################################

include ../make.inc

all: single complex double complex16 singleproto doubleproto complexproto complex16proto

SEIGTST= snep.out \
         ssep.out \
         sse2.out \
         ssvd.out \
         sec.out \
         sed.out \
         sgg.out \
         sgd.out \
         ssb.out \
         ssg.out \
         sbal.out \
         sbak.out \
         sgbal.out \
         sgbak.out \
         sbb.out \
         sglm.out \
         sgqr.out \
         sgsv.out \
         scsd.out \
         slse.out

CEIGTST= cnep.out \
         csep.out \
         cse2.out \
         csvd.out \
         cec.out \
         ced.out \
         cgg.out \
         cgd.out \
         csb.out \
         csg.out \
         cbal.out \
         cbak.out \
         cgbal.out \
         cgbak.out \
         cbb.out \
         cglm.out \
         cgqr.out \
         cgsv.out \
         ccsd.out \
         clse.out

DEIGTST= dnep.out \
         dsep.out \
         dse2.out \
         dsvd.out \
         dec.out \
         ded.out \
         dgg.out \
         dgd.out \
         dsb.out \
         dsg.out \
         dbal.out \
         dbak.out \
         dgbal.out \
         dgbak.out \
         dbb.out \
         dglm.out \
         dgqr.out \
         dgsv.out \
         dcsd.out \
         dlse.out

ZEIGTST= znep.out \
         zsep.out \
         zse2.out \
         zsvd.out \
         zec.out \
         zed.out \
         zgg.out \
         zgd.out \
         zsb.out \
         zsg.out \
         zbal.out \
         zbak.out \
         zgbal.out \
         zgbak.out \
         zbb.out \
         zglm.out \
         zgqr.out \
         zgsv.out \
         zcsd.out \
         zlse.out


SLINTST= stest.out

SLINTSTPROTO= stest_rfp.out

CLINTST= ctest.out

CLINTSTPROTO= ctest_rfp.out

DLINTST= dtest.out

DLINTSTPROTO= dstest.out dtest_rfp.out

ZLINTST= ztest.out

ZLINTSTPROTO= zctest.out ztest_rfp.out

single:         $(SLINTST) $(SEIGTST)
complex:        $(CLINTST) $(CEIGTST)
double:         $(DLINTST) $(DEIGTST)
complex16:      $(ZLINTST) $(ZEIGTST)
singleproto:    $(SLINTSTPROTO)
complexproto:   $(CLINTSTPROTO)
doubleproto:    $(DLINTSTPROTO)
complex16proto: $(ZLINTSTPROTO)

#
# ======== SINGLE LIN TESTS ===========================

stest.out: stest.in LIN/xlintsts
	@echo Testing REAL LAPACK linear equation routines
	./LIN/xlintsts < $< > $@ 2>&1
#
# ======== COMPLEX LIN TESTS ==========================

ctest.out: ctest.in LIN/xlintstc
	@echo Testing COMPLEX LAPACK linear equation routines
	./LIN/xlintstc < $< > $@ 2>&1
#
# ======== DOUBLE LIN TESTS ===========================

dtest.out: dtest.in LIN/xlintstd
	@echo Testing DOUBLE PRECISION LAPACK linear equation routines
	./LIN/xlintstd < $< > $@ 2>&1
#
# ======== COMPLEX16 LIN TESTS ========================

ztest.out: ztest.in LIN/xlintstz
	@echo Testing COMPLEX16 LAPACK linear equation routines
	./LIN/xlintstz < $< > $@ 2>&1
#
# ======== SINGLE-DOUBLE PROTO LIN TESTS ==============

dstest.out: dstest.in LIN/xlintstds
	@echo Testing SINGLE-DOUBLE PRECISION LAPACK prototype linear equation routines
	./LIN/xlintstds < $< > $@ 2>&1
#
# ======== COMPLEX-COMPLEX16 LIN TESTS ========================

zctest.out: zctest.in LIN/xlintstzc
	@echo Testing COMPLEX-COMPLEX16 LAPACK prototype linear equation routines
	./LIN/xlintstzc < $< > $@ 2>&1
#
# ======== SINGLE RFP LIN TESTS ========================

stest_rfp.out: stest_rfp.in LIN/xlintstrfs
	@echo Testing REAL LAPACK RFP prototype linear equation routines
	./LIN/xlintstrfs < $< > $@ 2>&1
#
# ======== COMPLEX16 RFP LIN TESTS ========================

dtest_rfp.out: dtest_rfp.in LIN/xlintstrfd
	@echo Testing DOUBLE PRECISION LAPACK RFP prototype linear equation routines
	./LIN/xlintstrfd < $< > $@ 2>&1
#
# ======== COMPLEX16 RFP LIN TESTS ========================

ctest_rfp.out: ctest_rfp.in LIN/xlintstrfc
	@echo Testing COMPLEX LAPACK RFP prototype linear equation routines
	./LIN/xlintstrfc < $< > $@ 2>&1
#
# ======== COMPLEX16 RFP LIN TESTS ========================

ztest_rfp.out: ztest_rfp.in LIN/xlintstrfz
	@echo Testing COMPLEX16 LAPACK RFP prototype linear equation routines
	./LIN/xlintstrfz < $< > $@ 2>&1
#
#
# ======== SINGLE EIG TESTS ===========================
#

snep.out: nep.in EIG/xeigtsts
	@echo NEP: Testing Nonsymmetric Eigenvalue Problem routines
	./EIG/xeigtsts < $< > $@ 2>&1

ssep.out: sep.in EIG/xeigtsts
	@echo SEP: Testing Symmetric Eigenvalue Problem routines
	./EIG/xeigtsts < $< > $@ 2>&1

sse2.out: se2.in EIG/xeigtsts
	@echo SEP: Testing Symmetric Eigenvalue Problem routines
	./EIG/xeigtsts < $< > $@ 2>&1

ssvd.out: svd.in EIG/xeigtsts
	@echo SVD: Testing Singular Value Decomposition routines
	./EIG/xeigtsts < $< > $@ 2>&1

sec.out: sec.in EIG/xeigtsts
	@echo SEC: Testing REAL Eigen Condition Routines
	./EIG/xeigtsts < $< > $@ 2>&1

sed.out: sed.in EIG/xeigtsts
	@echo SEV: Testing REAL Nonsymmetric Eigenvalue Driver
	./EIG/xeigtsts < $< > $@ 2>&1

sgg.out: sgg.in EIG/xeigtsts
	@echo SGG: Testing REAL Nonsymmetric Generalized Eigenvalue Problem routines
	./EIG/xeigtsts < $< > $@ 2>&1

sgd.out: sgd.in EIG/xeigtsts
	@echo SGD: Testing REAL Nonsymmetric Generalized Eigenvalue Problem driver routines
	./EIG/xeigtsts < $< > $@ 2>&1

ssb.out: ssb.in EIG/xeigtsts
	@echo SSB: Testing REAL Symmetric Eigenvalue Problem routines
	./EIG/xeigtsts < $< > $@ 2>&1

ssg.out: ssg.in EIG/xeigtsts
	@echo SSG: Testing REAL Symmetric Generalized Eigenvalue Problem routines
	./EIG/xeigtsts < $< > $@ 2>&1

sbal.out: sbal.in EIG/xeigtsts
	@echo SGEBAL: Testing the balancing of a REAL general matrix
	./EIG/xeigtsts < $< > $@ 2>&1

sbak.out: sbak.in EIG/xeigtsts
	@echo SGEBAK: Testing the back transformation of a REAL balanced matrix
	./EIG/xeigtsts < $< > $@ 2>&1

sgbal.out: sgbal.in EIG/xeigtsts
	@echo SGGBAL: Testing the balancing of a pair of REAL general matrices
	./EIG/xeigtsts < $< > $@ 2>&1

sgbak.out: sgbak.in EIG/xeigtsts
	@echo SGGBAK: Testing the back transformation of a pair of REAL balanced matrices
	./EIG/xeigtsts < $< > $@ 2>&1

sbb.out: sbb.in EIG/xeigtsts
	@echo SBB: Testing banded Singular Value Decomposition routines
	./EIG/xeigtsts < $< > $@ 2>&1

sglm.out: glm.in EIG/xeigtsts
	@echo GLM: Testing Generalized Linear Regression Model routines
	./EIG/xeigtsts < $< > $@ 2>&1

sgqr.out: gqr.in EIG/xeigtsts
	@echo GQR: Testing Generalized QR and RQ factorization routines
	./EIG/xeigtsts < $< > $@ 2>&1

sgsv.out: gsv.in EIG/xeigtsts
	@echo GSV: Testing Generalized Singular Value Decomposition routines
	./EIG/xeigtsts < $< > $@ 2>&1

scsd.out: csd.in EIG/xeigtsts
	@echo CSD: Testing CS Decomposition routines
	./EIG/xeigtsts < $< > $@ 2>&1

slse.out: lse.in EIG/xeigtsts
	@echo LSE: Testing Constrained Linear Least Squares routines
	./EIG/xeigtsts < $< > $@ 2>&1
#
# ======== COMPLEX EIG TESTS ===========================

cnep.out: nep.in EIG/xeigtstc
	@echo NEP: Testing Nonsymmetric Eigenvalue Problem routines
	./EIG/xeigtstc < $< > $@ 2>&1

csep.out: sep.in EIG/xeigtstc
	@echo SEP: Testing Symmetric Eigenvalue Problem routines
	./EIG/xeigtstc < $< > $@ 2>&1

cse2.out: se2.in EIG/xeigtstc
	@echo SEP: Testing Symmetric Eigenvalue Problem routines
	./EIG/xeigtstc < $< > $@ 2>&1

csvd.out: svd.in EIG/xeigtstc
	@echo SVD: Testing Singular Value Decomposition routines
	./EIG/xeigtstc < $< > $@ 2>&1

cec.out: cec.in EIG/xeigtstc
	@echo CEC: Testing COMPLEX Eigen Condition Routines
	./EIG/xeigtstc < $< > $@ 2>&1

ced.out: ced.in EIG/xeigtstc
	@echo CES: Testing COMPLEX Nonsymmetric Schur Form Driver
	./EIG/xeigtstc < $< > $@ 2>&1

cgg.out: cgg.in EIG/xeigtstc
	@echo CGG: Testing COMPLEX Nonsymmetric Generalized Eigenvalue Problem routines
	./EIG/xeigtstc < $< > $@ 2>&1

cgd.out: cgd.in EIG/xeigtstc
	@echo CGD: Testing COMPLEX Nonsymmetric Generalized Eigenvalue Problem driver routines
	./EIG/xeigtstc < $< > $@ 2>&1

csb.out: csb.in EIG/xeigtstc
	@echo CHB: Testing Hermitian Eigenvalue Problem routines
	./EIG/xeigtstc < $< > $@ 2>&1

csg.out: csg.in EIG/xeigtstc
	@echo CSG: Testing Symmetric Generalized Eigenvalue Problem routines
	./EIG/xeigtstc < $< > $@ 2>&1

cbal.out: cbal.in EIG/xeigtstc
	@echo CGEBAL: Testing the balancing of a COMPLEX general matrix
	./EIG/xeigtstc < $< > $@ 2>&1

cbak.out: cbak.in EIG/xeigtstc
	@echo CGEBAK: Testing the back transformation of a COMPLEX balanced matrix
	./EIG/xeigtstc < $< > $@ 2>&1

cgbal.out: cgbal.in EIG/xeigtstc
	@echo CGGBAL: Testing the balancing of a pair of COMPLEX general matrices
	./EIG/xeigtstc < $< > $@ 2>&1

cgbak.out: cgbak.in EIG/xeigtstc
	@echo CGGBAK: Testing the back transformation of a pair of COMPLEX balanced matrices
	./EIG/xeigtstc < $< > $@ 2>&1

cbb.out: cbb.in EIG/xeigtstc
	@echo CBB: Testing banded Singular Value Decomposition routines
	./EIG/xeigtstc < $< > $@ 2>&1

cglm.out: glm.in EIG/xeigtstc
	@echo GLM: Testing Generalized Linear Regression Model routines
	./EIG/xeigtstc < $< > $@ 2>&1

cgqr.out: gqr.in EIG/xeigtstc
	@echo GQR: Testing Generalized QR and RQ factorization routines
	./EIG/xeigtstc < $< > $@ 2>&1

cgsv.out: gsv.in EIG/xeigtstc
	@echo GSV: Testing Generalized Singular Value Decomposition routines
	./EIG/xeigtstc < $< > $@ 2>&1

ccsd.out: csd.in EIG/xeigtstc
	@echo CSD: Testing CS Decomposition routines
	./EIG/xeigtstc < $< > $@ 2>&1

clse.out: lse.in EIG/xeigtstc
	@echo LSE: Testing Constrained Linear Least Squares routines
	./EIG/xeigtstc < $< > $@ 2>&1
#
# ======== DOUBLE EIG TESTS ===========================

dnep.out: nep.in EIG/xeigtstd
	@echo NEP: Testing Nonsymmetric Eigenvalue Problem routines
	./EIG/xeigtstd < $< > $@ 2>&1

dsep.out: sep.in EIG/xeigtstd
	@echo SEP: Testing Symmetric Eigenvalue Problem routines
	./EIG/xeigtstd < $< > $@ 2>&1

dse2.out: se2.in EIG/xeigtstd
	@echo SEP: Testing Symmetric Eigenvalue Problem routines
	./EIG/xeigtstd < $< > $@ 2>&1

dsvd.out: svd.in EIG/xeigtstd
	@echo SVD: Testing Singular Value Decomposition routines
	./EIG/xeigtstd < $< > $@ 2>&1

dec.out: dec.in EIG/xeigtstd
	@echo DEC: Testing DOUBLE PRECISION Eigen Condition Routines
	./EIG/xeigtstd < $< > $@ 2>&1

ded.out: ded.in EIG/xeigtstd
	@echo DEV: Testing DOUBLE PRECISION Nonsymmetric Eigenvalue Driver
	./EIG/xeigtstd < $< > $@ 2>&1

dgg.out: dgg.in EIG/xeigtstd
	@echo DGG: Testing DOUBLE PRECISION Nonsymmetric Generalized Eigenvalue Problem routines
	./EIG/xeigtstd < $< > $@ 2>&1

dgd.out: dgd.in EIG/xeigtstd
	@echo DGD: Testing DOUBLE PRECISION Nonsymmetric Generalized Eigenvalue Problem driver routines
	./EIG/xeigtstd < $< > $@ 2>&1

dsb.out: dsb.in EIG/xeigtstd
	@echo DSB: Testing DOUBLE PRECISION Symmetric Eigenvalue Problem routines
	./EIG/xeigtstd < $< > $@ 2>&1

dsg.out: dsg.in EIG/xeigtstd
	@echo DSG: Testing DOUBLE PRECISION Symmetric Generalized Eigenvalue Problem routines
	./EIG/xeigtstd < $< > $@ 2>&1

dbal.out: dbal.in EIG/xeigtstd
	@echo DGEBAL: Testing the balancing of a DOUBLE PRECISION general matrix
	./EIG/xeigtstd < $< > $@ 2>&1

dbak.out: dbak.in EIG/xeigtstd
	@echo DGEBAK: Testing the back transformation of a DOUBLE PRECISION balanced matrix
	./EIG/xeigtstd < $< > $@ 2>&1

dgbal.out: dgbal.in EIG/xeigtstd
	@echo DGGBAL: Testing the balancing of a pair of DOUBLE PRECISION general matrices
	./EIG/xeigtstd < $< > $@ 2>&1

dgbak.out: dgbak.in EIG/xeigtstd
	@echo DGGBAK: Testing the back transformation of a pair of DOUBLE PRECISION balanced matrices
	./EIG/xeigtstd < $< > $@ 2>&1

dbb.out: dbb.in EIG/xeigtstd
	@echo DBB: Testing banded Singular Value Decomposition routines
	./EIG/xeigtstd < $< > $@ 2>&1

dglm.out: glm.in EIG/xeigtstd
	@echo GLM: Testing Generalized Linear Regression Model routines
	./EIG/xeigtstd < $< > $@ 2>&1

dgqr.out: gqr.in EIG/xeigtstd
	@echo GQR: Testing Generalized QR and RQ factorization routines
	./EIG/xeigtstd < $< > $@ 2>&1

dgsv.out: gsv.in EIG/xeigtstd
	@echo GSV: Testing Generalized Singular Value Decomposition routines
	./EIG/xeigtstd < $< > $@ 2>&1

dcsd.out: csd.in EIG/xeigtstd
	@echo CSD: Testing CS Decomposition routines
	./EIG/xeigtstd < $< > $@ 2>&1

dlse.out: lse.in EIG/xeigtstd
	@echo LSE: Testing Constrained Linear Least Squares routines
	./EIG/xeigtstd < $< > $@ 2>&1
#
# ======== COMPLEX16 EIG TESTS ===========================

znep.out: nep.in EIG/xeigtstz
	@echo NEP: Testing Nonsymmetric Eigenvalue Problem routines
	./EIG/xeigtstz < $< > $@ 2>&1

zsep.out: sep.in EIG/xeigtstz
	@echo SEP: Testing Symmetric Eigenvalue Problem routines
	./EIG/xeigtstz < $< > $@ 2>&1

zse2.out: se2.in EIG/xeigtstz
	@echo SEP: Testing Symmetric Eigenvalue Problem routines
	./EIG/xeigtstz < $< > $@ 2>&1

zsvd.out: svd.in EIG/xeigtstz
	@echo SVD: Testing Singular Value Decomposition routines
	./EIG/xeigtstz < $< > $@ 2>&1

zec.out: zec.in EIG/xeigtstz
	@echo ZEC: Testing COMPLEX16 Eigen Condition Routines
	./EIG/xeigtstz < $< > $@ 2>&1

zed.out: zed.in EIG/xeigtstz
	@echo ZES: Testing COMPLEX16 Nonsymmetric Schur Form Driver
	./EIG/xeigtstz < $< > $@ 2>&1

zgg.out: zgg.in EIG/xeigtstz
	@echo ZGG: Testing COMPLEX16 Nonsymmetric Generalized Eigenvalue Problem routines
	./EIG/xeigtstz < $< > $@ 2>&1

zgd.out: zgd.in EIG/xeigtstz
	@echo ZGD: Testing COMPLEX16 Nonsymmetric Generalized Eigenvalue Problem driver routines
	./EIG/xeigtstz < $< > $@ 2>&1

zsb.out: zsb.in EIG/xeigtstz
	@echo ZHB: Testing Hermitian Eigenvalue Problem routines
	./EIG/xeigtstz < $< > $@ 2>&1

zsg.out: zsg.in EIG/xeigtstz
	@echo ZSG: Testing Symmetric Generalized Eigenvalue Problem routines
	./EIG/xeigtstz < $< > $@ 2>&1

zbal.out: zbal.in EIG/xeigtstz
	@echo ZGEBAL: Testing the balancing of a COMPLEX16 general matrix
	./EIG/xeigtstz < $< > $@ 2>&1

zbak.out: zbak.in EIG/xeigtstz
	@echo ZGEBAK: Testing the back transformation of a COMPLEX16 balanced matrix
	./EIG/xeigtstz < $< > $@ 2>&1

zgbal.out: zgbal.in EIG/xeigtstz
	@echo ZGGBAL: Testing the balancing of a pair of COMPLEX general matrices
	./EIG/xeigtstz < $< > $@ 2>&1

zgbak.out: zgbak.in EIG/xeigtstz
	@echo ZGGBAK: Testing the back transformation of a pair of COMPLEX16 balanced matrices
	./EIG/xeigtstz < $< > $@ 2>&1

zbb.out: zbb.in EIG/xeigtstz
	@echo ZBB: Testing banded Singular Value Decomposition routines
	./EIG/xeigtstz < $< > $@ 2>&1

zglm.out: glm.in EIG/xeigtstz
	@echo GLM: Testing Generalized Linear Regression Model routines
	./EIG/xeigtstz < $< > $@ 2>&1

zgqr.out: gqr.in EIG/xeigtstz
	@echo GQR: Testing Generalized QR and RQ factorization routines
	./EIG/xeigtstz < $< > $@ 2>&1

zgsv.out: gsv.in EIG/xeigtstz
	@echo GSV: Testing Generalized Singular Value Decomposition routines
	./EIG/xeigtstz < $< > $@ 2>&1

zcsd.out: csd.in EIG/xeigtstz
	@echo CSD: Testing CS Decomposition routines
	./EIG/xeigtstz < $< > $@ 2>&1

zlse.out: lse.in EIG/xeigtstz
	@echo LSE: Testing Constrained Linear Least Squares routines
	./EIG/xeigtstz < $< > $@ 2>&1
# ==============================================================================

LIN/xlintsts: $(FRCLIN) $(FRC)
	$(MAKE) -C LIN xlintsts

LIN/xlintstc: $(FRCLIN) $(FRC)
	$(MAKE) -C LIN xlintstc

LIN/xlintstd: $(FRCLIN) $(FRC)
	$(MAKE) -C LIN xlintstd

LIN/xlintstz: $(FRCLIN) $(FRC)
	$(MAKE) -C LIN xlintstz

LIN/xlintstrfs: $(FRCLIN) $(FRC)
	$(MAKE) -C LIN xlintstrfs

LIN/xlintstrfc: $(FRCLIN) $(FRC)
	$(MAKE) -C LIN xlintstrfc

LIN/xlintstrfd: $(FRCLIN) $(FRC)
	$(MAKE) -C LIN xlintstrfd

LIN/xlintstrfz: $(FRCLIN) $(FRC)
	$(MAKE) -C LIN xlintstrfz

LIN/xlintstds: $(FRCLIN) $(FRC)
	$(MAKE) -C LIN xlintstds

LIN/xlintstzc: $(FRCLIN) $(FRC)
	$(MAKE) -C LIN xlintstzc

EIG/xeigtsts: $(FRCEIG) $(FRC)
	$(MAKE) -C EIG xeigtsts

EIG/xeigtstc: $(FRCEIG) $(FRC)
	$(MAKE) -C EIG xeigtstc

EIG/xeigtstd: $(FRCEIG) $(FRC)
	$(MAKE) -C EIG xeigtstd

EIG/xeigtstz: $(FRCEIG) $(FRC)
	$(MAKE) -C EIG xeigtstz

clean: cleantest
cleantest:
	rm -f *.out core

FRCLIN:
	@FRCLIN=$(FRCLIN)

FRCEIG:
	@FRCEIG=$(FRCEIG)

FRC:
	@FRC=$(FRC)

.NOTPARALLEL:

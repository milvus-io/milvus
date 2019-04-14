#
# The Makefile compiles c wrappers and testers for CBLAS.
#

include ../../make.inc

# Archive files necessary to compile
LIB = $(CBLASLIB) $(BLASLIB)

# Object files for single precision real
stestl1o = c_sblas1.o
stestl2o = c_sblas2.o c_s2chke.o auxiliary.o c_xerbla.o
stestl3o = c_sblas3.o c_s3chke.o auxiliary.o c_xerbla.o

# Object files for double precision real
dtestl1o = c_dblas1.o
dtestl2o = c_dblas2.o c_d2chke.o auxiliary.o c_xerbla.o
dtestl3o = c_dblas3.o c_d3chke.o auxiliary.o c_xerbla.o

# Object files for single precision complex
ctestl1o = c_cblas1.o
ctestl2o = c_cblas2.o c_c2chke.o auxiliary.o c_xerbla.o
ctestl3o = c_cblas3.o c_c3chke.o auxiliary.o c_xerbla.o

# Object files for double precision complex
ztestl1o = c_zblas1.o
ztestl2o = c_zblas2.o c_z2chke.o auxiliary.o c_xerbla.o
ztestl3o = c_zblas3.o c_z3chke.o auxiliary.o c_xerbla.o

all: all1 all2 all3
all1: xscblat1 xdcblat1 xccblat1 xzcblat1
all2: xscblat2 xdcblat2 xccblat2 xzcblat2
all3: xscblat3 xdcblat3 xccblat3 xzcblat3

#
# Compile each precision
#

# Single real
xscblat1: c_sblat1.o $(stestl1o) $(LIB)
	$(LOADER) $(LOADOPTS) -o $@ $^
xscblat2: c_sblat2.o $(stestl2o) $(LIB)
	$(LOADER) $(LOADOPTS) -o $@ $^
xscblat3: c_sblat3.o $(stestl3o) $(LIB)
	$(LOADER) $(LOADOPTS) -o $@ $^
# Double real
xdcblat1: c_dblat1.o $(dtestl1o) $(LIB)
	$(LOADER) $(LOADOPTS) -o $@ $^
xdcblat2: c_dblat2.o $(dtestl2o) $(LIB)
	$(LOADER) $(LOADOPTS) -o $@ $^
xdcblat3: c_dblat3.o $(dtestl3o) $(LIB)
	$(LOADER) $(LOADOPTS) -o $@ $^

# Single complex
xccblat1: c_cblat1.o $(ctestl1o) $(LIB)
	$(LOADER) $(LOADOPTS) -o $@ $^
xccblat2: c_cblat2.o $(ctestl2o) $(LIB)
	$(LOADER) $(LOADOPTS) -o $@ $^
xccblat3: c_cblat3.o $(ctestl3o) $(LIB)
	$(LOADER) $(LOADOPTS) -o $@ $^

# Double complex
xzcblat1: c_zblat1.o $(ztestl1o) $(LIB)
	$(LOADER) $(LOADOPTS) -o $@ $^
xzcblat2: c_zblat2.o $(ztestl2o) $(LIB)
	$(LOADER) $(LOADOPTS) -o $@ $^
xzcblat3: c_zblat3.o $(ztestl3o) $(LIB)
	$(LOADER) $(LOADOPTS) -o $@ $^


# RUN TESTS
run: all
	@echo "--> TESTING CBLAS 1 - SINGLE PRECISION REAL <--"
	@./xscblat1 > stest1.out
	@echo "--> TESTING CBLAS 1 - DOUBLE PRECISION REAL <--"
	@./xdcblat1 > dtest1.out
	@echo "--> TESTING CBLAS 1 - SINGLE PRECISION COMPLEX <--"
	@./xccblat1 > ctest1.out
	@echo "--> TESTING CBLAS 1 - DOUBLE PRECISION COMPLEX <--"
	@./xzcblat1 > ztest1.out
	@echo "--> TESTING CBLAS 2 - SINGLE PRECISION REAL <--"
	@./xscblat2 < sin2 > stest2.out
	@echo "--> TESTING CBLAS 2 - DOUBLE PRECISION REAL <--"
	@./xdcblat2 < din2 > dtest2.out
	@echo "--> TESTING CBLAS 2 - SINGLE PRECISION COMPLEX <--"
	@./xccblat2 < cin2 > ctest2.out
	@echo "--> TESTING CBLAS 2 - DOUBLE PRECISION COMPLEX <--"
	@./xzcblat2 < zin2 > ztest2.out
	@echo "--> TESTING CBLAS 3 - SINGLE PRECISION REAL <--"
	@./xscblat3 < sin3 > stest3.out
	@echo "--> TESTING CBLAS 3 - DOUBLE PRECISION REAL <--"
	@./xdcblat3 < din3 > dtest3.out
	@echo "--> TESTING CBLAS 3 - SINGLE PRECISION COMPLEX <--"
	@./xccblat3 < cin3 > ctest3.out
	@echo "--> TESTING CBLAS 3 - DOUBLE PRECISION COMPLEX <--"
	@./xzcblat3 < zin3 > ztest3.out

clean: cleanobj cleanexe cleantest
cleanobj:
	rm -f *.o
cleanexe:
	rm -f x*
cleantest:
	rm -f *.out core

.SUFFIXES: .o .f .c
.c.o:
	$(CC) $(CFLAGS) -I../include -c -o $@ $<
.f.o:
	$(FORTRAN) $(OPTS) -c -o $@ $<

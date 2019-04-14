include ../make.inc

all: blas

blas:
	$(MAKE) -C SRC

blas_testing: blas
	$(MAKE) -C TESTING run

clean:
	$(MAKE) -C SRC clean
	$(MAKE) -C TESTING clean
cleanobj:
	$(MAKE) -C SRC cleanobj
	$(MAKE) -C TESTING cleanobj
cleanlib:
	$(MAKE) -C SRC cleanlib
cleanexe:
	$(MAKE) -C TESTING cleanexe
cleantest:
	$(MAKE) -C TESTING cleantest

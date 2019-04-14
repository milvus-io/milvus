include ../make.inc

all: cblas

cblas: include/cblas_mangling.h
	$(MAKE) -C src

include/cblas_mangling.h: include/cblas_mangling_with_flags.h.in
	cp $< $@

cblas_testing: cblas
	$(MAKE) -C testing run

cblas_example: cblas
	$(MAKE) -C examples

clean:
	$(MAKE) -C src clean
	$(MAKE) -C testing clean
	$(MAKE) -C examples clean
cleanobj:
	$(MAKE) -C src cleanobj
	$(MAKE) -C testing cleanobj
	$(MAKE) -C examples cleanobj
cleanlib:
	$(MAKE) -C src cleanlib
cleanexe:
	$(MAKE) -C testing cleanexe
	$(MAKE) -C examples cleanexe
cleantest:
	$(MAKE) -C testing cleantest

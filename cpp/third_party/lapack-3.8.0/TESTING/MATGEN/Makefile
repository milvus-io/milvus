include ../../make.inc

#######################################################################
#  This is the makefile to create a library of the test matrix
#  generators used in LAPACK.  The files are organized as follows:
#
#     SCATGEN  -- Auxiliary routines called from single precision
#     DZATGEN  -- Auxiliary routines called from double precision
#     SMATGEN  -- Single precision real matrix generation routines
#     CMATGEN  -- Single precision complex matrix generation routines
#     DMATGEN  -- Double precision real matrix generation routines
#     ZMATGEN  -- Double precision complex matrix generation routines
#
#  The library can be set up to include routines for any combination
#  of the four precisions.  To create or add to the library, enter make
#  followed by one or more of the precisions desired.  Some examples:
#       make single
#       make single complex
#       make single double complex complex16
#  Alternatively, the command
#       make
#  without any arguments creates a library of all four precisions.
#  The library is called
#       tmglib.a
#  and is created at the LAPACK directory level.
#
#  To remove the object files after the library is created, enter
#       make cleanobj
#  On some systems, you can force the source files to be recompiled by
#  entering (for example)
#       make single FRC=FRC
#
#######################################################################

SCATGEN = slatm1.o slatm7.o slaran.o slarnd.o

SMATGEN = slatms.o slatme.o slatmr.o slatmt.o \
   slagge.o slagsy.o slakf2.o slarge.o slaror.o slarot.o slatm2.o \
   slatm3.o slatm5.o slatm6.o slahilb.o

CMATGEN = clatms.o clatme.o clatmr.o clatmt.o \
   clagge.o claghe.o clagsy.o clakf2.o clarge.o claror.o clarot.o \
   clatm1.o clarnd.o clatm2.o clatm3.o clatm5.o clatm6.o clahilb.o

DZATGEN = dlatm1.o dlatm7.o dlaran.o dlarnd.o

DMATGEN = dlatms.o dlatme.o dlatmr.o dlatmt.o \
   dlagge.o dlagsy.o dlakf2.o dlarge.o dlaror.o dlarot.o dlatm2.o \
   dlatm3.o dlatm5.o dlatm6.o dlahilb.o

ZMATGEN = zlatms.o zlatme.o zlatmr.o zlatmt.o \
   zlagge.o zlaghe.o zlagsy.o zlakf2.o zlarge.o zlaror.o zlarot.o \
   zlatm1.o zlarnd.o zlatm2.o zlatm3.o zlatm5.o zlatm6.o zlahilb.o

all: ../../$(TMGLIB)

ALLOBJ = $(SMATGEN) $(CMATGEN) $(SCATGEN) $(DMATGEN) $(ZMATGEN) \
	$(DZATGEN)

../../$(TMGLIB): $(ALLOBJ)
	$(ARCH) $(ARCHFLAGS) $@ $^
	$(RANLIB) $@

single: $(SMATGEN) $(SCATGEN)
	$(ARCH) $(ARCHFLAGS) ../../$(TMGLIB) $^
	$(RANLIB) ../../$(TMGLIB)

complex: $(CMATGEN) $(SCATGEN)
	$(ARCH) $(ARCHFLAGS) ../../$(TMGLIB) $^
	$(RANLIB) ../../$(TMGLIB)

double: $(DMATGEN) $(DZATGEN)
	$(ARCH) $(ARCHFLAGS) ../../$(TMGLIB) $^
	$(RANLIB) ../../$(TMGLIB)

complex16: $(ZMATGEN) $(DZATGEN)
	$(ARCH) $(ARCHFLAGS) ../../$(TMGLIB) $^
	$(RANLIB) ../../$(TMGLIB)

$(SCATGEN): $(FRC)
$(SMATGEN): $(FRC)
$(CMATGEN): $(FRC)
$(DZATGEN): $(FRC)
$(DMATGEN): $(FRC)
$(ZMATGEN): $(FRC)

FRC:
	@FRC=$(FRC)

clean: cleanobj cleanlib
cleanobj:
	rm -f *.o
cleanlib:
	rm -f ../../$(TMGLIB)

.f.o:
	$(FORTRAN) $(OPTS) -c -o $@ $<

slaran.o: slaran.f ; $(FORTRAN) $(NOOPT) -c -o $@ $<
dlaran.o: dlaran.f ; $(FORTRAN) $(NOOPT) -c -o $@ $<

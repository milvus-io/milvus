ifndef INCLUDED
TOPDIR = ../../..
include $(TOPDIR)/Makefile.system
endif

ifndef LASWP
LASWP	= ../generic/laswp_k.c
endif

ifndef ZLASWP
ZLASWP	= ../generic/zlaswp_k.c
endif

LASWP_DEPS  = ../generic/laswp_k_1.c ../generic/laswp_k_2.c \
	      ../generic/laswp_k_4.c ../generic/laswp_k_8.c

ZLASWP_DEPS = ../generic/zlaswp_k_1.c ../generic/zlaswp_k_2.c \
	      ../generic/zlaswp_k_4.c

include ../../../Makefile.tail

all:

../slaswp_plus.$(SUFFIX) : $(LASWP) $(LASWP_DEPS)
	$(CC) -c $(CFLAGS) -UDOUBLE -UMINUS $< -o ../$(@F)

../slaswp_minus.$(SUFFIX) : $(LASWP) $(LASWP_DEPS)
	$(CC) -c $(CFLAGS) -UDOUBLE -DMINUS $< -o ../$(@F)

../dlaswp_plus.$(SUFFIX) : $(LASWP) $(LASWP_DEPS)
	$(CC) -c $(CFLAGS) -DDOUBLE -UMINUS $< -o ../$(@F)

../dlaswp_minus.$(SUFFIX) : $(LASWP) $(LASWP_DEPS)
	$(CC) -c $(CFLAGS) -DDOUBLE -DMINUS $< -o ../$(@F)

../qlaswp_plus.$(SUFFIX) : $(LASWP) $(LASWP_DEPS)
	$(CC) -c $(CFLAGS) -DXDOUBLE -UMINUS $< -o ../$(@F)

../qlaswp_minus.$(SUFFIX) : $(LASWP) $(LASWP_DEPS)
	$(CC) -c $(CFLAGS) -DXDOUBLE -DMINUS $< -o ../$(@F)

../claswp_plus.$(SUFFIX) : $(ZLASWP) $(ZLASWP_DEPS)
	$(CC) -c $(CFLAGS) -UDOUBLE -UMINUS $< -o ../$(@F)

../claswp_minus.$(SUFFIX) : $(ZLASWP) $(ZLASWP_DEPS)
	$(CC) -c $(CFLAGS) -UDOUBLE -DMINUS $< -o ../$(@F)

../zlaswp_plus.$(SUFFIX) : $(ZLASWP) $(ZLASWP_DEPS)
	$(CC) -c $(CFLAGS) -DDOUBLE -UMINUS $< -o ../$(@F)

../zlaswp_minus.$(SUFFIX) : $(ZLASWP) $(ZLASWP_DEPS)
	$(CC) -c $(CFLAGS) -DDOUBLE -DMINUS $< -o ../$(@F)

../xlaswp_plus.$(SUFFIX) : $(ZLASWP) $(ZLASWP_DEPS)
	$(CC) -c $(CFLAGS) -DXDOUBLE -UMINUS $< -o ../$(@F)

../xlaswp_minus.$(SUFFIX) : $(ZLASWP) $(ZLASWP_DEPS)
	$(CC) -c $(CFLAGS) -DXDOUBLE -DMINUS $< -o ../$(@F)

../slaswp_plus.$(PSUFFIX) : $(LASWP)
	$(CC) -c $(PFLAGS) -UDOUBLE -UMINUS $< -o ../$(@F)

../slaswp_minus.$(PSUFFIX) : $(LASWP)
	$(CC) -c $(PFLAGS) -UDOUBLE -DMINUS $< -o ../$(@F)

../dlaswp_plus.$(PSUFFIX) : $(LASWP)
	$(CC) -c $(PFLAGS) -DDOUBLE -UMINUS $< -o ../$(@F)

../dlaswp_minus.$(PSUFFIX) : $(LASWP)
	$(CC) -c $(PFLAGS) -DDOUBLE -DMINUS $< -o ../$(@F)

../qlaswp_plus.$(PSUFFIX) : $(LASWP)
	$(CC) -c $(PFLAGS) -DXDOUBLE -UMINUS $< -o ../$(@F)

../qlaswp_minus.$(PSUFFIX) : $(LASWP)
	$(CC) -c $(PFLAGS) -DXDOUBLE -DMINUS $< -o ../$(@F)

../claswp_plus.$(PSUFFIX) : $(ZLASWP)
	$(CC) -c $(PFLAGS) -UDOUBLE -UMINUS $< -o ../$(@F)

../claswp_minus.$(PSUFFIX) : $(ZLASWP)
	$(CC) -c $(PFLAGS) -UDOUBLE -DMINUS $< -o ../$(@F)

../zlaswp_plus.$(PSUFFIX) : $(ZLASWP)
	$(CC) -c $(PFLAGS) -DDOUBLE -UMINUS $< -o ../$(@F)

../zlaswp_minus.$(PSUFFIX) : $(ZLASWP)
	$(CC) -c $(PFLAGS) -DDOUBLE -DMINUS $< -o ../$(@F)

../xlaswp_plus.$(PSUFFIX) : $(ZLASWP)
	$(CC) -c $(PFLAGS) -DXDOUBLE -UMINUS $< -o ../$(@F)

../xlaswp_minus.$(PSUFFIX) : $(ZLASWP)
	$(CC) -c $(PFLAGS) -DXDOUBLE -DMINUS $< -o ../$(@F)


TOPDIR	= ../..
include ../../Makefile.system

SBLASOBJS = slaswp_plus.$(SUFFIX) slaswp_minus.$(SUFFIX)
DBLASOBJS = dlaswp_plus.$(SUFFIX) dlaswp_minus.$(SUFFIX)
QBLASOBJS = qlaswp_plus.$(SUFFIX) qlaswp_minus.$(SUFFIX)
CBLASOBJS = claswp_plus.$(SUFFIX) claswp_minus.$(SUFFIX)
ZBLASOBJS = zlaswp_plus.$(SUFFIX) zlaswp_minus.$(SUFFIX)
XBLASOBJS = xlaswp_plus.$(SUFFIX) xlaswp_minus.$(SUFFIX)

slaswp_plus.$(SUFFIX)  slaswp_minus.$(SUFFIX)  dlaswp_plus.$(SUFFIX)  dlaswp_minus.$(SUFFIX) \
qlaswp_plus.$(SUFFIX)  qlaswp_minus.$(SUFFIX) \
claswp_plus.$(SUFFIX)  claswp_minus.$(SUFFIX)  zlaswp_plus.$(SUFFIX)  zlaswp_minus.$(SUFFIX) \
xlaswp_plus.$(SUFFIX)  xlaswp_minus.$(SUFFIX) \
slaswp_plus.$(PSUFFIX) slaswp_minus.$(PSUFFIX) dlaswp_plus.$(PSUFFIX) dlaswp_minus.$(PSUFFIX) \
qlaswp_plus.$(PSUFFIX) qlaswp_minus.$(PSUFFIX) \
claswp_plus.$(PSUFFIX) claswp_minus.$(PSUFFIX) zlaswp_plus.$(PSUFFIX) zlaswp_minus.$(PSUFFIX) \
xlaswp_plus.$(PSUFFIX) xlaswp_minus.$(PSUFFIX) : dummy
	cd $(ARCH) && $(MAKE) ../$(@F)

include ../../Makefile.tail


TOPDIR	= ../../..
include ../../../Makefile.system

ifeq ($(CORE), CORE2)
LASWP	= ../generic/laswp_k_2.c
ZLASWP	= ../generic/zlaswp_k_2.c
endif

ifeq ($(CORE), OPTERON)
LASWP	= ../generic/laswp_k_1.c
ZLASWP	= ../generic/zlaswp_k_1.c
endif

ifeq ($(CORE), PRESCOTT)
LASWP	= ../generic/laswp_k_1.c
ZLASWP	= ../generic/zlaswp_k_1.c
endif

ifeq ($(DYNAMIC_ARCH), 1)
LASWP	= ../generic/laswp_k_4.c
ZLASWP	= ../generic/zlaswp_k_4.c
endif

ifndef LASWP
LASWP	= ../generic/laswp_k.c
endif

ifndef ZLASWP
ZLASWP	= ../generic/zlaswp_k.c
endif

include ../generic/Makefile


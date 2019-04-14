/*********************************************************************/
/* Copyright 2009, 2010 The University of Texas at Austin.           */
/* All rights reserved.                                              */
/*                                                                   */
/* Redistribution and use in source and binary forms, with or        */
/* without modification, are permitted provided that the following   */
/* conditions are met:                                               */
/*                                                                   */
/*   1. Redistributions of source code must retain the above         */
/*      copyright notice, this list of conditions and the following  */
/*      disclaimer.                                                  */
/*                                                                   */
/*   2. Redistributions in binary form must reproduce the above      */
/*      copyright notice, this list of conditions and the following  */
/*      disclaimer in the documentation and/or other materials       */
/*      provided with the distribution.                              */
/*                                                                   */
/*    THIS  SOFTWARE IS PROVIDED  BY THE  UNIVERSITY OF  TEXAS AT    */
/*    AUSTIN  ``AS IS''  AND ANY  EXPRESS OR  IMPLIED WARRANTIES,    */
/*    INCLUDING, BUT  NOT LIMITED  TO, THE IMPLIED  WARRANTIES OF    */
/*    MERCHANTABILITY  AND FITNESS FOR  A PARTICULAR  PURPOSE ARE    */
/*    DISCLAIMED.  IN  NO EVENT SHALL THE UNIVERSITY  OF TEXAS AT    */
/*    AUSTIN OR CONTRIBUTORS BE  LIABLE FOR ANY DIRECT, INDIRECT,    */
/*    INCIDENTAL,  SPECIAL, EXEMPLARY,  OR  CONSEQUENTIAL DAMAGES    */
/*    (INCLUDING, BUT  NOT LIMITED TO,  PROCUREMENT OF SUBSTITUTE    */
/*    GOODS  OR  SERVICES; LOSS  OF  USE,  DATA,  OR PROFITS;  OR    */
/*    BUSINESS INTERRUPTION) HOWEVER CAUSED  AND ON ANY THEORY OF    */
/*    LIABILITY, WHETHER  IN CONTRACT, STRICT  LIABILITY, OR TORT    */
/*    (INCLUDING NEGLIGENCE OR OTHERWISE)  ARISING IN ANY WAY OUT    */
/*    OF  THE  USE OF  THIS  SOFTWARE,  EVEN  IF ADVISED  OF  THE    */
/*    POSSIBILITY OF SUCH DAMAGE.                                    */
/*                                                                   */
/* The views and conclusions contained in the software and           */
/* documentation are those of the authors and should not be          */
/* interpreted as representing official policies, either expressed   */
/* or implied, of The University of Texas at Austin.                 */
/*********************************************************************/

#ifndef COMMON_POWER
#define COMMON_POWER

#if defined(POWER8)
#define MB		__asm__ __volatile__ ("eieio":::"memory")
#define WMB		__asm__ __volatile__ ("eieio":::"memory")
#else
#define MB		__asm__ __volatile__ ("sync")
#define WMB		__asm__ __volatile__ ("sync")
#endif

#define INLINE inline

#ifdef PPC440
#define STDERR stdout
#define QNONCACHE 0x1
#define QCOMMS    0x2
#define QFAST     0x4
#endif

#ifndef ASSEMBLER

void *qalloc(int flags, size_t bytes);

static void INLINE blas_lock(volatile unsigned long *address){

  long int ret, val = 1;

  do {
    while (*address) {YIELDING;};

#if defined(OS_LINUX) || defined(OS_DARWIN)
  __asm__ __volatile__ (
	   "0:	lwarx %0, 0, %1\n"
	   "	cmpwi %0, 0\n"
	   "	bne- 1f\n"
	   "	stwcx. %2,0, %1\n"
	   "	bne- 0b\n"
	   "1:    "
	: "=&r"(ret)
	: "r"(address), "r" (val)
	: "cr0", "memory");
#else
  __asm__ __volatile__ (
	   ".machine \"any\"\n"
	   "	lwarx %0, 0, %1\n"
	   "	cmpwi  %0, 0\n"
	   "	bne-  $+12\n"
	   "	stwcx. %2,0, %1\n"
	   "	bne-  $-16\n"
	: "=&r"(ret)
	: "r"(address), "r" (val)
	: "cr0", "memory");
#endif
  } while (ret);
}
#define BLAS_LOCK_DEFINED

static inline unsigned long rpcc(void){
  unsigned long ret;

#ifdef OS_AIX
  __asm__ __volatile__(".machine \"any\" ;");
#endif
  __asm__ __volatile__ ("mftb %0" : "=r" (ret) : );

#if defined(POWER5) || defined(PPC970)
  return (ret << 6);
#else
  return (ret << 3);
#endif

}
#define RPCC_DEFINED

#ifdef __64BIT__
#define RPCC64BIT
#endif

static inline unsigned long getstackaddr(void){
  unsigned long addr;

  __asm__ __volatile__ ("mr %0, 1"
			 : "=r"(addr) : : "memory");

  return addr;
};

#if defined(OS_LINUX) || defined(OS_AIX)
#define GET_IMAGE(res)  __asm__ __volatile__("fmr %0, 2" : "=f"(res)  : : "memory")
#else
#define GET_IMAGE(res)  __asm__ __volatile__("fmr %0, f2" : "=f"(res)  : : "memory")

#define GET_IMAGE_CANCEL

#endif

#ifdef SMP
static inline int blas_quickdivide(blasint x, blasint y){
  return x / y;
}
#endif

#endif


#ifdef ASSEMBLER

#ifdef DOUBLE
#define LFD	lfd
#define LFDX	lfdx
#define LFPDX	lfpdx
#define LFSDX	lfsdx
#define LFXDX	lfxdx
#define LFDU	lfdu
#define LFDUX	lfdux
#define LFPDUX	lfpdux
#define LFSDUX	lfsdux
#define LFXDUX	lfxdux
#define STFD	stfd
#define STFDX	stfdx
#define STFPDX	stfpdx
#define STFSDX	stfsdx
#define STFXDX	stfxdx
#define STFDU	stfdu
#define STFDUX	stfdux
#define STFPDUX	stfpdux
#define STFSDUX	stfsdux
#define STFXDUX	stfxdux
#define FMADD	fmadd
#define FMSUB	fmsub
#define FNMADD	fnmadd
#define FNMSUB	fnmsub
#define FMUL	fmul
#define FADD	fadd
#define FSUB	fsub
#else
#define LFD	lfs
#define LFDX	lfsx
#define LFPDX	lfpsx
#define LFSDX	lfssx
#define LFXDX	lfxsx
#define LFDU	lfsu
#define LFDUX	lfsux
#define LFPDUX	lfpsux
#define LFSDUX	lfssux
#define LFXDUX	lfxsux
#define STFD	stfs
#define STFDX	stfsx
#define STFPDX	stfpsx
#define STFSDX	stfssx
#define STFXDX	stfxsx
#define STFDU	stfsu
#define STFDUX	stfsux
#define STFPDUX	stfpsux
#define STFSDUX	stfssux
#define STFXDUX	stfxsux
#define FMADD	fmadds
#define FMSUB	fmsubs
#define FNMADD	fnmadds
#define FNMSUB	fnmsubs
#define FMUL	fmuls
#define FADD	fadds
#define FSUB	fsubs
#endif

#ifdef __64BIT__
#define LDLONG	ld
#else
#define LDLONG	lwz
#endif

#ifdef OS_DARWIN
#define LL(x)	L##x
#endif

#ifdef OS_LINUX
#define LL(x)	.L##x
#endif

#ifndef LL
#define LL(x)	__L##x
#endif


#if   defined(__64BIT__) &&  defined(USE64BITINT)
#define LDINT	ld
#elif defined(__64BIT__) && !defined(USE64BITINT)
#define LDINT	lwa
#else
#define LDINT	lwz
#endif

/*
#define DCBT(REGA, REGB, NUM) .long (0x7c00022c | (REGA << 16) | (REGB << 11) | ((NUM) << 21))
#define DCBTST(REGA, REGB, NUM) .long (0x7c0001ec | (REGA << 16) | (REGB << 11) | ((NUM) << 21))
*/

#define DSTATTR_H(SIZE, COUNT, STRIDE) ((SIZE << 8) | (COUNT))
#define DSTATTR_L(SIZE, COUNT, STRIDE) (STRIDE)

#if defined(PPC970) || defined(POWER3) || defined(POWER4) || defined(POWER5) || defined(PPCG4)
#define HAVE_PREFETCH
#endif

#if defined(POWER3) || defined(POWER6) || defined(PPCG4) || defined(CELL) || defined(POWER8)
#define DCBT_ARG	0
#else
#define DCBT_ARG	8
#endif

#ifdef CELL
#define L1_DUALFETCH
#define L1_PREFETCHSIZE (64 + 128 * 13)
#endif

#if defined(POWER3) || defined(POWER4) || defined(POWER5)
#define L1_DUALFETCH
#define L1_PREFETCHSIZE (96 + 128 * 12)
#endif

#if defined(POWER6)
#define L1_DUALFETCH
#define L1_PREFETCHSIZE (16 + 128 * 100)
#define L1_PREFETCH	dcbtst
#endif

#if defined(POWER8)
#define L1_DUALFETCH
#define L1_PREFETCHSIZE (16 + 128 * 100)
#define L1_PREFETCH	dcbtst
#endif

#
#ifndef L1_PREFETCH
#define L1_PREFETCH	dcbt
#endif

#ifndef L1_PREFETCHW
#define L1_PREFETCHW	dcbtst
#endif

#if DCBT_ARG == 0
#define DCBT(REGA, REGB)	L1_PREFETCH	REGB, REGA
#define DCBTST(REGA, REGB)	L1_PREFETCHW	REGB, REGA
#else
#define DCBT(REGA, REGB)	L1_PREFETCH	DCBT_ARG, REGB, REGA
#define DCBTST(REGA, REGB)	L1_PREFETCHW	DCBT_ARG, REGB, REGA
#endif


#ifndef L1_PREFETCHSIZE
#define L1_PREFETCHSIZE (96 + 128 * 12)
#endif

#if !defined(OS_DARWIN) || defined(NEEDPARAM)
#define f0	0
#define f1	1
#define f2	2
#define f3	3
#define f4	4
#define f5	5
#define f6	6
#define f7	7
#define f8	8
#define f9	9
#define f10	10
#define f11	11
#define f12	12
#define f13	13
#define f14	14
#define f15	15
#define f16	16
#define f17	17
#define f18	18
#define f19	19
#define f20	20
#define f21	21
#define f22	22
#define f23	23
#define f24	24
#define f25	25
#define f26	26
#define f27	27
#define f28	28
#define f29	29
#define f30	30
#define f31	31

#define r0	0
#define r1	1
#define r2	2
#define r3	3
#define r4	4
#define r5	5
#define r6	6
#define r7	7
#define r8	8
#define r9	9
#define r10	10
#define r11	11
#define r12	12
#define r13	13
#define r14	14
#define r15	15
#define r16	16
#define r17	17
#define r18	18
#define r19	19
#define r20	20
#define r21	21
#define r22	22
#define r23	23
#define r24	24
#define r25	25
#define r26	26
#define r27	27
#define r28	28
#define r29	29
#define r30	30
#define r31	31

#define v0	0
#define v1	1
#define v2	2
#define v3	3
#define v4	4
#define v5	5
#define v6	6
#define v7	7
#define v8	8
#define v9	9
#define v10	10
#define v11	11
#define v12	12
#define v13	13
#define v14	14
#define v15	15
#define v16	16
#define v17	17
#define v18	18
#define v19	19
#define v20	20
#define v21	21
#define v22	22
#define v23	23
#define v24	24
#define v25	25
#define v26	26
#define v27	27
#define v28	28
#define v29	29
#define v30	30
#define v31	31

#define BO_dCTR_NZERO_AND_NOT	0
#define BO_dCTR_NZERO_AND_NOT_1	1
#define BO_dCTR_ZERO_AND_NOT	2
#define BO_dCTR_ZERO_AND_NOT_1	3
#define BO_IF_NOT	4
#define BO_IF_NOT_1	5
#define BO_IF_NOT_2	6
#define BO_IF_NOT_3	7
#define BO_dCTR_NZERO_AND	8
#define BO_dCTR_NZERO_AND_1	9
#define BO_dCTR_ZERO_AND	10
#define BO_dCTR_ZERO_AND_1	11
#define BO_IF	12
#define BO_IF_1	13
#define BO_IF_2	14
#define BO_IF_3	15
#define BO_dCTR_NZERO	16
#define BO_dCTR_NZERO_1	17
#define BO_dCTR_ZERO	18
#define BO_dCTR_ZERO_1	19
#define BO_ALWAYS	20
#define BO_ALWAYS_1	21
#define BO_ALWAYS_2	22
#define BO_ALWAYS_3	23
#define BO_dCTR_NZERO_8	24
#define BO_dCTR_NZERO_9	25
#define BO_dCTR_ZERO_8	26
#define BO_dCTR_ZERO_9	27
#define BO_ALWAYS_8	28
#define BO_ALWAYS_9	29
#define BO_ALWAYS_10	30
#define BO_ALWAYS_11	31

#define CR0_LT	0
#define CR0_GT	1
#define CR0_EQ	2
#define CR0_SO	3
#define CR1_FX	4
#define CR1_FEX	5
#define CR1_VX	6
#define CR1_OX	7
#define CR2_LT	8
#define CR2_GT	9
#define CR2_EQ	10
#define CR2_SO	11
#define CR3_LT	12
#define CR3_GT	13
#define CR3_EQ	14
#define CR3_SO	15
#define CR4_LT	16
#define CR4_GT	17
#define CR4_EQ	18
#define CR4_SO	19
#define CR5_LT	20
#define CR5_GT	21
#define CR5_EQ	22
#define CR5_SO	23
#define CR6_LT	24
#define CR6_GT	25
#define CR6_EQ	26
#define CR6_SO	27
#define CR7_LT	28
#define CR7_GT	29
#define CR7_EQ	30
#define CR7_SO	31
#define TO_LT	16
#define TO_GT	8
#define TO_EQ	4
#define TO_LLT	2
#define TO_LGT	1
#define CR0	 0
#define CR1	 1
#define CR2	 2
#define CR3	 3
#define CR4	 4
#define CR5	 5
#define CR6	 6
#define CR7	 7
#define cr0	 0
#define cr1	 1
#define cr2	 2
#define cr3	 3
#define cr4	 4
#define cr5	 5
#define cr6	 6
#define cr7	 7
#define VRsave	256

#endif

#define CTR 9
#define SP r1

#ifdef __64BIT__
#define	slwi	sldi
#define cmpwi	cmpdi
#define srawi	sradi
#define mullw	mulld
#endif

#ifndef F_INTERFACE
#define REALNAME ASMNAME
#else
#define REALNAME ASMFNAME
#endif

#if defined(ASSEMBLER) && !defined(NEEDPARAM)

#ifdef OS_LINUX
#ifndef __64BIT__
#define PROLOGUE \
	.section .text;\
	.align 6;\
	.globl	REALNAME;\
	.type	REALNAME, @function;\
REALNAME:
#define EPILOGUE	.size	REALNAME, .-REALNAME
#else
#if _CALL_ELF == 2
#define PROLOGUE \
	.section .text;\
	.align 6;\
	.globl	REALNAME;\
	.type	REALNAME, @function;\
REALNAME:
#define EPILOGUE	.size	REALNAME, .-REALNAME
#else
#define PROLOGUE \
	.section .text;\
	.align 5;\
	.globl REALNAME;\
	.section	".opd","aw";\
	.align 3;\
REALNAME:;\
	.quad	.REALNAME, .TOC.@tocbase, 0;\
	.previous;\
	.size	REALNAME, 24;\
	.type	.REALNAME, @function;\
	.globl	.REALNAME;\
.REALNAME:
#define EPILOGUE \
	.long 0 ; \
	.byte 0,0,0,1,128,0,0,0 ; \
	.size	.REALNAME, .-.REALNAME; \
	.section	.note.GNU-stack,"",@progbits
#endif
#endif

#ifdef PROFILE
#ifndef __64BIT__
#define PROFCODE ;\
	.section	".data";\
	.align 2;\
.LP3:;\
	.long	0;\
	.section	".text";\
	mflr	r0;\
	stw	r0,   4(SP);\
	lis	r12, .LP3@ha;\
	la	r0, .LP3@l(r12);\
	bl	_mcount;\
	lwz	r0,   4(SP);\
	mtlr	r0
#else
#define PROFCODE \
	.globl	 _mcount; \
	mflr	r0; \
	std	r0,    16(SP); \
	mr	r11, SP; \
	addi	SP, SP, -256; \
	std	r11,    0(SP); \
	std	r3,   128(SP); \
	std	r4,   136(SP); \
	std	r5,   144(SP); \
	std	r6,   152(SP); \
	std	r7,   160(SP); \
	std	r8,   168(SP); \
	std	r9,   176(SP); \
	std	r10,  184(SP); \
	stfd	f3,   192(SP); \
	stfd	f4,   200(SP); \
	bl	._mcount; \
	nop; \
	ld	r3,   128(SP);\
	ld	r4,   136(SP);\
	ld	r5,   144(SP);\
	ld	r6,   152(SP);\
	ld	r7,   160(SP);\
	ld	r8,   168(SP);\
	ld	r9,   176(SP);\
	ld	r10,  184(SP);\
	lfd	f3,   192(SP);\
	lfd	f4,   200(SP);\
	addi	SP, SP,  256;\
	ld	r0,    16(SP);\
	mtlr	r0
#endif
#else
#define PROFCODE
#endif

#endif

#if OS_AIX
#ifndef __64BIT__
#define PROLOGUE \
	.machine "any";\
	.globl .REALNAME;\
	.csect .text[PR],5;\
.REALNAME:;

#define EPILOGUE \
_section_.text:;\
	.csect .data[RW],4;\
	.long _section_.text;

#else

#define PROLOGUE \
	.machine "any";\
	.globl .REALNAME;\
	.csect .text[PR], 5;\
.REALNAME:;

#define EPILOGUE \
_section_.text:;\
	.csect .data[RW],4;\
	.llong _section_.text;
#endif

#define PROFCODE

#endif

#ifdef OS_DARWIN
#ifndef __64BIT__
	.macro PROLOGUE
	.section __TEXT,__text,regular,pure_instructions
	.section __TEXT,__picsymbolstub1,symbol_stubs,pure_instructions,32
	.machine ppc
	.text
	.align 4
	.globl REALNAME
REALNAME:
	.endmacro
#else
	.macro PROLOGUE
	.section __TEXT,__text,regular,pure_instructions
	.section __TEXT,__picsymbolstub1,symbol_stubs,pure_instructions,32
	.machine ppc64
	.text
	.align 4
	.globl REALNAME
REALNAME:
	.endmacro
#endif

#ifndef PROFILE
#define PROFCODE
#define EPILOGUE	.subsections_via_symbols
#else
#ifndef __64BIT__

	.macro PROFCODE
	mflr	r0
	stw	r0,     8(SP)
	addi	SP, SP, -64
	stw	SP,     0(SP)
	stw	r3,    12(SP)
	stw	r4,    16(SP)
	stw	r5,    20(SP)
	stw	r6,    24(SP)
	stw	r7,    28(SP)
	stw	r8,    32(SP)
	stw	r9,    36(SP)
	stw	r10,   40(SP)
	stfd	f1,    48(SP)
	stfd	f2,    56(SP)
	mr	r3, r0
	bl	Lmcount$stub
	nop
	lwz	r3,    12(SP)
	lwz	r4,    16(SP)
	lwz	r5,    20(SP)
	lwz	r6,    24(SP)
	lwz	r7,    28(SP)
	lwz	r8,    32(SP)
	lwz	r9,    36(SP)
	lwz	r10,   40(SP)
	lfd	f1,    48(SP)
	lfd	f2,    56(SP)
	addi	SP, SP,  64
	lwz	r0,     8(SP)
	mtlr	r0
	.endmacro

	.macro EPILOGUE
	.section __TEXT,__picsymbolstub1,symbol_stubs,pure_instructions,32
	.align 5
Lmcount$stub:
	.indirect_symbol mcount
	mflr r0
	bcl 20,31,L00000000001$spb
L00000000001$spb:
	mflr r11
	addis r11,r11,ha16(Lmcount$lazy_ptr-L00000000001$spb)
	mtlr r0
	lwzu r12,lo16(Lmcount$lazy_ptr-L00000000001$spb)(r11)
	mtctr r12
	bctr
	.lazy_symbol_pointer
Lmcount$lazy_ptr:
	.indirect_symbol mcount
	.long	dyld_stub_binding_helper
	.subsections_via_symbols
	.endmacro

#else
	.macro PROFCODE
	mflr	r0
	std	r0,    16(SP)
	addi	SP, SP, -128
	std	SP,     0(SP)
	std	r3,    24(SP)
	std	r4,    32(SP)
	std	r5,    40(SP)
	std	r6,    48(SP)
	std	r7,    56(SP)
	std	r8,    64(SP)
	std	r9,    72(SP)
	std	r10,   80(SP)
	stfd	f1,    88(SP)
	stfd	f2,    96(SP)
	mr	r3, r0
	bl	Lmcount$stub
	nop
	ld	r3,    24(SP)
	ld	r4,    32(SP)
	ld	r5,    40(SP)
	ld	r6,    48(SP)
	ld	r7,    56(SP)
	ld	r8,    64(SP)
	ld	r9,    72(SP)
	ld	r10,   80(SP)
	lfd	f1,    88(SP)
	lfd	f2,    86(SP)
	addi	SP, SP,  128
	ld	r0,    16(SP)
	mtlr	r0
	.endmacro

	.macro EPILOGUE
	.data
	.section __TEXT,__picsymbolstub1,symbol_stubs,pure_instructions,32
	.align 5
Lmcount$stub:
	.indirect_symbol mcount
	mflr r0
	bcl 20,31,L00000000001$spb
L00000000001$spb:
	mflr r11
	addis r11,r11,ha16(Lmcount$lazy_ptr-L00000000001$spb)
	mtlr r0
	ld r12,lo16(Lmcount$lazy_ptr-L00000000001$spb)(r11)
	mtctr r12
	bctr
	.lazy_symbol_pointer
Lmcount$lazy_ptr:
	.indirect_symbol mcount
	.quad	dyld_stub_binding_helper
	.subsections_via_symbols
	.endmacro
#endif

#endif

#endif
#endif

#endif

#define HALT		mfspr	r0, 1023

#ifdef OS_LINUX
#if defined(PPC440) || defined(PPC440FP2)
#undef  MAX_CPU_NUMBER
#define MAX_CPU_NUMBER 1
#endif
#if !defined(__64BIT__) && !defined(PROFILE) && !defined(PPC440) && !defined(PPC440FP2)
#define START_ADDRESS (0x0b000000UL)
#else
#define SEEK_ADDRESS
#endif
#endif

#ifdef OS_AIX
#ifndef __64BIT__
#define START_ADDRESS (0xf0000000UL)
#else
#define SEEK_ADDRESS
#endif
#endif

#ifdef OS_DARWIN
#define SEEK_ADDRESS
#endif

#if defined(PPC440)
#define BUFFER_SIZE     (  2 << 20)
#elif defined(PPC440FP2)
#define BUFFER_SIZE     ( 16 << 20)
#elif defined(POWER8)
#define BUFFER_SIZE     ( 64 << 20)
#else
#define BUFFER_SIZE     ( 16 << 20)
#endif

#ifndef PAGESIZE
#define PAGESIZE	( 4 << 10)
#endif
#define HUGE_PAGESIZE	(16 << 20)

#define BASE_ADDRESS (START_ADDRESS - BUFFER_SIZE * MAX_CPU_NUMBER)

#ifndef MAP_ANONYMOUS
#define MAP_ANONYMOUS MAP_ANON
#endif

#ifdef OS_LINUX
#ifndef __64BIT__
#define FRAMESLOT(X) (((X) * 4) + 8)
#else
#if _CALL_ELF == 2
#define FRAMESLOT(X) (((X) * 8) + 96)
#else
#define FRAMESLOT(X) (((X) * 8) + 112)
#endif
#endif
#endif

#if defined(OS_AIX) || defined(OS_DARWIN)
#ifndef __64BIT__
#define FRAMESLOT(X) (((X) * 4) + 56)
#else
#define FRAMESLOT(X) (((X) * 8) + 112)
#endif
#endif

#endif

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

#ifndef COMMON_X86
#define COMMON_X86

#ifndef ASSEMBLER

#ifdef C_MSVC
#include <intrin.h>
#endif

#define MB
#define WMB

#ifdef C_SUN
#define	__asm__ __asm
#define	__volatile__
#endif

static void __inline blas_lock(volatile BLASULONG *address){

  int ret;

  do {
    while (*address) {YIELDING;};

#if defined(_MSC_VER) && !defined(__clang__)
	// use intrinsic instead of inline assembly
	ret = _InterlockedExchange((volatile LONG *)address, 1);
	// inline assembly
	/*__asm {
		mov eax, address
		mov ebx, 1
		xchg [eax], ebx
		mov ret, ebx
	}*/
#else
    __asm__ __volatile__(
			 "xchgl %0, %1\n"
			 : "=r"(ret), "=m"(*address)
			 : "0"(1), "m"(*address)
			 : "memory");
#endif

  } while (ret);

}
#define BLAS_LOCK_DEFINED

static __inline unsigned long long rpcc(void){
#if defined(_MSC_VER) && !defined(__clang__)
  return __rdtsc(); // use MSVC intrinsic
#else
  unsigned int a, d;

  __asm__ __volatile__ ("rdtsc" : "=a" (a), "=d" (d));

  return ((unsigned long long)a + ((unsigned long long)d << 32));
#endif
};
#define RPCC_DEFINED

static __inline unsigned long getstackaddr(void){
#if defined(_MSC_VER) && !defined(__clang__)
  return (unsigned long)_ReturnAddress(); // use MSVC intrinsic
#else
  unsigned long addr;

  __asm__ __volatile__ ("mov %%esp, %0"
			 : "=r"(addr) : : "memory");

  return addr;
#endif
};


static __inline long double sqrt_long(long double val) {
#if defined(_MSC_VER) && !defined(__clang__)
  return sqrt(val); // not sure if this will use fsqrt
#else
  long double result;

  __asm__ __volatile__ ("fldt %1\n"
		    "fsqrt\n"
		    "fstpt %0\n" : "=m" (result) : "m"(val));
  return result;
#endif
}

#define SQRT(a)  sqrt_long(a)

/* This is due to gcc's bug */
void cpuid(int op, int *eax, int *ebx, int *ecx, int *edx);

#define WHEREAMI

static __inline int WhereAmI(void){
  int eax, ebx, ecx, edx;
  int apicid;

  cpuid(1, &eax, &ebx, &ecx, &edx);
  apicid  = BITMASK(ebx, 24, 0xff);

  return apicid;
}

#ifdef ENABLE_SSE_EXCEPTION

#define IDEBUG_START \
{ \
  unsigned int fp_sse_mode, new_fp_mode; \
  __asm__ __volatile__ ("stmxcsr %0" : "=m" (fp_sse_mode) : ); \
  new_fp_mode = fp_sse_mode & ~0xd00; \
  __asm__ __volatile__ ("ldmxcsr %0" : : "m" (new_fp_mode) );

#define IDEBUG_END \
  __asm__ __volatile__ ("ldmxcsr %0" : : "m" (fp_sse_mode) ); \
}

#endif

#ifdef XDOUBLE
#define GET_IMAGE(res)  __asm__ __volatile__("fstpt %0" : "=m"(res) : : "memory")
#elif defined(DOUBLE)
#define GET_IMAGE(res)  __asm__ __volatile__("fstpl %0" : "=m"(res) : : "memory")
#else
#define GET_IMAGE(res)  __asm__ __volatile__("fstps %0" : "=m"(res) : : "memory");
#endif

#define GET_IMAGE_CANCEL	__asm__ __volatile__ ("ffree %st")

#ifdef SMP
extern unsigned int blas_quick_divide_table[];

static __inline int blas_quickdivide(unsigned int x, unsigned int y){

  unsigned int result;

  if (y <= 1) return x;

#if defined(_MSC_VER) && !defined(__clang__)
  result = x/y;
  return result;
#else
#if (MAX_CPU_NUMBER > 64)
  if ( y > 64) {
	  result = x/y;
	  return result;
  }
#endif
	
  y = blas_quick_divide_table[y];

  __asm__ __volatile__  ("mull %0" :"=d" (result) :"a"(x), "0" (y));

  return result;
#endif
}
#endif

#endif

#ifndef PAGESIZE
#define PAGESIZE	( 4 << 10)
#endif
#define HUGE_PAGESIZE	( 4 << 20)

#define BUFFER_SIZE	(16 << 20)

#define SEEK_ADDRESS

#if defined(DOUBLE) || defined(XDOUBLE)
#define	MMXLOAD		movq
#define MMXSTORE	movq
#else
#define MMXLOAD		movd
#define MMXSTORE	movd
#endif

#if defined(PILEDRIVER) || defined(BULLDOZER) || defined(STEAMROLLER) || defined(EXCAVATOR)
//Enable some optimazation for barcelona.
#define BARCELONA_OPTIMIZATION
#endif

#if defined(HAVE_3DNOW)
#define EMMS	femms
#elif defined(HAVE_MMX)
#define EMMS	emms
#endif

#ifndef EMMS
#define EMMS
#endif

#if defined(CORE2) || defined(PENTIUM4)
#define movapd	movaps
#endif

#define BRANCH		.byte 0x3e
#define NOBRANCH	.byte 0x2e
#define PADDING		.byte 0x66;
#define HALT		hlt

#ifndef COMPLEX
#ifdef XDOUBLE
#define LOCAL_BUFFER_SIZE  QLOCAL_BUFFER_SIZE
#elif defined DOUBLE
#define LOCAL_BUFFER_SIZE  DLOCAL_BUFFER_SIZE
#else
#define LOCAL_BUFFER_SIZE  SLOCAL_BUFFER_SIZE
#endif
#else
#ifdef XDOUBLE
#define LOCAL_BUFFER_SIZE  XLOCAL_BUFFER_SIZE
#elif defined DOUBLE
#define LOCAL_BUFFER_SIZE  ZLOCAL_BUFFER_SIZE
#else
#define LOCAL_BUFFER_SIZE  CLOCAL_BUFFER_SIZE
#endif
#endif

#if defined(OS_WINDOWS)
#if   LOCAL_BUFFER_SIZE > 16384
#define STACK_TOUCHING \
	movl	$0,  4096 * 4(%esp);\
	movl	$0,  4096 * 3(%esp);\
	movl	$0,  4096 * 2(%esp);\
	movl	$0,  4096 * 1(%esp);
#elif LOCAL_BUFFER_SIZE > 12288
#define STACK_TOUCHING \
	movl	$0,  4096 * 3(%esp);\
	movl	$0,  4096 * 2(%esp);\
	movl	$0,  4096 * 1(%esp);
#elif LOCAL_BUFFER_SIZE > 8192
#define STACK_TOUCHING \
	movl	$0,  4096 * 2(%esp);\
	movl	$0,  4096 * 1(%esp);
#elif LOCAL_BUFFER_SIZE > 4096
#define STACK_TOUCHING \
	movl	$0,  4096 * 1(%esp);
#else
#define STACK_TOUCHING
#endif
#else
#define STACK_TOUCHING
#endif

#ifndef F_INTERFACE
#define REALNAME ASMNAME
#else
#define REALNAME ASMFNAME
#endif

#if defined(F_INTERFACE_PATHSCALE) || defined(F_INTERFACE_OPEN64)
#define RETURN_BY_STRUCT
#elif defined(F_INTERFACE_GFORT) || defined(F_INTERFACE_G95)
#define RETURN_BY_COMPLEX
#else
#define RETURN_BY_STACK
#endif

#ifdef OS_DARWIN
#define PROLOGUE .text;.align 5; .globl REALNAME; REALNAME:
#define EPILOGUE	.subsections_via_symbols
#define PROFCODE
#endif

#if defined(OS_WINNT) || defined(OS_CYGWIN_NT) || defined(OS_INTERIX)
#define SAVEREGISTERS \
	subl	$32, %esp;\
	movups	%xmm6,    0(%esp);\
	movups	%xmm7,   16(%esp)

#define RESTOREREGISTERS \
	movups	   0(%esp), %xmm6;\
	movups	  16(%esp), %xmm7;\
	addl	$32, %esp
#else
#define SAVEREGISTERS
#define RESTOREREGISTERS
#endif

#if defined(OS_WINNT) || defined(OS_CYGWIN_NT) || defined(OS_INTERIX)
#define PROLOGUE \
	.text; \
	.align 16; \
	.globl REALNAME ;\
	.def REALNAME;.scl	2;.type	32;.endef; \
REALNAME:

#define PROFCODE

#ifdef __clang__
#define EPILOGUE .end
#else
#define EPILOGUE .end	 REALNAME
#endif
#endif

#if defined(OS_LINUX) || defined(OS_FREEBSD) || defined(OS_NETBSD) || defined(OS_OPENBSD) || defined(__ELF__)
#define PROLOGUE \
	.text; \
	.align 16; \
	.globl REALNAME ;\
       .type REALNAME, @function; \
REALNAME:

#ifdef PROFILE
#define PROFCODE call mcount
#else
#define PROFCODE
#endif

#define EPILOGUE \
        .size	 REALNAME, .-REALNAME; \
        .section .note.GNU-stack,"",@progbits

#endif

#ifdef XDOUBLE
#define FLD	fldt
#define FST	fstpt
#define FSTU	fstt
#define FMUL	fmult
#define FADD	faddt
#define FSUB	fsubt
#define FSUBR	fsubrt
#elif defined(DOUBLE)
#define FLD	fldl
#define FST	fstpl
#define FSTU	fstl
#define FMUL	fmull
#define FADD	faddl
#define FSUB	fsubl
#define FSUBR	fsubrl
#else
#define FLD	flds
#define FST	fstps
#define FSTU	fsts
#define FMUL	fmuls
#define FADD	fadds
#define FSUB	fsubs
#define FSUBR	fsubrs
#endif
#endif

#ifdef C_SUN
#define	ffreep	fstp
#endif

#ifdef __APPLE__
#define ALIGN_2 .align 2
#define ALIGN_3 .align 3
#define ALIGN_4 .align 4
#define ALIGN_5 .align 5
#define ffreep	fstp
#endif

#ifndef ALIGN_2
#define ALIGN_2 .align 4
#endif

#ifndef ALIGN_3
#define ALIGN_3 .align 8
#endif

#ifndef ALIGN_4
#define ALIGN_4 .align 16
#endif

#ifndef ALIGN_5
#define ALIGN_5 .align 32
#endif

#ifndef ALIGN_6
#define ALIGN_6 .align 64
#endif
// ffreep %st(0).
// Because Clang didn't support ffreep, we directly use the opcode.
// Please check out http://www.sandpile.org/x86/opc_fpu.htm
#ifndef ffreep
#define ffreep .byte 0xdf, 0xc0 #
#endif

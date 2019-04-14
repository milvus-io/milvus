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

#define MB	__asm__ __volatile__ ("nop")
#define WMB	__asm__ __volatile__ ("nop")

#ifndef ASSEMBLER

static void __inline blas_lock(volatile unsigned long *address){

  long int ret = 1;

  do {
    while (*address) {YIELDING;};

    __asm__ __volatile__(
			 "ldstub [%1], %0"
			 : "=&r"(ret)
			 : "r" (address)
			 : "memory");
  } while (ret);
}
#define BLAS_LOCK_DEFINED

static __inline unsigned long rpcc(void){
  unsigned long clocks;

  __asm__ __volatile__ ("rd %%tick, %0" : "=r" (clocks));

  return clocks;
};
#define RPCC_DEFINED

#ifdef __64BIT__
#define RPCC64BIT
#endif

#ifndef __BIG_ENDIAN__
#define __BIG_ENDIAN__
#endif

#ifdef DOUBLE
#define GET_IMAGE(res)  __asm__ __volatile__("fmovd %%f2, %0" : "=f"(res)  : : "memory")
#else
#define GET_IMAGE(res)  __asm__ __volatile__("fmovs %%f1, %0" : "=f"(res)  : : "memory")
#endif

#define GET_IMAGE_CANCEL

#ifdef SMP
static __inline int blas_quickdivide(blasint x, blasint y){
  return x / y;
}
#endif
#endif


#ifdef ASSEMBLER

#ifndef __64BIT__
#define STACK_START 128
#define SAVESP		save	%sp,  -64, %sp
#else
#define STACK_START 2423
#define SAVESP		save	%sp, -256, %sp
#endif

#define NOP	or %g1, %g1, %g1

#ifdef DOUBLE
#define LDF	ldd
#define STF	std
#define FADD	faddd
#define FMUL	fmuld
#define FMOV	fmovd
#define FABS	fabsd
#define FSUB	fsubd
#define FCMP	fcmpd
#define FMOVG	fmovdg
#define FMOVL	fmovdl
#define FSQRT	fsqrtd
#define FDIV	fdivd
#else
#define LDF	ld
#define STF	st
#define FADD	fadds
#define FMUL	fmuls
#define FMOV	fmovs
#define FABS	fabss
#define FSUB	fsubs
#define FCMP	fcmps
#define FMOVG	fmovsg
#define FMOVL	fmovsl
#define FSQRT	fsqrts
#define FDIV	fdivs
#endif

#define HALT prefetch [%g0], 5

#define FMADDS(rs1, rs2, rs3, rd) \
        .word	((2 << 30) | ((rd) << 25) | ( 0x37 << 19) | ((rs1) << 14) | ((rs3) << 9) | ( 1 << 5) | (rs2))

#define FMADDD(rs1, rs2, rs3, rd) \
        .word	((2 << 30) | ((rd) << 25) | ( 0x37 << 19) | ((rs1) << 14) | ((rs3) << 9) | ( 2 << 5) | (rs2))

#define FMSUBS(rs1, rs2, rs3, rd) \
        .word	((2 << 30) | ((rd) << 25) | ( 0x37 << 19) | ((rs1) << 14) | ((rs3) << 9) | ( 5 << 5) | (rs2))

#define FMSUBD(rs1, rs2, rs3, rd) \
        .word	((2 << 30) | ((rd) << 25) | ( 0x37 << 19) | ((rs1) << 14) | ((rs3) << 9) | ( 6 << 5) | (rs2))

#define FNMSUBS(rs1, rs2, rs3, rd) \
        .word	((2 << 30) | ((rd) << 25) | ( 0x37 << 19) | ((rs1) << 14) | ((rs3) << 9) | ( 9 << 5) | (rs2))

#define FNMSUBD(rs1, rs2, rs3, rd) \
        .word	((2 << 30) | ((rd) << 25) | ( 0x37 << 19) | ((rs1) << 14) | ((rs3) << 9) | (10 << 5) | (rs2))

#define FNMADDS(rs1, rs2, rs3, rd) \
        .word	((2 << 30) | ((rd) << 25) | ( 0x37 << 19) | ((rs1) << 14) | ((rs3) << 9) | (13 << 5) | (rs2))

#define FNMADDD(rs1, rs2, rs3, rd) \
        .word	((2 << 30) | ((rd) << 25) | ( 0x37 << 19) | ((rs1) << 14) | ((rs3) << 9) | (14 << 5) | (rs2))

#define FCLRS(rd) \
        .word	((2 << 30) | ((rd) << 25) | ( 0x36 << 19) | ( 0x61 << 5))

#define FCLRD(rd) \
        .word	((2 << 30) | ((rd) << 25) | ( 0x36 << 19) | ( 0x60 << 5))

#define FONES(rd) \
        .word	((2 << 30) | ((rd) << 25) | ( 0x36 << 19) | ( 0x7f << 5))

#define FONED(rd) \
        .word	((2 << 30) | ((rd) << 25) | ( 0x36 << 19) | ( 0x7e << 5))

#ifndef DOUBLE
#define	FCLR(a)			FCLRS(a)
#define	FONE(a)			FONES(a)
#define	FMADD(a, b, c, d)	FMADDS(a, b, c, d)
#define	FMSUB(a, b, c, d)	FMSUBS(a, b, c, d)
#define	FNMADD(a, b, c, d)	FNMADDS(a, b, c, d)
#define	FNMSUB(a, b, c, d)	FNMSUBS(a, b, c, d)
#else
#define	FCLR(a)			FCLRD(a)
#define	FONE(a)			FONED(a)
#define	FMADD(a, b, c, d)	FMADDD(a, b, c, d)
#define	FMSUB(a, b, c, d)	FMSUBD(a, b, c, d)
#define	FNMADD(a, b, c, d)	FNMADDD(a, b, c, d)
#define	FNMSUB(a, b, c, d)	FNMSUBD(a, b, c, d)
#endif

#ifndef F_INTERFACE
#define REALNAME ASMNAME
#else
#define REALNAME ASMFNAME
#endif

#ifdef sparc
#define PROLOGUE \
	.section	".text"; \
	.align 32; \
	.global REALNAME;\
	.type	REALNAME, #function; \
	.proc	07; \
REALNAME:;

#if defined(__linux__) && defined(__ELF__)
#define GNUSTACK .section .note.GNU-stack,"",@progbits
#else
#define GNUSTACK
#endif

#define EPILOGUE \
        .size	 REALNAME, .-REALNAME; \
        GNUSTACK

#endif

#endif

#ifdef sparc
#define SEEK_ADDRESS
#endif

#define BUFFER_SIZE	(32 << 20)

#ifndef PAGESIZE
#define PAGESIZE	( 8 << 10)
#endif
#define HUGE_PAGESIZE	( 4 << 20)

#define BASE_ADDRESS (START_ADDRESS - BUFFER_SIZE * MAX_CPU_NUMBER)

#ifndef MAP_ANONYMOUS
#define MAP_ANONYMOUS MAP_ANON
#endif
#endif

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

#ifndef COMMON_ALPHA
#define COMMON_ALPHA

#ifndef ASSEMBLER

#define MB  asm("mb")
#define WMB asm("wmb")

static void __inline blas_lock(unsigned long *address){
#ifndef __DECC
  unsigned long tmp1, tmp2;
  asm volatile(
    "1: ldq	%1,  %0\n"
    "	bne	%1,  2f\n"
    "	ldq_l	%1,  %0\n"
    "	bne	%1,  2f\n"
    "	or	%1,  1, %2\n"
    "	stq_c	%2,  %0\n"
    "	beq	%2,  2f\n"
    "	mb\n              "
    "	br      $31, 3f\n"
    "2: br      $31, 1b\n"
    "3:\n" : "=m"(*address), "=&r"(tmp1), "=&r"(tmp2) : :  "memory");
#else
  asm (
    "10:"
    "   ldq	%t0,  0(%a0); "
    "	bne	%t0, 20f;     "
    "	ldq_l	%t0,  0(%a0); "
    "	bne	%t0, 20f;     "
    "	or	%t0, 1, %t1;"
    "	stq_c	%t1,  0(%a0); "
    "	beq	%t1, 20f;     "
    "	mb;                   "
    "	br      %r31,30f;     "
    "20:                      "
    "	br      %r31,10b;     "
    "30:", address);
#endif
}
#define BLAS_LOCK_DEFINED

static __inline unsigned int rpcc(void){

  unsigned int r0;

#ifndef __DECC
  asm __volatile__("rpcc %0" : "=r"(r0)  : : "memory");
#else
  r0 = asm("rpcc %v0");
#endif

  return r0;
}
#define RPCC_DEFINED


#define HALT 	ldq	$0, 0($0)

#ifndef __DECC
#define GET_IMAGE(res)  asm __volatile__("fmov $f1, %0" : "=f"(res)  : : "memory")
#else
#define GET_IMAGE(res) res = dasm("fmov $f1, %f0")
#endif

#ifdef SMP
#ifdef USE64BITINT
static __inline long blas_quickdivide(long x, long y){
  return x/y;
}
#else
extern unsigned int blas_quick_divide_table[];

static __inline int blas_quickdivide(unsigned int x, unsigned int y){
  if (y <= 1) return x;
  return (int)((x * (unsigned long)blas_quick_divide_table[y]) >> 32);
}
#endif
#endif

#define BASE_ADDRESS ((0x1b0UL << 33) | (0x1c0UL << 23) | (0x000UL << 13))

#ifndef PAGESIZE
#define PAGESIZE	( 8UL << 10)
#define HUGE_PAGESIZE	( 4 << 20)
#endif
#define BUFFER_SIZE	(32UL << 20)

#else

#ifndef F_INTERFACE
#define REALNAME ASMNAME
#else
#define REALNAME ASMFNAME
#endif

#define PROLOGUE \
	.arch ev6; \
	.set noat; \
	.set noreorder; \
.text; \
	.align 5; \
	.globl REALNAME; \
	.ent REALNAME; \
REALNAME:

#ifdef PROFILE
#define PROFCODE \
	ldgp	$gp, 0($27); \
	lda	$28, _mcount; \
	jsr	$28, ($28), _mcount; \
	.prologue 1
#else
#define PROFCODE .prologue 0
#endif

#if defined(__linux__) && defined(__ELF__)
#define GNUSTACK .section .note.GNU-stack,"",@progbits
#else
#define GNUSTACK
#endif

#define EPILOGUE \
	.end REALNAME; \
	.ident VERSION; \
	GNUSTACK

#endif

#ifdef DOUBLE
#define SXADDQ	s8addq
#define SXSUBL	s8subl
#define LD	ldt
#define ST	stt
#define STQ	stq
#define ADD	addt/su
#define SUB	subt/su
#define MUL	mult/su
#define DIV	divt/su
#else
#define SXADDQ  s4addq
#define SXSUBL	s4subl
#define LD      lds
#define ST	sts
#define STQ	stl
#define ADD	adds/su
#define SUB	subs/su
#define MUL	muls/su
#define DIV	divs/su
#endif
#endif

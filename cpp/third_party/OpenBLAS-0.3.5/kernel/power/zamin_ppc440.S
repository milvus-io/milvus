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

#define ASSEMBLER
#include "common.h"

#define N	r3
#define X	r4
#define INCX	r5

#define PREX	r8
#define	INC1	r9

#define STACKSIZE 160

	PROLOGUE
	PROFCODE

	addi	SP, SP, -STACKSIZE
	li	r0,   0

	stfd	f14,    0(SP)
	stfd	f15,    8(SP)
	stfd	f16,   16(SP)
	stfd	f17,   24(SP)

	stfd	f18,   32(SP)
	stfd	f19,   40(SP)
	stfd	f20,   48(SP)
	stfd	f21,   56(SP)

	stfd	f22,   64(SP)
	stfd	f23,   72(SP)
	stfd	f24,   80(SP)
	stfd	f25,   88(SP)

	stfd	f26,   96(SP)
	stfd	f27,  104(SP)
	stfd	f28,  112(SP)
	stfd	f29,  120(SP)

	stfd	f30,  128(SP)
	stfd	f31,  136(SP)

#ifdef F_INTERFACE
	LDINT	N,    0(N)
	LDINT	INCX, 0(INCX)
#endif

	slwi	INCX, INCX, ZBASE_SHIFT

	sub	X, X, INCX
	li	INC1, SIZE

	cmpwi	cr0, N, 0
	ble-	LL(9999)
	cmpwi	cr0, INCX, 0
	ble-	LL(9999)

	LFDUX	f1, X, INCX
	LFDX	f2, X, INC1

	fabs	f1, f1
	li	PREX, 4 * 8 * SIZE
	fabs	f2, f2
	subi	N, N, 1
	fadd	f1, f1, f2

	fmr	f0, f1
	srawi.	r0, N, 3
	fmr	f2, f1
	mtspr	CTR,  r0
	fmr	f3, f1
	beq-	LL(150)

	LFDUX	f24,   X, INCX
	LFDX	f25,   X, INC1
	LFDUX	f26,   X, INCX
	LFDX	f27,   X, INC1
	LFDUX	f28,   X, INCX
	LFDX	f29,   X, INC1
	LFDUX	f30,   X, INCX
	LFDX	f31,   X, INC1

	fabs	f8,  f24
	LFDUX	f24,   X, INCX
	fabs	f9,  f25
	LFDX	f25,   X, INC1
	fabs	f10, f26
	LFDUX	f26,   X, INCX
	fabs	f11, f27
	LFDX	f27,   X, INC1
	fabs	f12, f28
	LFDUX	f28,   X, INCX
	fabs	f13, f29
	LFDX	f29,   X, INC1
	fabs	f14, f30
	LFDUX	f30,   X, INCX
	fabs	f15, f31
	LFDX	f31,   X, INC1
	bdz	LL(120)
	.align 4

LL(110):
	fadd	f4,  f8,  f9
#ifdef PPCG4
	dcbt	X, PREX
#endif
	fadd	f5,  f10, f11
	fadd	f6,  f12, f13
	fadd	f7,  f14, f15

	fabs	f8,  f24
	LFDUX	f24,   X, INCX
	fabs	f9,  f25
	LFDX	f25,   X, INC1
	fabs	f10, f26
	LFDUX	f26,   X, INCX
	fabs	f11, f27
	LFDX	f27,   X, INC1

	fabs	f12, f28
#ifdef PPCG4
	dcbt	X, PREX
#endif
	fabs	f13, f29
	LFDUX	f28,   X, INCX
	fabs	f14, f30
	LFDX	f29,   X, INC1
	fabs	f15, f31
	LFDUX	f30,   X, INCX

	fsub	f16, f0,  f4
	LFDX	f31,   X, INC1
	fsub	f17, f1,  f5
	fsub	f18, f2,  f6
	fsub	f19, f3,  f7

	fadd	f20, f8,  f9
#ifdef PPCG4
	dcbt	X, PREX
#endif
	fadd	f21, f10, f11
	fadd	f22, f12, f13
	fadd	f23, f14, f15

	fabs	f8,  f24
	LFDUX	f24,   X, INCX
	fabs	f9,  f25
	LFDX	f25,   X, INC1
	fabs	f10, f26
  	LFDUX	f26,   X, INCX
	fabs	f11, f27
	LFDX	f27,   X, INC1

	fsel	f0,  f16, f4,  f0
#ifdef PPCG4
	dcbt	X, PREX
#endif
	fsel	f1,  f17, f5,  f1
	fsel	f2,  f18, f6,  f2
	fsel	f3,  f19, f7,  f3

	fabs	f12, f28
	LFDUX	f28,   X, INCX
	fabs	f13, f29
	LFDX	f29,   X, INC1
	fabs	f14, f30
	LFDUX	f30,   X, INCX
	fabs	f15, f31
	LFDX	f31,   X, INC1

	fsub	f16, f0,  f20
	fsub	f17, f1,  f21
	fsub	f18, f2,  f22
	fsub	f19, f3,  f23

	fsel	f0,  f16, f20, f0
	fsel	f1,  f17, f21, f1
	fsel	f2,  f18, f22, f2
	fsel	f3,  f19, f23, f3
	bdnz	LL(110)
	.align 4

LL(120):
	fadd	f4,  f8,  f9
	fadd	f5,  f10, f11
	fadd	f6,  f12, f13
	fadd	f7,  f14, f15

	fabs	f8,  f24
	fabs	f9,  f25
	fabs	f10, f26
	fabs	f11, f27

	fabs	f12, f28
	fabs	f13, f29
	fabs	f14, f30
	fabs	f15, f31

	fsub	f16, f0,  f4
	fsub	f17, f1,  f5
	fsub	f18, f2,  f6
	fsub	f19, f3,  f7

	fadd	f20, f8,  f9
	fadd	f21, f10, f11
	fadd	f22, f12, f13
	fadd	f23, f14, f15

	fsel	f0,  f16, f4,  f0
	fsel	f1,  f17, f5,  f1
	fsel	f2,  f18, f6,  f2
	fsel	f3,  f19, f7,  f3

	fsub	f16, f0,  f20
	fsub	f17, f1,  f21
	fsub	f18, f2,  f22
	fsub	f19, f3,  f23

	fsel	f0,  f16, f20, f0
	fsel	f1,  f17, f21, f1
	fsel	f2,  f18, f22, f2
	fsel	f3,  f19, f23, f3
	.align 4

LL(150):
	andi.	r0,  N, 7
	mtspr	CTR, r0
	beq	LL(999)
	.align 4

LL(160):
	LFDUX	f8,    X, INCX
	LFDX	f9,    X, INC1

	fabs	f8, f8
	fabs	f9, f9
	fadd	f8, f8, f9
	fsub	f16, f1, f8
	fsel	f1, f16, f8, f1
	bdnz	LL(160)
	.align 4

LL(999):
	fsub	f8,  f0,  f1
	fsub	f9,  f2,  f3

	fsel	f0,  f8,  f1,  f0
	fsel	f2,  f9,  f3,  f2
	fsub	f8,  f0,  f2
	fsel	f1,  f8,  f2,  f0
	.align 4

LL(9999):
	lfd	f14,    0(SP)
	lfd	f15,    8(SP)
	lfd	f16,   16(SP)
	lfd	f17,   24(SP)

	lfd	f18,   32(SP)
	lfd	f19,   40(SP)
	lfd	f20,   48(SP)
	lfd	f21,   56(SP)

	lfd	f22,   64(SP)
	lfd	f23,   72(SP)
	lfd	f24,   80(SP)
	lfd	f25,   88(SP)

	lfd	f26,   96(SP)
	lfd	f27,  104(SP)
	lfd	f28,  112(SP)
	lfd	f29,  120(SP)

	lfd	f30,  128(SP)
	lfd	f31,  136(SP)

	addi	SP, SP, STACKSIZE
	blr

	EPILOGUE

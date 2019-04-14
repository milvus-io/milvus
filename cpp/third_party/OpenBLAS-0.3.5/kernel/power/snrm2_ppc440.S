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

#define PRE	r8

#define FZERO	144(SP)
#define FONE	148(SP)
#define C1	152(SP)
#define C2	156(SP)

#define STACKSIZE 160

	PROLOGUE
	PROFCODE

	addi	SP, SP, -STACKSIZE

	li	r10,   0
	lis	r11,   0x3f80
	lis	r6,    0x3f00
	lis	r7,    0x4040

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

	stw	r10,  FZERO
	stw	r11,  FONE
	stw	r6,   C1
	stw	r7,   C2

	lfs	f1,   FZERO

#ifdef F_INTERFACE
	LDINT	N,    0(N)
	LDINT	INCX, 0(INCX)
#endif

	slwi	INCX, INCX, BASE_SHIFT
	li	PRE, 3 * 16 * SIZE

	sub	X, X, INCX

	cmpwi	cr0, N, 0
	ble-	LL(999)
	cmpwi	cr0, INCX, 0
	ble-	LL(999)

	fmr	f0,  f1
	fmr	f2,  f1
	fmr	f3,  f1
	fmr	f4,  f1
	fmr	f5,  f1
	fmr	f6,  f1
	fmr	f7,  f1
	fmr	f8,  f1
	fmr	f9,  f1
	fmr	f10, f1
	fmr	f11, f1
	fmr	f12, f1
	fmr	f13, f1
	fmr	f14, f1
	fmr	f15, f1

	srawi.	r0, N, 4
	mtspr	CTR, r0
	beq-	cr0, LL(50)

	LFDUX	f16, X, INCX
	LFDUX	f17, X, INCX
	LFDUX	f18, X, INCX
	LFDUX	f19, X, INCX
	LFDUX	f20, X, INCX
	LFDUX	f21, X, INCX
	LFDUX	f22, X, INCX
	LFDUX	f23, X, INCX

	LFDUX	f24, X, INCX
	LFDUX	f25, X, INCX
	LFDUX	f26, X, INCX
	LFDUX	f27, X, INCX

	LFDUX	f28, X, INCX
	LFDUX	f29, X, INCX
	LFDUX	f30, X, INCX
	LFDUX	f31, X, INCX
	bdz	LL(20)
	.align 4

LL(10):
	fmadd	f0,  f16, f16, f0
	LFDUX	f16, X, INCX
	fmadd	f1,  f17, f17, f1
	LFDUX	f17, X, INCX
	fmadd	f2,  f18, f18, f2
	LFDUX	f18, X, INCX
	fmadd	f3,  f19, f19, f3
	LFDUX	f19, X, INCX

#ifdef PPCG4
	dcbt	X, PRE
#endif

	fmadd	f4,  f20, f20, f4
	LFDUX	f20, X, INCX
	fmadd	f5,  f21, f21, f5
	LFDUX	f21, X, INCX
	fmadd	f6,  f22, f22, f6
	LFDUX	f22, X, INCX
	fmadd	f7,  f23, f23, f7
	LFDUX	f23, X, INCX

	fmadd	f8,  f24, f24, f8
	LFDUX	f24, X, INCX
	fmadd	f9,  f25, f25, f9
	LFDUX	f25, X, INCX
	fmadd	f10, f26, f26, f10
	LFDUX	f26, X, INCX
	fmadd	f11, f27, f27, f11
	LFDUX	f27, X, INCX

#ifdef PPCG4
	dcbt	X, PRE
#endif

	fmadd	f12, f28, f28, f12
	LFDUX	f28, X, INCX
	fmadd	f13, f29, f29, f13
	LFDUX	f29, X, INCX
	fmadd	f14, f30, f30, f14
	LFDUX	f30, X, INCX
	fmadd	f15, f31, f31, f15
	LFDUX	f31, X, INCX

	bdnz	LL(10)
	.align 4

LL(20):
	fmadd	f0,  f16, f16, f0
	fmadd	f1,  f17, f17, f1
	fmadd	f2,  f18, f18, f2
	fmadd	f3,  f19, f19, f3

	fmadd	f4,  f20, f20, f4
	fmadd	f5,  f21, f21, f5
	fmadd	f6,  f22, f22, f6
	fmadd	f7,  f23, f23, f7

	fmadd	f8,  f24, f24, f8
	fmadd	f9,  f25, f25, f9
	fmadd	f10, f26, f26, f10
	fmadd	f11, f27, f27, f11

	fmadd	f12, f28, f28, f12
	fmadd	f13, f29, f29, f13
	fmadd	f14, f30, f30, f14
	fmadd	f15, f31, f31, f15
	.align 4

LL(50):
	andi.	r0,  N, 15
	mtspr	CTR, r0
	beq-	cr0, LL(70)
	.align 4

LL(60):
	LFDUX	f16, X, INCX
	fmadd	f0,  f16, f16, f0
	bdnz	LL(60)
	.align 4

LL(70):
	fadd   f0,  f0,  f1
	fadd   f2,  f2,  f3
	fadd   f4,  f4,  f5
	fadd   f6,  f6,  f7

	fadd   f8,  f8,  f9
	fadd   f10, f10, f11
	fadd   f12, f12, f13
	fadd   f14, f14, f15

	fadd   f0,  f0,  f2
	fadd   f4,  f4,  f6
	fadd   f8,  f8,  f10
	fadd   f12, f12, f14

	fadd   f0,  f0,  f4
	fadd   f8,  f8,  f12

	fadd   f1,  f0,  f8
	lfs    f4,   FZERO

	fcmpu	cr0, f1, f4
	beq	cr0, LL(999)

	frsqrte f0, f1
	lfs	f8, C1
	lfs	f9, C2

	fmul	f2, f1, f0
	fadd	f7, f8, f8
	fmul	f3, f0, f8
	fnmsub	f4, f2, f0, f9
	fmul	f0, f3, f4

	fmul	f5, f1, f0
	fmul	f2, f5, f8
	fnmsub	f3, f5, f0, f7
	fmadd	f1, f2, f3, f5
	.align 4

LL(999):
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

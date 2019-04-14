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

#define M	r3
#define	N	r4
#define A	r6
#define LDA	r7
#define X	r8
#define	INCX	r9
#define	Y	r10
#define	INCY	r5

#define I	r11
#define	J	r12

#define INCY2	r24
#define A1	r25
#define A2	r26
#define A3	r27
#define A4	r28

#define YL	r29
#define	YS	r30
#define INC2	r31

#define yl1 f0
#define yl2 f2
#define yl3 f3
#define yl4 f4
#define ys1 f5
#define ys2 f6
#define ys3 f7
#define ys4 f8
#define	yl5 f27
#define ys5 f28

#define alpha1 f9
#define alpha2 f10

#define a1     f11
#define a2     f12
#define a3     f13
#define a4     f14
#define a5     f15
#define a6     f16
#define a7     f17
#define a8     f18

#define a9     f19
#define a10    f20
#define a11    f21
#define a12    f22
#define a13    f23
#define a14    f24
#define a15    f25
#define a16    f26

#define alpha  f1

	PROLOGUE
	PROFCODE

	li	r0, -16
	lwz	INCY,      8(SP)

	stfpdux	f14, SP, r0
	stfpdux	f15, SP, r0
	stfpdux	f16, SP, r0
	stfpdux	f17, SP, r0
	stfpdux	f18, SP, r0
	stfpdux	f19, SP, r0
	stfpdux	f20, SP, r0
	stfpdux	f21, SP, r0
	stfpdux	f22, SP, r0
	stfpdux	f23, SP, r0
	stfpdux	f24, SP, r0
	stfpdux	f25, SP, r0
	stfpdux	f26, SP, r0
	stfpdux	f27, SP, r0
	stfpdux	f28, SP, r0
	stfpdux	f29, SP, r0
	stfpdux	f30, SP, r0
	stfpdux	f31, SP, r0

	stwu	r31,  -4(SP)
	stwu	r30,  -4(SP)
	stwu	r29,  -4(SP)
	stwu	r28,  -4(SP)

	stwu	r27,  -4(SP)
	stwu	r26,  -4(SP)
	stwu	r25,  -4(SP)
	stwu	r24,  -4(SP)

	stwu	r23,  -4(SP)
	stwu	r22,  -4(SP)
	stwu	r21,  -4(SP)
	stwu	r20,  -4(SP)

	stwu	r19,  -4(SP)
	stwu	r18,  -4(SP)
	stwu	r17,  -4(SP)
	stwu	r16,  -4(SP)

	slwi	LDA,  LDA,  BASE_SHIFT
	slwi	INCX, INCX, BASE_SHIFT
	slwi	INCY, INCY, BASE_SHIFT

	fsmfp	alpha, alpha

	cmpwi	cr0, M, 0
	ble-	.L999
	cmpwi	cr0, N, 0
	ble-	.L999

	add	INCY2, INCY, INCY
	li	INC2, 2 * SIZE
	sub	X, X, INCX

	andi.	r0, A,  2 * SIZE - 1
#	bne	.L100

# All cases for aligned A, even LDA

	cmpwi	cr0, INCY,  SIZE
	bne	.L70

	andi.	r0, Y,  2 * SIZE - 1
	bne	.L40

# A : aligned  LDA : even  Y : Unit Aligned

	sub	A, A, INC2
	sub	Y, Y, INCY2

	srawi.	J, N, 2
	ble	.L20
	.align 4

.L11:
	LFDUX	alpha1, X, INCX
	mr	A1, A
	add	A2, A,  LDA
	add	A3, A2, LDA
	LFSDUX	alpha1, X, INCX
	LFDUX	alpha2, X, INCX
	add	A4, A3, LDA
	add	A,  A4, LDA
	mr	YL, Y
	LFSDUX	alpha2, X, INCX
	fpmul	alpha1, alpha, alpha1
	mr	YS, Y
	srawi.	r0,  M, 3
	mtspr	CTR, r0
	fpmul	alpha2, alpha, alpha2
	ble	.L15

	LFPDUX	yl1, YL, INCY2
	LFPDUX	yl2, YL, INCY2
	LFPDUX	yl3, YL, INCY2
	LFPDUX	yl4, YL, INCY2

	LFPDUX	a1,  A1, INC2
	LFPDUX	a5,  A1, INC2
	LFPDUX	a9,  A1, INC2
	LFPDUX	a13, A1, INC2

	LFPDUX	a2,  A2, INC2
	LFPDUX	a6,  A2, INC2
	LFPDUX	a10, A2, INC2
	LFPDUX	a14, A2, INC2

	LFPDUX	a3,  A3, INC2
	LFPDUX	a7,  A3, INC2
	LFPDUX	a11, A3, INC2
	LFPDUX	a15, A3, INC2

	LFPDUX	a4,  A4, INC2
	fxcpmadd  ys1, alpha1, a1,  yl1
	LFPDUX	a8,  A4, INC2
	fxcpmadd  ys2, alpha1, a5,  yl2
	LFPDUX	a12, A4, INC2
	fxcpmadd  ys3, alpha1, a9,  yl3
	LFPDUX	a16, A4, INC2
	fxcpmadd  ys4, alpha1, a13, yl4
	bdz	.L13
	.align 4

.L12:
	LFPDUX	yl1, YL, INCY2

	fxcsmadd  ys1, alpha1, a2,  ys1
	LFPDUX	a1,  A1, INC2
	fxcsmadd  ys2, alpha1, a6,  ys2
	LFPDUX	a5,  A1, INC2
	fxcsmadd  ys3, alpha1, a10, ys3
	LFPDUX	a9,  A1, INC2
	fxcsmadd  ys4, alpha1, a14, ys4
	LFPDUX	a13, A1, INC2

	LFPDUX	yl2, YL, INCY2

	fxcpmadd  ys1, alpha2, a3,  ys1
	LFPDUX	a2,  A2, INC2
	fxcpmadd  ys2, alpha2, a7,  ys2
	LFPDUX	a6,  A2, INC2
	fxcpmadd  ys3, alpha2, a11, ys3
	LFPDUX	a10, A2, INC2
	fxcpmadd  ys4, alpha2, a15, ys4
	LFPDUX	a14, A2, INC2

	LFPDUX	yl3, YL, INCY2

	fxcsmadd  ys1, alpha2, a4,  ys1
	LFPDUX	a3,  A3, INC2
	fxcsmadd  ys2, alpha2, a8,  ys2
	LFPDUX	a7,  A3, INC2
	fxcsmadd  ys3, alpha2, a12, ys3
	LFPDUX	a11, A3, INC2
	fxcsmadd  ys4, alpha2, a16, ys4
	LFPDUX	a15, A3, INC2

	LFPDUX	yl4, YL, INCY2

	STFPDUX	ys1, YS, INCY2
	STFPDUX	ys2, YS, INCY2
	STFPDUX	ys3, YS, INCY2
	STFPDUX	ys4, YS, INCY2

	LFPDUX	a4,  A4, INC2
	fxcpmadd  ys1, alpha1, a1,  yl1
	LFPDUX	a8,  A4, INC2
	fxcpmadd  ys2, alpha1, a5,  yl2
	LFPDUX	a12, A4, INC2
	fxcpmadd  ys3, alpha1, a9,  yl3
	LFPDUX	a16, A4, INC2
	fxcpmadd  ys4, alpha1, a13, yl4
	bdnz	.L12
	.align 4

.L13:
	fxcsmadd  ys1, alpha1, a2,  ys1
	fxcsmadd  ys2, alpha1, a6,  ys2
	fxcsmadd  ys3, alpha1, a10, ys3
	fxcsmadd  ys4, alpha1, a14, ys4

	fxcpmadd  ys1, alpha2, a3,  ys1
	fxcpmadd  ys2, alpha2, a7,  ys2
	fxcpmadd  ys3, alpha2, a11, ys3
	fxcpmadd  ys4, alpha2, a15, ys4

	fxcsmadd  ys1, alpha2, a4,  ys1
	fxcsmadd  ys2, alpha2, a8,  ys2
	fxcsmadd  ys3, alpha2, a12, ys3
	fxcsmadd  ys4, alpha2, a16, ys4

	STFPDUX	ys1, YS, INCY2
	STFPDUX	ys2, YS, INCY2
	STFPDUX	ys3, YS, INCY2
	STFPDUX	ys4, YS, INCY2
	.align 4

.L15:
	andi.	r0, M, 7
	ble	.L19

	andi.	r0, M, 4
	ble	.L17

	LFPDUX	yl1, YL, INCY2
	LFPDUX	a1,  A1, INC2
	LFPDUX	yl2, YL, INCY2
	LFPDUX	a5,  A1, INC2

	LFPDUX	a2,  A2, INC2
	LFPDUX	a6,  A2, INC2
	LFPDUX	a3,  A3, INC2
	LFPDUX	a7,  A3, INC2

	LFPDUX	a4,  A4, INC2
	LFPDUX	a8,  A4, INC2

	fxcpmadd  ys1, alpha1, a1, yl1
	fxcpmadd  ys2, alpha1, a5, yl2
	fxcsmadd  ys1, alpha1, a2, ys1
	fxcsmadd  ys2, alpha1, a6, ys2

	fxcpmadd  ys1, alpha2, a3, ys1
	fxcpmadd  ys2, alpha2, a7, ys2
	fxcsmadd  ys1, alpha2, a4, ys1
	fxcsmadd  ys2, alpha2, a8, ys2

	STFPDUX	ys1, YS, INCY2
	STFPDUX	ys2, YS, INCY2
	.align 4

.L17:
	andi.	r0, M, 2
	ble	.L18

	LFPDUX	yl1, YL, INCY2

	LFPDUX	a1,  A1, INC2
	LFPDUX	a2,  A2, INC2
	LFPDUX	a3,  A3, INC2
	LFPDUX	a4,  A4, INC2

	fxcpmadd  ys1, alpha1, a1, yl1
	fxcsmadd  ys1, alpha1, a2, ys1
	fxcpmadd  ys1, alpha2, a3, ys1
	fxcsmadd  ys1, alpha2, a4, ys1

	STFPDUX	ys1, YS, INCY2
	.align 4

.L18:
	andi.	r0, M, 1
	ble	.L19

	LFDUX	yl1, YL, INCY2

	LFDUX	a1,  A1, INC2
	LFDUX	a2,  A2, INC2
	LFDUX	a3,  A3, INC2
	LFDUX	a4,  A4, INC2

	fxcpmadd  ys1, alpha1, a1, yl1
	fxcsmadd  ys1, alpha1, a2, ys1
	fxcpmadd  ys1, alpha2, a3, ys1
	fxcsmadd  ys1, alpha2, a4, ys1

	STFDUX	ys1, YS, INCY2
	.align 4

.L19:
	addi	J, J, -1
	cmpi	cr0, 0, J, 0
	bgt	.L11
	.align 4

.L20:
	andi.	J, N, 2
	ble	.L30

	LFDUX	alpha1, X, INCX

	mr	A1, A
	add	A2, A,  LDA
	add	A,  A2, LDA
	LFSDUX	alpha1, X, INCX

	mr	YL, Y
	mr	YS, Y
	fpmul	alpha1, alpha, alpha1

	srawi.	r0,  M, 3
	mtspr	CTR, r0
	ble	.L25

	LFPDUX	yl1, YL, INCY2
	LFPDUX	a1,  A1, INC2
	LFPDUX	yl2, YL, INCY2
	LFPDUX	a5,  A1, INC2

	LFPDUX	yl3, YL, INCY2
	LFPDUX	a9,  A1, INC2
	LFPDUX	yl4, YL, INCY2
	LFPDUX	a13, A1, INC2

	LFPDUX	a2,  A2, INC2
	LFPDUX	a6,  A2, INC2
	LFPDUX	a10, A2, INC2
	LFPDUX	a14, A2, INC2
	bdz	.L23
	.align 4

.L22:
	fxcpmadd  ys1, alpha1, a1,  yl1
	LFPDUX	a1,  A1, INC2
	LFPDUX	yl1, YL, INCY2
	fxcpmadd  ys2, alpha1, a5,  yl2
	LFPDUX	a5,  A1, INC2
	LFPDUX	yl2, YL, INCY2
	fxcpmadd  ys3, alpha1, a9,  yl3
	LFPDUX	a9,  A1, INC2
	LFPDUX	yl3, YL, INCY2
	fxcpmadd  ys4, alpha1, a13, yl4
	LFPDUX	a13, A1, INC2
	LFPDUX	yl4, YL, INCY2

	fxcsmadd  ys1, alpha1, a2,  ys1
	LFPDUX	a2,  A2, INC2
	fxcsmadd  ys2, alpha1, a6,  ys2
	LFPDUX	a6,  A2, INC2
	fxcsmadd  ys3, alpha1, a10, ys3
	LFPDUX	a10, A2, INC2
	fxcsmadd  ys4, alpha1, a14, ys4
	LFPDUX	a14, A2, INC2

	STFPDUX	ys1, YS, INCY2
	STFPDUX	ys2, YS, INCY2
	STFPDUX	ys3, YS, INCY2
	STFPDUX	ys4, YS, INCY2
	bdnz	.L22
	.align 4

.L23:
	fxcpmadd  ys1, alpha1, a1,  yl1
	fxcpmadd  ys2, alpha1, a5,  yl2
	fxcpmadd  ys3, alpha1, a9,  yl3
	fxcpmadd  ys4, alpha1, a13, yl4

	fxcsmadd  ys1, alpha1, a2,  ys1
	fxcsmadd  ys2, alpha1, a6,  ys2
	fxcsmadd  ys3, alpha1, a10, ys3
	fxcsmadd  ys4, alpha1, a14, ys4

	STFPDUX	ys1, YS, INCY2
	STFPDUX	ys2, YS, INCY2
	STFPDUX	ys3, YS, INCY2
	STFPDUX	ys4, YS, INCY2
	.align 4

.L25:
	andi.	r0, M, 7
	ble	.L30

	andi.	r0, M, 4
	ble	.L27

	LFPDUX	yl1, YL, INCY2
	LFPDUX	a1,  A1, INC2
	LFPDUX	a2,  A2, INC2

	LFPDUX	yl2, YL, INCY2
	LFPDUX	a5,  A1, INC2
	LFPDUX	a6,  A2, INC2

	fxcpmadd  ys1, alpha1, a1, yl1
	fxcsmadd  ys1, alpha1, a2, ys1
	fxcpmadd  ys2, alpha1, a5, yl2
	fxcsmadd  ys2, alpha1, a6, ys2

	STFPDUX	ys1, YS, INCY2
	STFPDUX	ys2, YS, INCY2
	.align 4

.L27:
	andi.	r0, M, 2
	ble	.L28

	LFPDUX	yl1, YL, INCY2
	LFPDUX	a1,  A1, INC2
	LFPDUX	a2,  A2, INC2

	fxcpmadd  ys1, alpha1, a1, yl1
	fxcsmadd  ys1, alpha1, a2, ys1

	STFPDUX	ys1, YS, INCY2
	.align 4

.L28:
	andi.	r0, M, 1
	ble	.L30

	LFDUX	yl1, YL, INCY2
	LFDUX	a1,  A1, INC2
	LFDUX	a2,  A2, INC2

	fxcpmadd  ys1, alpha1, a1, yl1
	fxcsmadd  ys1, alpha1, a2, ys1

	STFDUX	ys1, YS, INCY2
	.align 4

.L30:
	andi.	J, N, 1
	ble	.L999

	LFDUX	alpha1, X, INCX

	mr	A1, A
	mr	YL, Y
	mr	YS, Y
	fmul	alpha1, alpha, alpha1

	srawi.	r0,  M, 3
	mtspr	CTR, r0
	ble	.L35

	LFPDUX	yl1, YL, INCY2
	LFPDUX	a1,  A1, INC2
	LFPDUX	yl2, YL, INCY2
	LFPDUX	a5,  A1, INC2

	LFPDUX	yl3, YL, INCY2
	LFPDUX	a9,  A1, INC2
	LFPDUX	yl4, YL, INCY2
	LFPDUX	a13, A1, INC2
	bdz	.L33
	.align 4

.L32:
	fxcpmadd  ys1, alpha1, a1,  yl1
	LFPDUX	yl1, YL, INCY2
	LFPDUX	a1,  A1, INC2
	fxcpmadd  ys2, alpha1, a5,  yl2
	LFPDUX	yl2, YL, INCY2
	LFPDUX	a5,  A1, INC2
	fxcpmadd  ys3, alpha1, a9,  yl3
	LFPDUX	yl3, YL, INCY2
	LFPDUX	a9,  A1, INC2
	fxcpmadd  ys4, alpha1, a13, yl4
	LFPDUX	yl4, YL, INCY2
	LFPDUX	a13, A1, INC2

	STFPDUX	ys1, YS, INCY2
	STFPDUX	ys2, YS, INCY2
	STFPDUX	ys3, YS, INCY2
	STFPDUX	ys4, YS, INCY2
	bdnz	.L32
	.align 4

.L33:
	fxcpmadd  ys1, alpha1, a1,  yl1
	fxcpmadd  ys2, alpha1, a5,  yl2
	fxcpmadd  ys3, alpha1, a9,  yl3
	fxcpmadd  ys4, alpha1, a13, yl4

	STFPDUX	ys1, YS, INCY2
	STFPDUX	ys2, YS, INCY2
	STFPDUX	ys3, YS, INCY2
	STFPDUX	ys4, YS, INCY2
	.align 4

.L35:
	andi.	r0, M, 7
	ble	.L999

	andi.	r0, M, 4
	ble	.L37

	LFPDUX	yl1, YL, INCY2
	LFPDUX	a1,  A1, INC2

	LFPDUX	yl2, YL, INCY2
	LFPDUX	a5,  A1, INC2

	fxcpmadd  ys1, alpha1, a1, yl1
	fxcpmadd  ys2, alpha1, a5, yl2

	STFPDUX	ys1, YS, INCY2
	STFPDUX	ys2, YS, INCY2
	.align 4

.L37:
	andi.	r0, M, 2
	ble	.L38

	LFPDUX	yl1, YL, INCY2
	LFPDUX	a1,  A1, INC2

	fxcpmadd  ys1, alpha1, a1, yl1

	STFPDUX	ys1, YS, INCY2
	.align 4

.L38:
	andi.	r0, M, 1
	ble	.L999

	LFDUX	yl1, YL, INCY2
	LFDUX	a1,  A1, INC2

	fxcpmadd  ys1, alpha1, a1, yl1

	STFDUX	ys1, YS, INCY2
	b	.L999
	.align 4

.L40:
# A : aligned  LDA : even  Y : Unaligned

	sub	A, A, INC2
	sub	Y, Y, INCY

	srawi.	J, N, 2
	ble	.L50
	.align 4

.L41:
	LFDUX	alpha1, X, INCX
	LFSDUX	alpha1, X, INCX
	LFDUX	alpha2, X, INCX
	LFSDUX	alpha2, X, INCX

	fpmul	alpha1, alpha, alpha1
	fpmul	alpha2, alpha, alpha2

	mr	A1, A
	add	A2, A,  LDA
	add	A3, A2, LDA
	add	A4, A3, LDA
	add	A,  A4, LDA

	mr	YL, Y
	sub	YS, Y, INCY2

	LFSDX	ys1, YS, INCY2
	LFDX	yl1, YL, INCY

	srawi.	r0,  M, 3
	mtspr	CTR, r0
	ble	.L45

	LFPDUX	a1,  A1, INC2
	LFPDUX	a5,  A1, INC2
	LFPDUX	a9,  A1, INC2
	LFPDUX	a13, A1, INC2

	LFXDUX	yl2, YL, INCY2
	LFXDUX	yl3, YL, INCY2
	LFXDUX	yl4, YL, INCY2
	LFXDUX	yl5, YL, INCY2

	LFPDUX	a2,  A2, INC2
	LFPDUX	a6,  A2, INC2
	LFPDUX	a10, A2, INC2
	LFPDUX	a14, A2, INC2

	LFPDUX	a3,  A3, INC2
	LFPDUX	a7,  A3, INC2
	LFPDUX	a11, A3, INC2
	LFPDUX	a15, A3, INC2

	LFPDUX	a4,  A4, INC2
	fsmr	yl1, yl2
	LFPDUX	a8,  A4, INC2
	fsmr	yl2, yl3
	LFPDUX	a12, A4, INC2
	fsmr	yl3, yl4
	LFPDUX	a16, A4, INC2
	fsmr	yl4, yl5
	bdz	.L43
	.align 4

.L42:
	fxcpmadd  ys2, alpha1, a1,  yl1
	LFPDUX	a1,  A1, INC2
	fxcpmadd  ys3, alpha1, a5,  yl2
	LFPDUX	a5,  A1, INC2
	fxcpmadd  ys4, alpha1, a9,  yl3
	LFPDUX	a9,  A1, INC2
	fxcpmadd  ys5, alpha1, a13, yl4
	LFPDUX	a13, A1, INC2

	fxcsmadd  ys2, alpha1, a2,  ys2
	LFPDUX	a2,  A2, INC2
	fxcsmadd  ys3, alpha1, a6,  ys3
	LFPDUX	a6,  A2, INC2
	fxcsmadd  ys4, alpha1, a10, ys4
	LFPDUX	a10, A2, INC2
	fxcsmadd  ys5, alpha1, a14, ys5
	LFPDUX	a14, A2, INC2

	fxcpmadd  ys2, alpha2, a3,  ys2
	LFPDUX	a3,  A3, INC2
	fxcpmadd  ys3, alpha2, a7,  ys3
	LFPDUX	a7,  A3, INC2
	fxcpmadd  ys4, alpha2, a11, ys4
	LFPDUX	a11, A3, INC2
	fxcpmadd  ys5, alpha2, a15, ys5
	LFPDUX	a15, A3, INC2

	fxcsmadd  ys2, alpha2, a4,  ys2
	LFPDUX	a4,  A4, INC2
	fxcsmadd  ys3, alpha2, a8,  ys3
	LFPDUX	a8,  A4, INC2
	fxcsmadd  ys4, alpha2, a12, ys4
	LFPDUX	a12, A4, INC2
	fxcsmadd  ys5, alpha2, a16, ys5
	LFPDUX	a16, A4, INC2

	fmr	yl1, yl5
	LFXDUX	yl2, YL, INCY2
	fmr	ys1, ys2
	LFXDUX	yl3, YL, INCY2
	fmr	ys2, ys3
	LFXDUX	yl4, YL, INCY2
	fmr	ys3, ys4
	LFXDUX	yl5, YL, INCY2
	fmr	ys4, ys5

	STFXDUX	ys1, YS, INCY2
	fsmr	ys1, ys5
	STFXDUX	ys2, YS, INCY2
	fsmr	yl1, yl2
	STFXDUX	ys3, YS, INCY2
	fsmr	yl2, yl3
	STFXDUX	ys4, YS, INCY2
	fsmr	yl3, yl4

	fsmr	yl4, yl5
	bdnz	.L42
	.align 4

.L43:
	fxcpmadd  ys2, alpha1, a1,  yl1
	fxcpmadd  ys3, alpha1, a5,  yl2
	fxcpmadd  ys4, alpha1, a9,  yl3
	fxcpmadd  ys5, alpha1, a13, yl4

	fxcsmadd  ys2, alpha1, a2,  ys2
	fxcsmadd  ys3, alpha1, a6,  ys3
	fxcsmadd  ys4, alpha1, a10, ys4
	fxcsmadd  ys5, alpha1, a14, ys5

	fxcpmadd  ys2, alpha2, a3,  ys2
	fxcpmadd  ys3, alpha2, a7,  ys3
	fxcpmadd  ys4, alpha2, a11, ys4
	fxcpmadd  ys5, alpha2, a15, ys5

	fxcsmadd  ys2, alpha2, a4,  ys2
	fxcsmadd  ys3, alpha2, a8,  ys3
	fxcsmadd  ys4, alpha2, a12, ys4
	fxcsmadd  ys5, alpha2, a16, ys5

	fmr	ys1, ys2
	fmr	ys2, ys3
	fmr	ys3, ys4
	fmr	ys4, ys5
	fmr	yl1, yl5

	STFXDUX	ys1, YS, INCY2
	fsmr	ys1, ys5
	STFXDUX	ys2, YS, INCY2
	STFXDUX	ys3, YS, INCY2
	STFXDUX	ys4, YS, INCY2
	.align 4

.L45:
	andi.	r0, M, 7
	ble	.L48

	andi.	r0, M, 4
	ble	.L46

	LFXDUX	yl2, YL, INCY2
	LFXDUX	yl3, YL, INCY2

	LFPDUX	a1,  A1, INC2
	LFPDUX	a5,  A1, INC2

	LFPDUX	a2,  A2, INC2
	LFPDUX	a6,  A2, INC2
	LFPDUX	a3,  A3, INC2
	LFPDUX	a7,  A3, INC2

	LFPDUX	a4,  A4, INC2
	fsmr	yl1, yl2
	LFPDUX	a8,  A4, INC2
	fsmr	yl2, yl3

	fxcpmadd  ys2, alpha1, a1, yl1
	fxcpmadd  ys3, alpha1, a5, yl2
	fxcsmadd  ys2, alpha1, a2, ys2
	fxcsmadd  ys3, alpha1, a6, ys3

	fxcpmadd  ys2, alpha2, a3, ys2
	fxcpmadd  ys3, alpha2, a7, ys3
	fxcsmadd  ys2, alpha2, a4, ys2
	fxcsmadd  ys3, alpha2, a8, ys3

	fmr	yl1, yl3
	fmr	ys1, ys2
	fmr	ys2, ys3

	STFXDUX	ys1, YS, INCY2
	fsmr	ys1, ys3
	STFXDUX	ys2, YS, INCY2
	.align 4

.L46:
	andi.	r0, M, 2
	ble	.L47

	LFXDUX	yl2, YL, INCY2

	LFPDUX	a1,  A1, INC2
	LFPDUX	a2,  A2, INC2
	LFPDUX	a3,  A3, INC2
	LFPDUX	a4,  A4, INC2

	fsmr	yl1, yl2
	fxcpmadd  ys2, alpha1, a1, yl1
	fxcsmadd  ys2, alpha1, a2, ys2
	fxcpmadd  ys2, alpha2, a3, ys2
	fxcsmadd  ys2, alpha2, a4, ys2
	fmr	yl1, yl2

	fmr	ys1, ys2
	STFXDUX	ys1, YS, INCY2
	fsmr	ys1, ys2
	.align 4

.L47:
	andi.	r0, M, 1
	ble	.L48

	LFDUX	a1,  A1, INC2
	LFDUX	a2,  A2, INC2
	LFDUX	a3,  A3, INC2
	LFDUX	a4,  A4, INC2

	fxcpmadd  ys2, alpha1, a1, yl1
	fxcsmadd  ys2, alpha1, a2, ys2
	fxcpmadd  ys2, alpha2, a3, ys2
	fxcsmadd  ys2, alpha2, a4, ys2

	STFSDX	ys1, YS, INCY2
	add	YS, YS, INCY
	STFDX	ys2, YS, INCY2
	b	.L49
	.align 4

.L48:
	STFSDUX	ys1, YS, INCY2
	.align 4

.L49:
	addi	J, J, -1
	cmpi	cr0, 0, J, 0
	bgt	.L41
	.align 4

.L50:
	andi.	J, N, 2
	ble	.L60

	LFDUX	alpha1, X, INCX

	mr	A1, A
	add	A2, A,  LDA
	add	A,  A2, LDA
	LFSDUX	alpha1, X, INCX

	mr	YL, Y
	sub	YS, Y, INCY2
	fpmul	alpha1, alpha, alpha1

	LFSDX	ys1, YS, INCY2
	LFDX	yl1, YL, INCY

	srawi.	r0,  M, 3
	mtspr	CTR, r0
	ble	.L55

	LFPDUX	a1,  A1, INC2
	LFPDUX	a5,  A1, INC2
	LFPDUX	a9,  A1, INC2
	LFPDUX	a13, A1, INC2

	LFXDUX	yl2, YL, INCY2
	LFXDUX	yl3, YL, INCY2
 	LFXDUX	yl4, YL, INCY2
	LFXDUX	yl5, YL, INCY2

	LFPDUX	a2,  A2, INC2
	fsmr	yl1, yl2
	LFPDUX	a6,  A2, INC2
	fsmr	yl2, yl3
	LFPDUX	a10, A2, INC2
	fsmr	yl3, yl4
	LFPDUX	a14, A2, INC2
	fsmr	yl4, yl5
	bdz	.L53
	.align 4

.L52:
	fxcpmadd  ys2, alpha1, a1,  yl1
	LFPDUX	a1,  A1, INC2
	fxcpmadd  ys3, alpha1, a5,  yl2
	LFPDUX	a5,  A1, INC2
	fxcpmadd  ys4, alpha1, a9,  yl3
	LFPDUX	a9,  A1, INC2
	fxcpmadd  ys5, alpha1, a13, yl4
	LFPDUX	a13, A1, INC2

	fxcsmadd  ys2, alpha1, a2,  ys2
	LFPDUX	a2,  A2, INC2
	fxcsmadd  ys3, alpha1, a6,  ys3
	LFPDUX	a6,  A2, INC2
	fxcsmadd  ys4, alpha1, a10, ys4
	LFPDUX	a10, A2, INC2
	fxcsmadd  ys5, alpha1, a14, ys5
	LFPDUX	a14, A2, INC2

	fmr	yl1, yl5
	LFXDUX	yl2, YL, INCY2
	fmr	ys1, ys2
	LFXDUX	yl3, YL, INCY2
	fmr	ys2, ys3
 	LFXDUX	yl4, YL, INCY2
	fmr	ys3, ys4
	LFXDUX	yl5, YL, INCY2
	fmr	ys4, ys5

	STFXDUX	ys1, YS, INCY2
	fsmr	ys1, ys5
	STFXDUX	ys2, YS, INCY2
	fsmr	yl1, yl2
	STFXDUX	ys3, YS, INCY2
	fsmr	yl2, yl3
	STFXDUX	ys4, YS, INCY2
	fsmr	yl3, yl4

	fsmr	yl4, yl5
	bdnz	.L52
	.align 4

.L53:
	fxcpmadd  ys2, alpha1, a1,  yl1
	fxcpmadd  ys3, alpha1, a5,  yl2
	fxcpmadd  ys4, alpha1, a9,  yl3
	fxcpmadd  ys5, alpha1, a13, yl4

	fxcsmadd  ys2, alpha1, a2,  ys2
	fxcsmadd  ys3, alpha1, a6,  ys3
	fxcsmadd  ys4, alpha1, a10, ys4
	fxcsmadd  ys5, alpha1, a14, ys5

	fmr	yl1, yl5
	fmr	ys1, ys2
	fmr	ys2, ys3
	fmr	ys3, ys4
	fmr	ys4, ys5

	STFXDUX	ys1, YS, INCY2
	fsmr	ys1, ys5
	STFXDUX	ys2, YS, INCY2
	STFXDUX	ys3, YS, INCY2
	STFXDUX	ys4, YS, INCY2
	.align 4

.L55:
	andi.	r0, M, 7
	ble	.L59

	andi.	r0, M, 4
	ble	.L57

	LFXDUX	yl2, YL, INCY2
	LFXDUX	yl3, YL, INCY2

	LFPDUX	a1,  A1, INC2
	LFPDUX	a2,  A2, INC2

	LFPDUX	a5,  A1, INC2
	LFPDUX	a6,  A2, INC2

	fsmr	yl1, yl2
	fsmr	yl2, yl3

	fxcpmadd  ys2, alpha1, a1, yl1
	fxcsmadd  ys2, alpha1, a2, ys2
	fxcpmadd  ys3, alpha1, a5, yl2
	fxcsmadd  ys3, alpha1, a6, ys3

	fmr	yl1, yl3
	fmr	ys1, ys2
	fmr	ys2, ys3

	STFXDUX	ys1, YS, INCY2
	STFXDUX	ys2, YS, INCY2
	fsmr	  ys1, ys3
	.align 4

.L57:
	andi.	r0, M, 2
	ble	.L58

	LFXDUX	yl2, YL, INCY2
	LFPDUX	a1,  A1, INC2
	LFPDUX	a2,  A2, INC2

	fsmr	yl1, yl2
	fxcpmadd  ys2, alpha1, a1, yl1
	fxcsmadd  ys2, alpha1, a2, ys2
	fmr	yl1, yl2

	fmr	ys1, ys2
	STFXDUX	ys1, YS, INCY2
	fsmr	ys1, ys2
	.align 4

.L58:
	andi.	r0, M, 1
	ble	.L59

	LFDUX	a1,  A1, INC2
	LFDUX	a2,  A2, INC2

	fxmr	alpha2, alpha1
	fmadd	ys1, alpha1, a1, yl1
	fmadd	ys1, alpha2, a2, ys1

	STFXDUX	ys1, YS, INCY2
	b	.L60
	.align 4

.L59:
	STFSDUX	ys1, YS, INCY2
	.align 4

.L60:
	andi.	J, N, 1
	ble	.L999

	LFDUX	alpha1, X, INCX
	mr	A1, A

	mr	YL, Y
	sub	YS, Y, INCY2

	fmul	alpha1, alpha, alpha1

	LFSDX	ys1, YS, INCY2
	LFDX	yl1, YL, INCY

	srawi.	r0,  M, 3
	mtspr	CTR, r0
	ble	.L65

	LFXDUX	yl2, YL, INCY2
	LFXDUX	yl3, YL, INCY2
	LFXDUX	yl4, YL, INCY2
	LFXDUX	yl5, YL, INCY2

	LFPDUX	a1,  A1, INC2
	LFPDUX	a5,  A1, INC2
	LFPDUX	a9,  A1, INC2
	LFPDUX	a13, A1, INC2

	fsmr	yl1, yl2
	fsmr	yl2, yl3
	fsmr	yl3, yl4
	fsmr	yl4, yl5
	bdz	.L63
	.align 4

.L62:
	fxcpmadd  ys2, alpha1, a1,  yl1
	LFPDUX	a1,  A1, INC2
	fxcpmadd  ys3, alpha1, a5,  yl2
	LFXDUX	yl2, YL, INCY2
	fxcpmadd  ys4, alpha1, a9,  yl3
	LFXDUX	yl3, YL, INCY2
	fxcpmadd  ys5, alpha1, a13, yl4
	LFXDUX	yl4, YL, INCY2

	fmr	yl1, yl5
	LFXDUX	yl5, YL, INCY2
	fmr	ys1, ys2
	LFPDUX	a5,  A1, INC2
	fmr	ys2, ys3
	LFPDUX	a9,  A1, INC2
	fmr	ys3, ys4
	LFPDUX	a13, A1, INC2
	fmr	ys4, ys5

	STFXDUX	ys1, YS, INCY2
	fsmr	ys1, ys5
	STFXDUX	ys2, YS, INCY2
	fsmr	yl1, yl2
	STFXDUX	ys3, YS, INCY2
	fsmr	yl2, yl3
	STFXDUX	ys4, YS, INCY2
	fsmr	yl3, yl4

	fsmr	yl4, yl5
	bdnz	.L62
	.align 4

.L63:
	fxcpmadd  ys2, alpha1, a1,  yl1
	fxcpmadd  ys3, alpha1, a5,  yl2
	fxcpmadd  ys4, alpha1, a9,  yl3
	fxcpmadd  ys5, alpha1, a13, yl4

	fmr	yl1, yl5
	fmr	ys1, ys2
	fmr	ys2, ys3
	fmr	ys3, ys4
	fmr	ys4, ys5

	STFXDUX	ys1, YS, INCY2
	fsmr	ys1, ys5
	STFXDUX	ys2, YS, INCY2
	STFXDUX	ys3, YS, INCY2
	STFXDUX	ys4, YS, INCY2
	.align 4

.L65:
	andi.	r0, M, 7
	ble	.L69

	andi.	r0, M, 4
	ble	.L67

	LFXDUX	yl2, YL, INCY2
	LFXDUX	yl3, YL, INCY2

	LFPDUX	a1,  A1, INC2
	LFPDUX	a5,  A1, INC2

	fsmr	yl1, yl2
	fsmr	yl2, yl3

	fxcpmadd  ys2, alpha1, a1, yl1
	fxcpmadd  ys3, alpha1, a5, yl2

	fmr	yl1, yl3
	fmr	ys1, ys2
	fmr	ys2, ys3

	STFXDUX	ys1, YS, INCY2
	fsmr	  ys1, ys3
	STFXDUX	ys2, YS, INCY2
	.align 4

.L67:
	andi.	r0, M, 2
	ble	.L68

	LFPDUX	a1,  A1, INC2
	LFXDUX	yl2, YL, INCY2

	fsmr	yl1, yl2
	fxcpmadd  ys2, alpha1, a1, yl1
	fmr	yl1, yl2
	fmr	ys1, ys2
	STFXDUX	ys1, YS, INCY2
	fsmr	ys1, ys2
	.align 4

.L68:
	andi.	r0, M, 1
	ble	.L69

	LFDUX	a1,  A1, INC2
	fmadd  ys1, alpha1, a1, yl1
	STFXDUX	ys1, YS, INCY2
	b	.L999
	.align 4

.L69:
	STFSDUX	ys1, YS, INCY2
	b	.L999
	.align 4

.L70:
	sub	A, A, INC2
	sub	Y, Y, INCY
	srawi.	J, N, 2
	ble	.L80
	.align 4

.L71:
	LFDUX	alpha1, X, INCX
	mr	A1, A
	add	A2, A,  LDA
	add	A3, A2, LDA
	LFSDUX	alpha1, X, INCX
	LFDUX	alpha2, X, INCX
	add	A4, A3, LDA
	add	A,  A4, LDA
	mr	YL, Y
	LFSDUX	alpha2, X, INCX
	fpmul	alpha1, alpha, alpha1
	mr	YS, Y
	srawi.	r0,  M, 3
	mtspr	CTR, r0
	fpmul	alpha2, alpha, alpha2
	ble	.L75

	LFDUX	yl1, YL, INCY
	LFPDUX	a1,  A1, INC2
	LFPDUX	a5,  A1, INC2
	LFPDUX	a9,  A1, INC2
	LFPDUX	a13, A1, INC2
	LFSDUX	yl1, YL, INCY

	LFDUX	yl2, YL, INCY
	LFPDUX	a2,  A2, INC2
	LFPDUX	a6,  A2, INC2
	LFPDUX	a10, A2, INC2
	LFPDUX	a14, A2, INC2
	LFSDUX	yl2, YL, INCY

	LFDUX	yl3, YL, INCY
	LFPDUX	a3,  A3, INC2
	LFPDUX	a7,  A3, INC2
	LFPDUX	a11, A3, INC2
	LFPDUX	a15, A3, INC2
	LFSDUX	yl3, YL, INCY

	LFDUX	yl4, YL, INCY
	LFPDUX	a4,  A4, INC2
	LFPDUX	a8,  A4, INC2
	LFPDUX	a12, A4, INC2
	LFPDUX	a16, A4, INC2
	LFSDUX	yl4, YL, INCY
	bdz	.L73
	.align 4

.L72:
	fxcpmadd  ys1, alpha1, a1,  yl1
	LFPDUX	a1,  A1, INC2
	LFDUX	yl1, YL, INCY
	fxcpmadd  ys2, alpha1, a5,  yl2
	LFPDUX	a5,  A1, INC2
	fxcpmadd  ys3, alpha1, a9,  yl3
	LFPDUX	a9,  A1, INC2
	fxcpmadd  ys4, alpha1, a13, yl4
	LFPDUX	a13, A1, INC2
	LFSDUX	yl1, YL, INCY

	fxcsmadd  ys1, alpha1, a2,  ys1
	LFPDUX	a2,  A2, INC2
	LFDUX	yl2, YL, INCY
	fxcsmadd  ys2, alpha1, a6,  ys2
	LFPDUX	a6,  A2, INC2
	fxcsmadd  ys3, alpha1, a10, ys3
	LFPDUX	a10, A2, INC2
	fxcsmadd  ys4, alpha1, a14, ys4
	LFPDUX	a14, A2, INC2
	LFSDUX	yl2, YL, INCY

	fxcpmadd  ys1, alpha2, a3,  ys1
	LFPDUX	a3,  A3, INC2
	LFDUX	yl3, YL, INCY
	fxcpmadd  ys2, alpha2, a7,  ys2
	LFPDUX	a7,  A3, INC2
	fxcpmadd  ys3, alpha2, a11, ys3
	LFPDUX	a11, A3, INC2
	fxcpmadd  ys4, alpha2, a15, ys4
	LFPDUX	a15, A3, INC2
	LFSDUX	yl3, YL, INCY

	fxcsmadd  ys1, alpha2, a4,  ys1
	LFPDUX	a4,  A4, INC2
	LFDUX	yl4, YL, INCY
	fxcsmadd  ys2, alpha2, a8,  ys2
	LFPDUX	a8,  A4, INC2
	fxcsmadd  ys3, alpha2, a12, ys3
	LFPDUX	a12, A4, INC2
	fxcsmadd  ys4, alpha2, a16, ys4
	LFPDUX	a16, A4, INC2
	LFSDUX	yl4, YL, INCY

	STFDUX	ys1, YS, INCY
	STFSDUX	ys1, YS, INCY
	STFDUX	ys2, YS, INCY
	STFSDUX	ys2, YS, INCY
	STFDUX	ys3, YS, INCY
	STFSDUX	ys3, YS, INCY
	STFDUX	ys4, YS, INCY
	STFSDUX	ys4, YS, INCY
	bdnz	.L72
	.align 4

.L73:
	fxcpmadd  ys1, alpha1, a1,  yl1
	fxcpmadd  ys2, alpha1, a5,  yl2
	fxcpmadd  ys3, alpha1, a9,  yl3
	fxcpmadd  ys4, alpha1, a13, yl4

	fxcsmadd  ys1, alpha1, a2,  ys1
	fxcsmadd  ys2, alpha1, a6,  ys2
	fxcsmadd  ys3, alpha1, a10, ys3
	fxcsmadd  ys4, alpha1, a14, ys4

	fxcpmadd  ys1, alpha2, a3,  ys1
	fxcpmadd  ys2, alpha2, a7,  ys2
	fxcpmadd  ys3, alpha2, a11, ys3
	fxcpmadd  ys4, alpha2, a15, ys4

	fxcsmadd  ys1, alpha2, a4,  ys1
	fxcsmadd  ys2, alpha2, a8,  ys2
	fxcsmadd  ys3, alpha2, a12, ys3
	fxcsmadd  ys4, alpha2, a16, ys4

	STFDUX	ys1, YS, INCY
	STFSDUX	ys1, YS, INCY
	STFDUX	ys2, YS, INCY
	STFSDUX	ys2, YS, INCY
	STFDUX	ys3, YS, INCY
	STFSDUX	ys3, YS, INCY
	STFDUX	ys4, YS, INCY
	STFSDUX	ys4, YS, INCY
	.align 4

.L75:
	andi.	r0, M, 7
	ble	.L79

	andi.	r0, M, 4
	ble	.L77

	LFDUX	yl1, YL, INCY
	LFPDUX	a1,  A1, INC2
	LFPDUX	a5,  A1, INC2
	LFSDUX	yl1, YL, INCY
	LFPDUX	a2,  A2, INC2
	LFPDUX	a6,  A2, INC2

	LFDUX	yl2, YL, INCY
	LFPDUX	a3,  A3, INC2
	LFPDUX	a7,  A3, INC2
	LFSDUX	yl2, YL, INCY
	LFPDUX	a4,  A4, INC2
	LFPDUX	a8,  A4, INC2

	fxcpmadd  ys1, alpha1, a1, yl1
	fxcpmadd  ys2, alpha1, a5, yl2
	fxcsmadd  ys1, alpha1, a2, ys1
	fxcsmadd  ys2, alpha1, a6, ys2

	fxcpmadd  ys1, alpha2, a3, ys1
	fxcpmadd  ys2, alpha2, a7, ys2
	fxcsmadd  ys1, alpha2, a4, ys1
	fxcsmadd  ys2, alpha2, a8, ys2

	STFDUX	ys1, YS, INCY
	STFSDUX	ys1, YS, INCY
	STFDUX	ys2, YS, INCY
	STFSDUX	ys2, YS, INCY
	.align 4

.L77:
	andi.	r0, M, 2
	ble	.L78

	LFDUX	yl1, YL, INCY
	LFPDUX	a1,  A1, INC2
	LFPDUX	a2,  A2, INC2
	LFSDUX	yl1, YL, INCY
	LFPDUX	a3,  A3, INC2
	LFPDUX	a4,  A4, INC2

	fxcpmadd  ys1, alpha1, a1, yl1
	fxcsmadd  ys1, alpha1, a2, ys1
	fxcpmadd  ys1, alpha2, a3, ys1
	fxcsmadd  ys1, alpha2, a4, ys1

	STFDUX	ys1, YS, INCY
	STFSDUX	ys1, YS, INCY
	.align 4

.L78:
	andi.	r0, M, 1
	ble	.L79

	LFDUX	yl1, YL, INCY

	LFDUX	a1,  A1, INC2
	LFDUX	a2,  A2, INC2
	LFDUX	a3,  A3, INC2
	LFDUX	a4,  A4, INC2

	fxcpmadd  ys1, alpha1, a1, yl1
	fxcsmadd  ys1, alpha1, a2, ys1
	fxcpmadd  ys1, alpha2, a3, ys1
	fxcsmadd  ys1, alpha2, a4, ys1

	STFDUX	ys1, YS, INCY
	.align 4

.L79:
	addi	J, J, -1
	cmpi	cr0, 0, J, 0
	bgt	.L71
	.align 4

.L80:
	andi.	J, N, 2
	ble	.L90

	LFDUX	alpha1, X, INCX

	mr	A1, A
	add	A2, A,  LDA
	add	A,  A2, LDA
	LFSDUX	alpha1, X, INCX

	mr	YL, Y
	mr	YS, Y
	fpmul	alpha1, alpha, alpha1

	srawi.	r0,  M, 3
	mtspr	CTR, r0
	ble	.L85

	LFDUX	yl1, YL, INCY
	LFDUX	a9,  YL, INCY
	LFDUX	yl2, YL, INCY
	LFDUX	a10, YL, INCY

	LFPDUX	a1,  A1, INC2
	LFPDUX	a5,  A1, INC2
	LFPDUX	a3,  A1, INC2
	LFPDUX	a7,  A1, INC2

	LFDUX	yl3, YL, INCY
	LFDUX	a11, YL, INCY
	LFDUX	yl4, YL, INCY
	LFDUX	a12, YL, INCY

	LFPDUX	a2,  A2, INC2
	LFPDUX	a6,  A2, INC2
	LFPDUX	a4,  A2, INC2
	LFPDUX	a8,  A2, INC2

	bdz	.L83
	.align 4

.L82:
	fsmfp	yl1, a9
	fsmfp	yl2, a10
	fsmfp	yl3, a11
	fsmfp	yl4, a12

	fxcpmadd  ys1, alpha1, a1,  yl1
	LFDUX	yl1, YL, INCY
	LFDUX	a9,  YL, INCY
	LFPDUX	a1,  A1, INC2
	fxcpmadd  ys2, alpha1, a5,  yl2
	LFDUX	yl2, YL, INCY
	LFDUX	a10, YL, INCY
	LFPDUX	a5,  A1, INC2
	fxcpmadd  ys3, alpha1, a3,  yl3
	LFDUX	yl3, YL, INCY
	LFDUX	a11, YL, INCY
	LFPDUX	a3,  A1, INC2
	fxcpmadd  ys4, alpha1, a7,  yl4
	LFDUX	yl4, YL, INCY
	LFDUX	a12, YL, INCY
	LFPDUX	a7,  A1, INC2

	fxcsmadd  ys1, alpha1, a2,  ys1
	LFPDUX	a2,  A2, INC2
	fxcsmadd  ys2, alpha1, a6,  ys2
	LFPDUX	a6,  A2, INC2
	fxcsmadd  ys3, alpha1, a4,  ys3
	LFPDUX	a4,  A2, INC2
	fxcsmadd  ys4, alpha1, a8,  ys4
	LFPDUX	a8,  A2, INC2

	STFDUX	ys1, YS, INCY
	STFSDUX	ys1, YS, INCY
	STFDUX	ys2, YS, INCY
	STFSDUX	ys2, YS, INCY

	STFDUX	ys3, YS, INCY
	STFSDUX	ys3, YS, INCY
	STFDUX	ys4, YS, INCY
	STFSDUX	ys4, YS, INCY
	bdnz	.L82
	.align 4

.L83:
	fsmfp	yl1, a9
	fsmfp	yl2, a10
	fsmfp	yl3, a11
	fsmfp	yl4, a12

	fxcpmadd  ys1, alpha1, a1,  yl1
	fxcpmadd  ys2, alpha1, a5,  yl2
	fxcpmadd  ys3, alpha1, a3,  yl3
	fxcpmadd  ys4, alpha1, a7,  yl4

	fxcsmadd  ys1, alpha1, a2,  ys1
	fxcsmadd  ys2, alpha1, a6,  ys2
	fxcsmadd  ys3, alpha1, a4,  ys3
	fxcsmadd  ys4, alpha1, a8,  ys4

	STFDUX	ys1, YS, INCY
	STFSDUX	ys1, YS, INCY
	STFDUX	ys2, YS, INCY
	STFSDUX	ys2, YS, INCY
	STFDUX	ys3, YS, INCY
	STFSDUX	ys3, YS, INCY
	STFDUX	ys4, YS, INCY
	STFSDUX	ys4, YS, INCY
	.align 4

.L85:
	andi.	r0, M, 7
	ble	.L90

	andi.	r0, M, 4
	ble	.L87

	LFDUX	yl1, YL, INCY
	LFPDUX	a1,  A1, INC2
	LFPDUX	a2,  A2, INC2
	LFSDUX	yl1, YL, INCY
	LFDUX	yl2, YL, INCY
	LFPDUX	a5,  A1, INC2
	LFPDUX	a6,  A2, INC2
	LFSDUX	yl2, YL, INCY

	fxcpmadd  ys1, alpha1, a1, yl1
	fxcpmadd  ys2, alpha1, a5, yl2
	fxcsmadd  ys1, alpha1, a2, ys1
	fxcsmadd  ys2, alpha1, a6, ys2

	STFDUX	ys1, YS, INCY
	STFSDUX	ys1, YS, INCY
	STFDUX	ys2, YS, INCY
	STFSDUX	ys2, YS, INCY
	.align 4

.L87:
	andi.	r0, M, 2
	ble	.L88

	LFDUX	yl1, YL, INCY
	LFPDUX	a1,  A1, INC2
	LFPDUX	a2,  A2, INC2
	LFSDUX	yl1, YL, INCY

	fxcpmadd  ys1, alpha1, a1, yl1
	fxcsmadd  ys1, alpha1, a2, ys1

	STFDUX	ys1, YS, INCY
	STFSDUX	ys1, YS, INCY
	.align 4

.L88:
	andi.	r0, M, 1
	ble	.L90

	LFDUX	yl1, YL, INCY
	LFDUX	a1,  A1, INC2
	LFDUX	a2,  A2, INC2

	fxcpmadd  ys1, alpha1, a1, yl1
	fxcsmadd  ys1, alpha1, a2, ys1

	STFDUX	ys1, YS, INCY
	.align 4

.L90:
	andi.	J, N, 1
	ble	.L999

	LFDUX	alpha1, X, INCX

	mr	A1, A
	mr	YL, Y
	mr	YS, Y
	fmul	alpha1, alpha, alpha1

	srawi.	r0,  M, 3
	mtspr	CTR, r0
	ble	.L95

	LFDUX	yl1, YL, INCY
	LFSDUX	a2,  YL, INCY
	LFDUX	yl2, YL, INCY
	LFSDUX	a4,  YL, INCY
	LFDUX	yl3, YL, INCY
	LFSDUX	a6,  YL, INCY
	LFDUX	yl4, YL, INCY
	LFSDUX	a8,  YL, INCY

	LFPDUX	a1,  A1, INC2
	LFPDUX	a5,  A1, INC2
	LFPDUX	a9,  A1, INC2
	LFPDUX	a13, A1, INC2
	bdz	.L93
	.align 4

.L92:
	fmr	a2, yl1
	fmr	a4, yl2
	fmr	a6, yl3
	fmr	a8, yl4

	fxcpmadd  ys1, alpha1, a1,  a2
	LFDUX	yl1, YL, INCY
	LFSDUX	a2,  YL, INCY
	fxcpmadd  ys2, alpha1, a5,  a4
	LFDUX	yl2, YL, INCY
	LFSDUX	a4,  YL, INCY
	fxcpmadd  ys3, alpha1, a9,  a6
	LFDUX	yl3, YL, INCY
	LFSDUX	a6,  YL, INCY
	fxcpmadd  ys4, alpha1, a13, a8
	LFDUX	yl4, YL, INCY
	LFSDUX	a8,  YL, INCY

	LFPDUX	a1,  A1, INC2
	LFPDUX	a5,  A1, INC2
	LFPDUX	a9,  A1, INC2
	LFPDUX	a13, A1, INC2

	STFDUX	ys1, YS, INCY
	STFSDUX	ys1, YS, INCY
	STFDUX	ys2, YS, INCY
	STFSDUX	ys2, YS, INCY
	STFDUX	ys3, YS, INCY
	STFSDUX	ys3, YS, INCY
	STFDUX	ys4, YS, INCY
	STFSDUX	ys4, YS, INCY
	bdnz	.L92
	.align 4

.L93:
	fmr	a2, yl1
	fmr	a4, yl2
	fmr	a6, yl3
	fmr	a8, yl4

	fxcpmadd  ys1, alpha1, a1,  a2
	fxcpmadd  ys2, alpha1, a5,  a4
	fxcpmadd  ys3, alpha1, a9,  a6
	fxcpmadd  ys4, alpha1, a13, a8

	STFDUX	ys1, YS, INCY
	STFSDUX	ys1, YS, INCY
	STFDUX	ys2, YS, INCY
	STFSDUX	ys2, YS, INCY
	STFDUX	ys3, YS, INCY
	STFSDUX	ys3, YS, INCY
	STFDUX	ys4, YS, INCY
	STFSDUX	ys4, YS, INCY
	.align 4

.L95:
	andi.	r0, M, 7
	ble	.L999

	andi.	r0, M, 4
	ble	.L97

	LFPDUX	a1,  A1, INC2
	LFDUX	yl1, YL, INCY
	LFDUX	yl2, YL, INCY
	LFPDUX	a2,  A1, INC2
	LFDUX	yl3, YL, INCY
	LFDUX	yl4, YL, INCY

	fxcpmadd  ys1, a1, alpha1, yl1
	fxcsmadd  ys2, a1, alpha1, yl2
	fxcpmadd  ys3, a2, alpha1, yl3
	fxcsmadd  ys4, a2, alpha1, yl4

	STFDUX	ys1, YS, INCY
	STFDUX	ys2, YS, INCY
	STFDUX	ys3, YS, INCY
	STFDUX	ys4, YS, INCY
	.align 4

.L97:
	andi.	r0, M, 2
	ble	.L98

	LFPDUX	a1,  A1, INC2
	LFDUX	yl1, YL, INCY
	LFDUX	yl2, YL, INCY

	fxcpmadd  ys1, a1, alpha1, yl1
	fxcsmadd  ys2, a1, alpha1, yl2

	STFDUX	ys1, YS, INCY
	STFDUX	ys2, YS, INCY
	.align 4

.L98:
	andi.	r0, M, 1
	ble	.L999

	LFDUX	yl1, YL, INCY
	LFDUX	a1,  A1, INC2

	fxcpmadd  ys1, alpha1, a1, yl1

	STFDUX	ys1, YS, INCY
	b	.L999
	.align 4


.L999:
	addi	SP, SP, -4

	lwzu	r16,   4(SP)
	lwzu	r17,   4(SP)
	lwzu	r18,   4(SP)
	lwzu	r19,   4(SP)

	lwzu	r20,   4(SP)
	lwzu	r21,   4(SP)
	lwzu	r22,   4(SP)
	lwzu	r23,   4(SP)

	lwzu	r24,   4(SP)
	lwzu	r25,   4(SP)
	lwzu	r26,   4(SP)
	lwzu	r27,   4(SP)

	lwzu	r28,   4(SP)
	lwzu	r29,   4(SP)
	lwzu	r30,   4(SP)
	lwzu	r31,   4(SP)

	subi	SP, SP, 12
	li	r0, 16

	lfpdux	f31, SP, r0
	lfpdux	f30, SP, r0
	lfpdux	f29, SP, r0
	lfpdux	f28, SP, r0
	lfpdux	f27, SP, r0
	lfpdux	f26, SP, r0
	lfpdux	f25, SP, r0
	lfpdux	f24, SP, r0
	lfpdux	f23, SP, r0
	lfpdux	f22, SP, r0
	lfpdux	f21, SP, r0
	lfpdux	f20, SP, r0
	lfpdux	f19, SP, r0
	lfpdux	f18, SP, r0
	lfpdux	f17, SP, r0
	lfpdux	f16, SP, r0
	lfpdux	f15, SP, r0
	lfpdux	f14, SP, r0
	addi	SP, SP, 16
	blr

	EPILOGUE

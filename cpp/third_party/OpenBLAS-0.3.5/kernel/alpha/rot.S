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
#include "version.h"

#define N	$16
#define	X	$17
#define INCX	$18
#define Y	$19
#define INCY	$20
#define I	$21
#define XX	$23
#define YY	$24

#define C	$f10
#define S	$f11

#define PREFETCH_SIZE 80

	PROLOGUE
	PROFCODE
	.frame	$sp, 0, $26, 0

#ifndef PROFILE
	.prologue 0
#else
	.prologue 1
#endif

	fmov	$f21,   C
	LD	S, 0($sp)

	cmpeq	INCX, 1,  $23
	cmpeq	INCY, 1,  $24
	ble	N,  $L998

	and	$23, $24, $23
	beq	$23, $L50

	sra	N, 3, I
	ble	I, $L15

	LD	$f12,   0*SIZE(X)
	LD	$f13,   0*SIZE(Y)
	LD	$f14,   1*SIZE(X)
	LD	$f15,   1*SIZE(Y)

	LD	$f16,   2*SIZE(X)
	LD	$f17,   2*SIZE(Y)
	LD	$f18,   3*SIZE(X)
	LD	$f19,   3*SIZE(Y)

	MUL	C, $f12, $f21
	unop
	MUL	S, $f13, $f22
	MUL	C, $f13, $f23

	LD	$f13,   4*SIZE(Y)
	MUL	S, $f12, $f24
	LD	$f12,   4*SIZE(X)
	MUL	C, $f14, $f25

	lda	I, -1(I)
	MUL	S, $f15, $f26
	ADD	$f21, $f22, $f22
	MUL	C, $f15, $f27

	LD	$f15,   5*SIZE(Y)
	MUL	S, $f14, $f28
	SUB	$f23, $f24, $f24
	ble	I, $L13
	.align 4

$L12:
	MUL	C, $f16, $f21
	lds	$f31, (PREFETCH_SIZE) * SIZE(X)
	unop
	LD	$f14,   5*SIZE(X)

	ST	$f22,   0*SIZE(X)
	MUL	S, $f17, $f22
	unop
	ADD	$f25, $f26, $f26

	MUL	C, $f17, $f23
	lds	$f31, (PREFETCH_SIZE) * SIZE(Y)
	unop
	LD	$f17,   6*SIZE(Y)

	ST	$f24,   0*SIZE(Y)
	MUL	S, $f16, $f24
	unop
	SUB	$f27, $f28, $f28

	MUL	C, $f18, $f25
	LD	$f16,   6*SIZE(X)
	unop
	unop

	ST	$f26,   1*SIZE(X)
	MUL	S, $f19, $f26
	unop
	ADD	$f21, $f22, $f22

	MUL	C, $f19, $f27
	unop
	unop
	LD	$f19,   7*SIZE(Y)

	ST	$f28,   1*SIZE(Y)
	MUL	S, $f18, $f28
	unop
	SUB	$f23, $f24, $f24

	MUL	C, $f12, $f21
	LD	$f18,   7*SIZE(X)
	unop
	unop

	ST	$f22,   2*SIZE(X)
	unop
	MUL	S, $f13, $f22
	ADD	$f25, $f26, $f26

	MUL	C, $f13, $f23
	LD	$f13,   8*SIZE(Y)
	unop
	unop

	ST	$f24,   2*SIZE(Y)
	MUL	S, $f12, $f24
	unop
	SUB	$f27, $f28, $f28

	MUL	C, $f14, $f25
	LD	$f12,   8*SIZE(X)
	unop
	unop

	ST	$f26,   3*SIZE(X)
	MUL	S, $f15, $f26
	unop
	ADD	$f21, $f22, $f22

	MUL	C, $f15, $f27
	LD	$f15,   9*SIZE(Y)
	unop
	unop

	ST	$f28,   3*SIZE(Y)
	MUL	S, $f14, $f28
	unop
	SUB	$f23, $f24, $f24

	MUL	C, $f16, $f21
	LD	$f14,   9*SIZE(X)
	unop
	unop

	ST	$f22,   4*SIZE(X)
	MUL	S, $f17, $f22
	unop
	ADD	$f25, $f26, $f26

	MUL	C, $f17, $f23
	LD	$f17,  10*SIZE(Y)
	unop
	unop

	ST	$f24,   4*SIZE(Y)
	MUL	S, $f16, $f24
	unop
	SUB	$f27, $f28, $f28

	MUL	C, $f18, $f25
	LD	$f16,  10*SIZE(X)
	unop
	unop

	ST	$f26,   5*SIZE(X)
	MUL	S, $f19, $f26
	unop
	ADD	$f21, $f22, $f22

	MUL	C, $f19, $f27
	LD	$f19,  11*SIZE(Y)
	unop
	unop

	ST	$f28,   5*SIZE(Y)
	MUL	S, $f18, $f28
	lda	I, -1(I)
	SUB	$f23, $f24, $f24

	MUL	C, $f12, $f21
	LD	$f18,  11*SIZE(X)
	unop
	unop

	ST	$f22,   6*SIZE(X)
	MUL	S, $f13, $f22
	unop
	ADD	$f25, $f26, $f26

	MUL	C, $f13, $f23
	LD	$f13,  12*SIZE(Y)
	lda	X,   8*SIZE(X)
	unop

	ST	$f24,   6*SIZE(Y)
	MUL	S, $f12, $f24
	unop
	SUB	$f27, $f28, $f28

	MUL	C, $f14, $f25
	LD	$f12,   4*SIZE(X)
	lda	Y,   8*SIZE(Y)
	unop

	ST	$f26,  -1*SIZE(X)
	MUL	S, $f15, $f26
	unop
	ADD	$f21, $f22, $f22

	MUL	C, $f15, $f27
	LD	$f15,   5*SIZE(Y)
	unop
	unop

	ST	$f28,  -1*SIZE(Y)
	MUL	S, $f14, $f28
	SUB	$f23, $f24, $f24
	bgt	I, $L12
	.align 4

$L13:
	MUL	C, $f16, $f21
	LD	$f14,   5*SIZE(X)
	unop
	unop

	ST	$f22,   0*SIZE(X)
	MUL	S, $f17, $f22
	unop
	ADD	$f25, $f26, $f26

	MUL	C, $f17, $f23
	unop
	unop
	LD	$f17,   6*SIZE(Y)

	ST	$f24,   0*SIZE(Y)
	MUL	S, $f16, $f24
	LD	$f16,   6*SIZE(X)
	SUB	$f27, $f28, $f28

	MUL	C, $f18, $f25
	unop
	unop
	unop

	ST	$f26,   1*SIZE(X)
	MUL	S, $f19, $f26
	unop
	ADD	$f21, $f22, $f22

	MUL	C, $f19, $f27
	unop
	unop
	LD	$f19,   7*SIZE(Y)

	ST	$f28,   1*SIZE(Y)
	MUL	S, $f18, $f28
	LD	$f18,   7*SIZE(X)
	SUB	$f23, $f24, $f24

	MUL	C, $f12, $f21
	unop
	unop
	unop

	ST	$f22,   2*SIZE(X)
	unop
	MUL	S, $f13, $f22
	ADD	$f25, $f26, $f26

	MUL	C, $f13, $f23
	unop
	unop
	unop

	ST	$f24,   2*SIZE(Y)
	MUL	S, $f12, $f24
	unop
	SUB	$f27, $f28, $f28

	MUL	C, $f14, $f25
	unop
	unop
	unop

	ST	$f26,   3*SIZE(X)
	MUL	S, $f15, $f26
	unop
	ADD	$f21, $f22, $f22

	MUL	C, $f15, $f27
	unop
	unop
	unop

	ST	$f28,   3*SIZE(Y)
	MUL	S, $f14, $f28
	unop
	SUB	$f23, $f24, $f24

	MUL	C, $f16, $f21
	unop
	unop
	unop

	ST	$f22,   4*SIZE(X)
	MUL	S, $f17, $f22
	unop
	ADD	$f25, $f26, $f26

	MUL	C, $f17, $f23
	unop
	unop
	unop

	ST	$f24,   4*SIZE(Y)
	MUL	S, $f16, $f24
	unop
	SUB	$f27, $f28, $f28

	MUL	C, $f18, $f25
	unop
	unop
	unop

	ST	$f26,   5*SIZE(X)
	MUL	S, $f19, $f26
	unop
	ADD	$f21, $f22, $f22

	MUL	C, $f19, $f27
	unop
	unop
	unop

	ST	$f28,   5*SIZE(Y)
	MUL	S, $f18, $f28
	unop
	SUB	$f23, $f24, $f24

	ST	$f22,   6*SIZE(X)
	ADD	$f25, $f26, $f26
	ST	$f24,   6*SIZE(Y)
	SUB	$f27, $f28, $f28

	ST	$f26,   7*SIZE(X)
	lda	X,   8*SIZE(X)
	ST	$f28,   7*SIZE(Y)
	lda	Y,   8*SIZE(Y)
	.align 4


$L15:
	and	N, 7, I
	ble	I, $L998
	.align 4

$L16:
	LD	$f12,   0*SIZE(X)
	LD	$f13,   0*SIZE(Y)

	MUL	C, $f12, $f21
	MUL	S, $f13, $f22
	MUL	C, $f13, $f23
	MUL	S, $f12, $f24

	ADD	$f21, $f22, $f25
	SUB	$f23, $f24, $f26
	lda	I, -1(I)

	ST	$f25,   0*SIZE(X)
	lda	X, 1 * SIZE(X)
	ST	$f26,   0*SIZE(Y)
	lda	Y, 1 * SIZE(Y)

	bgt	I, $L16
	.align 4

$L998:
	clr	$0
	ret
	.align 4

$L50:
	mov	X, XX
	mov	Y, YY

	sra	N, 3, I
	ble	I, $L55
	.align 4

$L51:
	LD	$f12,   0*SIZE(X)
	SXADDQ	INCX, X, X
	LD	$f13,   0*SIZE(Y)
	SXADDQ	INCY, Y, Y

	LD	$f14,   0*SIZE(X)
	SXADDQ	INCX, X, X
	LD	$f15,   0*SIZE(Y)
	SXADDQ	INCY, Y, Y

	LD	$f16,   0*SIZE(X)
	SXADDQ	INCX, X, X
	LD	$f17,   0*SIZE(Y)
	SXADDQ	INCY, Y, Y

	LD	$f18,   0*SIZE(X)
	SXADDQ	INCX, X, X
	LD	$f19,   0*SIZE(Y)
	SXADDQ	INCY, Y, Y

	MUL	C, $f12, $f21
	MUL	S, $f13, $f22
	MUL	C, $f13, $f23
	MUL	S, $f12, $f24

	ADD	$f21, $f22, $f22
	SUB	$f23, $f24, $f24

	ST	$f22,   0*SIZE(XX)
	SXADDQ	INCX, XX, XX
	ST	$f24,   0*SIZE(YY)
	SXADDQ	INCY, YY, YY

	MUL	C, $f14, $f25
	MUL	S, $f15, $f26
	MUL	C, $f15, $f27
	MUL	S, $f14, $f28

	ADD	$f25, $f26, $f26
	SUB	$f27, $f28, $f28

	ST	$f26,   0*SIZE(XX)
	SXADDQ	INCX, XX, XX
	ST	$f28,   0*SIZE(YY)
	SXADDQ	INCY, YY, YY

	MUL	C, $f16, $f21
	MUL	S, $f17, $f22
	MUL	C, $f17, $f23
	MUL	S, $f16, $f24

	ADD	$f21, $f22, $f22
	SUB	$f23, $f24, $f24

	ST	$f22,   0*SIZE(XX)
	SXADDQ	INCX, XX, XX
	ST	$f24,   0*SIZE(YY)
	SXADDQ	INCY, YY, YY

	MUL	C, $f18, $f25
	MUL	S, $f19, $f26
	MUL	C, $f19, $f27
	MUL	S, $f18, $f28

	ADD	$f25, $f26, $f26
	SUB	$f27, $f28, $f28

	ST	$f26,   0*SIZE(XX)
	SXADDQ	INCX, XX, XX
	ST	$f28,   0*SIZE(YY)
	SXADDQ	INCY, YY, YY


	LD	$f12,   0*SIZE(X)
	SXADDQ	INCX, X, X
	LD	$f13,   0*SIZE(Y)
	SXADDQ	INCY, Y, Y

	LD	$f14,   0*SIZE(X)
	SXADDQ	INCX, X, X
	LD	$f15,   0*SIZE(Y)
	SXADDQ	INCY, Y, Y

	LD	$f16,   0*SIZE(X)
	SXADDQ	INCX, X, X
	LD	$f17,   0*SIZE(Y)
	SXADDQ	INCY, Y, Y

	LD	$f18,   0*SIZE(X)
	SXADDQ	INCX, X, X
	LD	$f19,   0*SIZE(Y)
	SXADDQ	INCY, Y, Y

	MUL	C, $f12, $f21
	MUL	S, $f13, $f22
	MUL	C, $f13, $f23
	MUL	S, $f12, $f24

	ADD	$f21, $f22, $f22
	SUB	$f23, $f24, $f24

	ST	$f22,   0*SIZE(XX)
	SXADDQ	INCX, XX, XX
	ST	$f24,   0*SIZE(YY)
	SXADDQ	INCY, YY, YY

	MUL	C, $f14, $f25
	MUL	S, $f15, $f26
	MUL	C, $f15, $f27
	MUL	S, $f14, $f28

	ADD	$f25, $f26, $f26
	SUB	$f27, $f28, $f28

	ST	$f26,   0*SIZE(XX)
	SXADDQ	INCX, XX, XX
	ST	$f28,   0*SIZE(YY)
	SXADDQ	INCY, YY, YY

	MUL	C, $f16, $f21
	MUL	S, $f17, $f22
	MUL	C, $f17, $f23
	MUL	S, $f16, $f24

	ADD	$f21, $f22, $f22
	SUB	$f23, $f24, $f24

	ST	$f22,   0*SIZE(XX)
	SXADDQ	INCX, XX, XX
	ST	$f24,   0*SIZE(YY)
	SXADDQ	INCY, YY, YY

	MUL	C, $f18, $f25
	MUL	S, $f19, $f26
	MUL	C, $f19, $f27
	MUL	S, $f18, $f28

	ADD	$f25, $f26, $f26
	SUB	$f27, $f28, $f28

	ST	$f26,   0*SIZE(XX)
	SXADDQ	INCX, XX, XX
	ST	$f28,   0*SIZE(YY)
	SXADDQ	INCY, YY, YY

	lda	I, -1(I)
	bgt	I, $L51
	.align 4

$L55:
	and	N, 7, I
	ble	I, $L999
	.align 4

$L56:
	LD	$f12,   0*SIZE(X)
	LD	$f13,   0*SIZE(Y)

	MUL	C, $f12, $f21
	MUL	S, $f13, $f22
	MUL	C, $f13, $f23
	MUL	S, $f12, $f24

	ADD	$f21, $f22, $f25
	SUB	$f23, $f24, $f26
	lda	I, -1(I)

	ST	$f25,   0*SIZE(X)
	SXADDQ	INCX, X, X
	ST	$f26,   0*SIZE(Y)
	SXADDQ	INCY, Y, Y

	bgt	I, $L56
	.align 4

$L999:
	clr	$0
	ret
	EPILOGUE

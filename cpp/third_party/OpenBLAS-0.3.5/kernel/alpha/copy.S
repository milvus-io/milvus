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
#define X	$17
#define INCX	$18
#define Y	$19
#define INCY	$20

	PROLOGUE
	PROFCODE
	.frame	$sp, 0, $26, 0

#ifndef PROFILE
	.prologue 0
#else
	.prologue 1
#endif

	cmpeq	INCX,  1, $0
	ble	N, $End
#ifndef COMPLEX
	sra	N,  4, $4
#else
	sra	N,  3, $4
#endif
	cmpeq	INCY,  1, $1

	and	$0,  $1, $0
	beq	$0, $Sub
#ifndef COMPLEX
	and	N, 15, $5
#else
	and	N,  7, $5
#endif
	ble	$4,  $Remain

	LD	$f10,  0*SIZE(X)
	LD	$f11,  1*SIZE(X)
	LD	$f12,  2*SIZE(X)
	LD	$f13,  3*SIZE(X)
	LD	$f14,  4*SIZE(X)
	LD	$f15,  5*SIZE(X)
	LD	$f16,  6*SIZE(X)
	LD	$f17,  7*SIZE(X)

	LD	$f18,  8*SIZE(X)
	LD	$f19,  9*SIZE(X)
	LD	$f20, 10*SIZE(X)
	LD	$f21, 11*SIZE(X)
	LD	$f22, 12*SIZE(X)
	LD	$f23, 13*SIZE(X)
	LD	$f24, 14*SIZE(X)
	LD	$f25, 15*SIZE(X)

	subq	$4, 1, $4
	lda	X, 16*SIZE(X)
	ble	$4, $MainLoopEnd
	.align 4

$MainLoop:
	ST	$f10,  0*SIZE(Y)
	ST	$f11,  1*SIZE(Y)
	ST	$f12,  2*SIZE(Y)
	ST	$f13,  3*SIZE(Y)

	LD	$f10,  0*SIZE(X)
	LD	$f11,  1*SIZE(X)
	LD	$f12,  2*SIZE(X)
	LD	$f13,  3*SIZE(X)

	ST	$f14,  4*SIZE(Y)
	ST	$f15,  5*SIZE(Y)
	ST	$f16,  6*SIZE(Y)
	ST	$f17,  7*SIZE(Y)

	LD	$f14,  4*SIZE(X)
	LD	$f15,  5*SIZE(X)
	LD	$f16,  6*SIZE(X)
	LD	$f17,  7*SIZE(X)

	ST	$f18,  8*SIZE(Y)
	ST	$f19,  9*SIZE(Y)
	ST	$f20, 10*SIZE(Y)
	ST	$f21, 11*SIZE(Y)

	LD	$f18,  8*SIZE(X)
	LD	$f19,  9*SIZE(X)
	LD	$f20, 10*SIZE(X)
	LD	$f21, 11*SIZE(X)

	ST	$f22, 12*SIZE(Y)
	ST	$f23, 13*SIZE(Y)
	ST	$f24, 14*SIZE(Y)
	ST	$f25, 15*SIZE(Y)

	LD	$f22, 12*SIZE(X)
	LD	$f23, 13*SIZE(X)
	LD	$f24, 14*SIZE(X)
	LD	$f25, 15*SIZE(X)

	subq	$4, 1, $4
	lda	Y, 16*SIZE(Y)
	lda	X, 16*SIZE(X)
	bgt	$4, $MainLoop
	.align 4

$MainLoopEnd:
	ST	$f10,  0*SIZE(Y)
	ST	$f11,  1*SIZE(Y)
	ST	$f12,  2*SIZE(Y)
	ST	$f13,  3*SIZE(Y)
	ST	$f14,  4*SIZE(Y)
	ST	$f15,  5*SIZE(Y)
	ST	$f16,  6*SIZE(Y)
	ST	$f17,  7*SIZE(Y)

	ST	$f18,  8*SIZE(Y)
	ST	$f19,  9*SIZE(Y)
	ST	$f20, 10*SIZE(Y)
	ST	$f21, 11*SIZE(Y)
	ST	$f22, 12*SIZE(Y)
	ST	$f23, 13*SIZE(Y)
	ST	$f24, 14*SIZE(Y)
	ST	$f25, 15*SIZE(Y)

	lda	Y, 16*SIZE(Y)
	.align 4

$Remain:
	ble	$5, $End
	.align 4

$RemainLoop:
#ifndef COMPLEX
	LD	$f10,  0*SIZE(X)
	lda	X,   1*SIZE(X)
	ST	$f10,  0*SIZE(Y)
	lda	Y,   1*SIZE(Y)
#else
	LD	$f10,  0*SIZE(X)
	LD	$f11,  1*SIZE(X)
	lda	X,   2*SIZE(X)
	ST	$f10,  0*SIZE(Y)
	ST	$f11,  1*SIZE(Y)
	lda	Y,   2*SIZE(Y)
#endif
	subq	$5, 1, $5
	bgt	$5, $RemainLoop
	.align 4
$End:
	ret
	.align 4

$Sub:
#ifdef COMPLEX
	addq	INCX, INCX, INCX
	addq	INCY, INCY, INCY
	and	N,  7, $5
#else
	and	N, 15, $5
#endif
	ble	$4, $SubRemain
	.align 4

$SubMainLoop:
#ifndef COMPLEX
	LD	$f10,  0(X)
	SXADDQ	INCX, X, X
	LD	$f11,  0(X)
	SXADDQ	INCX, X, X

	LD	$f12,  0(X)
	SXADDQ	INCX, X, X
	LD	$f13,  0(X)
	SXADDQ	INCX, X, X

	LD	$f14,  0(X)
	SXADDQ	INCX, X, X
	LD	$f15,  0(X)
	SXADDQ	INCX, X, X

	LD	$f16,  0(X)
	SXADDQ	INCX, X, X
	LD	$f17,  0(X)
	SXADDQ	INCX, X, X

	LD	$f18,  0(X)
	SXADDQ	INCX, X, X
	LD	$f19,  0(X)
	SXADDQ	INCX, X, X

	LD	$f20,  0(X)
	SXADDQ	INCX, X, X
	LD	$f21,  0(X)
	SXADDQ	INCX, X, X

	LD	$f22,  0(X)
	SXADDQ	INCX, X, X
	LD	$f23,  0(X)
	SXADDQ	INCX, X, X

	LD	$f24,  0(X)
	SXADDQ	INCX, X, X
	LD	$f25,  0(X)
	SXADDQ	INCX, X, X

	ST	$f10,  0(Y)
	SXADDQ	INCY, Y, Y
	ST	$f11,  0(Y)
	SXADDQ	INCY, Y, Y

	ST	$f12,  0(Y)
	SXADDQ	INCY, Y, Y
	ST	$f13,  0(Y)
	SXADDQ	INCY, Y, Y

	ST	$f14,  0(Y)
	SXADDQ	INCY, Y, Y
	ST	$f15,  0(Y)
	SXADDQ	INCY, Y, Y

	ST	$f16,  0(Y)
	SXADDQ	INCY, Y, Y
	ST	$f17,  0(Y)
	SXADDQ	INCY, Y, Y

	ST	$f18,  0(Y)
	SXADDQ	INCY, Y, Y
	ST	$f19,  0(Y)
	SXADDQ	INCY, Y, Y

	ST	$f20,  0(Y)
	SXADDQ	INCY, Y, Y
	ST	$f21,  0(Y)
	SXADDQ	INCY, Y, Y

	ST	$f22,  0(Y)
	SXADDQ	INCY, Y, Y
	ST	$f23,  0(Y)
	SXADDQ	INCY, Y, Y

	ST	$f24,  0(Y)
	SXADDQ	INCY, Y, Y
	ST	$f25,  0(Y)
	SXADDQ	INCY, Y, Y
#else
	LD	$f10,    0(X)
	LD	$f11, SIZE(X)
	SXADDQ	INCX, X, X

	LD	$f12,    0(X)
	LD	$f13, SIZE(X)
	SXADDQ	INCX, X, X

	LD	$f14,    0(X)
	LD	$f15, SIZE(X)
	SXADDQ	INCX, X, X

	LD	$f16,    0(X)
	LD	$f17, SIZE(X)
	SXADDQ	INCX, X, X

	LD	$f18,    0(X)
	LD	$f19, SIZE(X)
	SXADDQ	INCX, X, X

	LD	$f20,    0(X)
	LD	$f21, SIZE(X)
	SXADDQ	INCX, X, X

	LD	$f22,    0(X)
	LD	$f23, SIZE(X)
	SXADDQ	INCX, X, X

	LD	$f24,    0(X)
	LD	$f25, SIZE(X)
	SXADDQ	INCX, X, X

	ST	$f10,    0(Y)
	ST	$f11, SIZE(Y)
	SXADDQ	INCY, Y, Y

	ST	$f12,    0(Y)
	ST	$f13, SIZE(Y)
	SXADDQ	INCY, Y, Y

	ST	$f14,    0(Y)
	ST	$f15, SIZE(Y)
	SXADDQ	INCY, Y, Y

	ST	$f16,    0(Y)
	ST	$f17, SIZE(Y)
	SXADDQ	INCY, Y, Y

	ST	$f18,    0(Y)
	ST	$f19, SIZE(Y)
	SXADDQ	INCY, Y, Y

	ST	$f20,    0(Y)
	ST	$f21, SIZE(Y)
	SXADDQ	INCY, Y, Y

	ST	$f22,    0(Y)
	ST	$f23, SIZE(Y)
	SXADDQ	INCY, Y, Y

	ST	$f24,    0(Y)
	ST	$f25, SIZE(Y)
	SXADDQ	INCY, Y, Y
#endif
	subq	$4, 1, $4
	bgt	$4, $SubMainLoop
	.align 4

$SubRemain:
	ble	$5, $SubEnd
	.align 4

 $SubRemainLoop:
#ifndef COMPLEX
	LD	$f10,  0(X)
	SXADDQ	INCX, X, X
	ST	$f10,  0(Y)
	SXADDQ	INCY, Y, Y
#else
	LD	$f10,    0(X)
	LD	$f11, SIZE(X)
	SXADDQ	INCX, X, X
	ST	$f10,    0(Y)
	ST	$f11, SIZE(Y)
	SXADDQ	INCY, Y, Y
#endif
	subq	$5, 1, $5
	bgt	$5, $SubRemainLoop
	.align 4

$SubEnd:
	ret
	EPILOGUE

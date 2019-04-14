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

#ifndef USE_MIN
#define CMPLT(a, b) cmptlt a, b
#else
#define CMPLT(a, b) cmptlt b, a
#endif

#define STACKSIZE 6 * 8

	PROLOGUE
	PROFCODE
	.frame	$sp, STACKSIZE, $26, 0

	lda	$sp, -STACKSIZE($sp)
	nop
	.align 4

	stt	$f2,   0($sp)
	fclr	$f16
	cmplt	$31, N,    $2
	unop

	stt	$f3,   8($sp)
	fclr	$f17
	cmplt	$31, INCX, $3
	unop

	stt	$f4,  16($sp)
	fclr	$f18
	SXADDQ	INCX, $31, INCX
	unop

	stt	$f5,  24($sp)
	fclr	$f19
	and	$2,  $3,  $0
	unop

	stt	$f6,  32($sp)
	fclr	$f0
	sra	N, 3, $1
	beq	$0,  $End		# if (n <= 0) or (incx <= 0) return
	.align 4

	LD	$f20,  0 * SIZE(X)
	unop
	fabs	$f20, $f0
	ble	$1,  $L15
	.align 4

	fabs	$f20, $f1
	unop
	addq	X, INCX, X
	unop

	LD	$f21,  0 * SIZE(X)
	fabs	$f20, $f2
	addq	X, INCX, X
	unop

	LD	$f22,  0 * SIZE(X)
	fabs	$f20, $f3
	addq	X, INCX, X
	unop

	LD	$f23,  0 * SIZE(X)
	fabs	$f20, $f4
	addq	X, INCX, X
	unop

	LD	$f24,  0 * SIZE(X)
	addq	X, INCX, X
	fabs	$f20, $f5
	unop

	LD	$f25,  0 * SIZE(X)
	fabs	$f20, $f6
	addq	X, INCX, X
	unop

	LD	$f26,  0 * SIZE(X)
	fabs	$f20, $f28
	addq	X, INCX, X
	lda	$1,  -1($1)

	LD	$f27,  0 * SIZE(X)
	unop
	addq	X, INCX, X
	ble	$1, $L13
	.align 4

$L12:
	fcmovne	$f16, $f12, $f4
	unop
	fabs	$f20, $f29
	ldl	$31, 56 * SIZE(X)

	fcmovne	$f17, $f13, $f5
	LD	$f20,  0 * SIZE(X)
	fabs	$f21, $f30
	addq	X, INCX, X

	fcmovne	$f18, $f14, $f6
	LD	$f21,  0 * SIZE(X)
	fabs	$f22, $f10
	addq	X, INCX, X

	fcmovne	$f19, $f15, $f28
	LD	$f22,  0 * SIZE(X)
	fabs	$f23, $f11
	addq	X, INCX, X

	fabs	$f24, $f12
	LD	$f23,  0 * SIZE(X)
	CMPLT($f0,  $f29),  $f16
	addq	X, INCX, X

	fabs	$f25, $f13
	LD	$f24,  0 * SIZE(X)
	CMPLT($f1,  $f30),  $f17
	addq	X, INCX, X

	fabs	$f26, $f14
	LD	$f25,  0 * SIZE(X)
	CMPLT($f2,  $f10), $f18
	addq	X, INCX, X

	fabs	$f27, $f15
	LD	$f26,  0 * SIZE(X)
	CMPLT($f3,  $f11), $f19
	addq	X, INCX, X

	fcmovne	$f16, $f29,  $f0
	LD	$f27,  0 * SIZE(X)
	CMPLT($f4,  $f12), $f16
	addq	X, INCX, X

	fcmovne	$f17, $f30,  $f1
	unop
	CMPLT($f5,  $f13), $f17
	lda	$1,   -1($1)		# i --

	fcmovne	$f18, $f10, $f2
	unop
	CMPLT($f6,  $f14), $f18
	unop

	fcmovne	$f19, $f11, $f3
	unop
	CMPLT($f28,  $f15), $f19
	bgt	$1,$L12
	.align 4

$L13:
	fcmovne	$f16, $f12, $f4
	fabs	$f20, $f29
	fcmovne	$f17, $f13, $f5
	fabs	$f21, $f30

	fcmovne	$f18, $f14, $f6
	fabs	$f22, $f10
	fcmovne	$f19, $f15, $f28
	fabs	$f23, $f11

	fabs	$f24, $f12
	CMPLT($f0,  $f29),  $f16
	fabs	$f25, $f13
	CMPLT($f1,  $f30),  $f17

	fabs	$f26, $f14
	CMPLT($f2,  $f10), $f18
	fabs	$f27, $f15
	CMPLT($f3,  $f11), $f19

	fcmovne	$f16, $f29,  $f0
	CMPLT($f4,  $f12), $f16
	fcmovne	$f17, $f30,  $f1
	CMPLT($f5,  $f13), $f17

	fcmovne	$f18, $f10, $f2
	CMPLT($f6,  $f14), $f18
	fcmovne	$f19, $f11, $f3
	CMPLT($f28,  $f15), $f19

	fcmovne	$f16, $f12, $f4
	CMPLT($f0,  $f1), $f16
	fcmovne	$f17, $f13, $f5
	CMPLT($f2,  $f3), $f17

	fcmovne	$f18, $f14, $f6
	CMPLT($f4,  $f5), $f18
	fcmovne	$f19, $f15, $f28
	CMPLT($f6,  $f28), $f19

	fcmovne	$f16, $f1, $f0
	fcmovne	$f17, $f3, $f2
	fcmovne	$f18, $f5, $f4
	fcmovne	$f19, $f28, $f6

	CMPLT($f0,  $f2), $f16
	CMPLT($f4,  $f6), $f17

	fcmovne	$f16, $f2, $f0
	fcmovne	$f17, $f6, $f4

	CMPLT($f0,  $f4), $f16
	fcmovne	$f16, $f4, $f0
	.align 4

$L15:
	and	N, 7, $1
	unop
	unop
	ble	$1,  $End
	.align 4

$L16:
	LD	$f20,  0 * SIZE(X)
	addq	X, INCX, X

	fabs	$f20, $f29
	CMPLT($f0,  $f29), $f16
	fcmovne	$f16, $f29, $f0

	lda	$1,   -1($1)		# i --
	bgt	$1, $L16
	.align 4

$End:
	ldt	$f2,   0($sp)
	ldt	$f3,   8($sp)
	ldt	$f4,  16($sp)
	ldt	$f5,  24($sp)

	ldt	$f6,  32($sp)
	lda	$sp,  STACKSIZE($sp)
	ret

	EPILOGUE

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

#define STACKSIZE 8 * 8

	PROLOGUE
	PROFCODE
	.frame	$sp, STACKSIZE, $26, 0

#ifdef F_INTERFACE
	ldl	N,     0(N)		# n
	ldl	INCX,  0(INCX)		# incx
#endif
	lda	$sp, -STACKSIZE($sp)
	nop
	.align 4

	cmplt	$31, N,    $2
	cmplt	$31, INCX, $3
	SXADDQ	INCX, $31, INCX
	and	$2,  $3,  $0

	sra	N, 3, $1
	fclr	$f0
	unop
	beq	$0,  $End		# if (n <= 0) or (incx <= 0) return
	.align 4

	LD	$f0,  0 * SIZE(X)
	unop
	unop
	ble	$1,  $L15
	.align 4

	fmov	$f0,  $f1
	addq	X, INCX, X
	fmov	$f0,  $f10
	lda	$1,  -1($1)

	LD	$f21,  0 * SIZE(X)
	fmov	$f0,  $f11
	addq	X, INCX, X
	fmov	$f0,  $f12

	LD	$f22,  0 * SIZE(X)
	fmov	$f0,  $f13
	addq	X, INCX, X
	fmov	$f0,  $f14

	LD	$f23,  0 * SIZE(X)
	fmov	$f0,  $f15
	addq	X, INCX, X
	fmov	$f0,  $f20

	LD	$f24,  0 * SIZE(X)
	addq	X, INCX, X
	LD	$f25,  0 * SIZE(X)
	addq	X, INCX, X
	LD	$f26,  0 * SIZE(X)
	addq	X, INCX, X
	LD	$f27,  0 * SIZE(X)
	addq	X, INCX, X

	CMPLT($f0,  $f20), $f16
	CMPLT($f1,  $f21), $f17
	CMPLT($f10, $f22), $f18
	CMPLT($f11, $f23), $f19

	ble	$1, $L13
	.align 4

$L12:
	fcmovne	$f16, $f20, $f0
	LD	$f20,  0 * SIZE(X)
	CMPLT($f12,  $f24), $f16
	addq	X, INCX, X

	fcmovne	$f17, $f21, $f1
	LD	$f21,  0 * SIZE(X)
	CMPLT($f13,  $f25), $f17
	addq	X, INCX, X

	fcmovne	$f18, $f22, $f10
	LD	$f22,  0 * SIZE(X)
	CMPLT($f14,  $f26), $f18
	addq	X, INCX, X

	fcmovne	$f19, $f23, $f11
	LD	$f23,  0 * SIZE(X)
	CMPLT($f15,  $f27), $f19
	addq	X, INCX, X

	fcmovne	$f16, $f24, $f12
	LD	$f24,  0 * SIZE(X)
	CMPLT($f0,  $f20), $f16
	addq	X, INCX, X

	fcmovne	$f17, $f25, $f13
	LD	$f25,  0 * SIZE(X)
	CMPLT($f1,  $f21), $f17
	addq	X, INCX, X

	fcmovne	$f18, $f26, $f14
	LD	$f26,  0 * SIZE(X)
	CMPLT($f10,  $f22), $f18
	addq	X, INCX, X

	fcmovne	$f19, $f27, $f15
	LD	$f27,  0 * SIZE(X)
	CMPLT($f11,  $f23), $f19
	lda	$1,   -1($1)		# i --

	addq	X, INCX, X
	unop
	unop
	bgt	$1,$L12
	.align 4

$L13:
	fcmovne	$f16, $f20, $f0
	CMPLT($f12,  $f24), $f16

	fcmovne	$f17, $f21, $f1
	CMPLT($f13,  $f25), $f17

	fcmovne	$f18, $f22, $f10
	CMPLT($f14,  $f26), $f18

	fcmovne	$f19, $f23, $f11
	CMPLT($f15,  $f27), $f19

	fcmovne	$f16, $f24, $f12
	CMPLT($f0,  $f1), $f16
	fcmovne	$f17, $f25, $f13
	CMPLT($f10,  $f11), $f17

	fcmovne	$f18, $f26, $f14
	CMPLT($f12,  $f13), $f18
	fcmovne	$f19, $f27, $f15
	CMPLT($f14,  $f15), $f19

	fcmovne	$f16, $f1, $f0
	fcmovne	$f17, $f11, $f10
	fcmovne	$f18, $f13, $f12
	fcmovne	$f19, $f15, $f14

	CMPLT($f0,  $f10), $f16
	CMPLT($f12,  $f14), $f17

	fcmovne	$f16, $f10, $f0
	fcmovne	$f17, $f14, $f12

	CMPLT($f0,  $f12), $f16
	fcmovne	$f16, $f12, $f0
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

	CMPLT($f0,  $f20), $f16
	fcmovne	$f16, $f20, $f0
	lda	$1,   -1($1)		# i --
	bgt	$1, $L16
	.align 4

$End:
	lda	$sp,  STACKSIZE($sp)
	ret

	EPILOGUE

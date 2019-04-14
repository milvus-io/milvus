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
#define XX	$19

#ifndef USE_MIN
#define CMPLT(a, b) cmptlt a, b
#else
#define CMPLT(a, b) cmptlt b, a
#endif

#define STACKSIZE 8 * 8

	PROLOGUE
	PROFCODE

	lda	$sp, -STACKSIZE($sp)

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
	and	$2,  $3,  $2
	clr	$0

	stt	$f6,  32($sp)
	mov	X, XX

	stt	$f7,  40($sp)
	stt	$f8,  48($sp)
	stt	$f9,  56($sp)

	fclr	$f0
	beq	$2,  $End		# if (n <= 0) or (incx <= 0) return
	.align 4

	LD	$f20,  0 * SIZE(X)
	LD	$f21,  1 * SIZE(X)
	sra	N, 2, $1
	addq	INCX, INCX, INCX

	fabs	$f20, $f20
	fabs	$f21, $f21
	addt	$f20, $f21, $f0
	ble	$1,  $L15
	.align 4

	lda	$1,  -1($1)
	unop
	addq	X, INCX, X
	unop

	LD	$f22,  0 * SIZE(X)
	fmov	$f0,  $f1
	LD	$f23,  1 * SIZE(X)
	addq	X, INCX, X

	LD	$f24,  0 * SIZE(X)
	fmov	$f0,  $f2
	LD	$f25,  1 * SIZE(X)
	addq	X, INCX, X

	LD	$f26,  0 * SIZE(X)
	fmov	$f0,  $f3
	LD	$f27,  1 * SIZE(X)
	addq	X, INCX, X

	fabs	$f20, $f8
	fabs	$f21, $f9
	fabs	$f22, $f10
	fabs	$f23, $f11

	fabs	$f24, $f12
	fabs	$f25, $f13
	fabs	$f26, $f14
	fabs	$f27, $f15

	ble	$1, $L14
	.align 4

	LD	$f20,  0 * SIZE(X)
	LD	$f21,  1 * SIZE(X)
	lda	$1,  -1($1)
	addq	X, INCX, X

	LD	$f22,  0 * SIZE(X)
	LD	$f23,  1 * SIZE(X)
	unop
	addq	X, INCX, X

	LD	$f24,  0 * SIZE(X)
	LD	$f25,  1 * SIZE(X)
	unop
	addq	X, INCX, X

	LD	$f26,  0 * SIZE(X)
	LD	$f27,  1 * SIZE(X)
	addq	X, INCX, X
	ble	$1, $L13
	.align 4

$L12:
	addt	$f8,  $f9,  $f16
	unop
	fabs	$f20, $f8
	ldl	$31, 64 * SIZE(X)

	addt	$f10, $f11, $f17
	unop
	fabs	$f21, $f9
	LD	$f20,  0 * SIZE(X)

	addt	$f12, $f13, $f18
	LD	$f21,  1 * SIZE(X)
	fabs	$f22, $f10
	addq	X, INCX, X

	addt	$f14, $f15, $f19
	LD	$f22,  0 * SIZE(X)
	fabs	$f23, $f11
	unop

	CMPLT($f0,  $f16), $f4
	LD	$f23,  1 * SIZE(X)
	fabs	$f24, $f12
	addq	X, INCX, X

	CMPLT($f1,  $f17), $f5
	LD	$f24,  0 * SIZE(X)
	fabs	$f25, $f13
	unop

	CMPLT($f2,  $f18), $f6
	LD	$f25,  1 * SIZE(X)
	fabs	$f26, $f14
	addq	X, INCX, X

	CMPLT($f3,  $f19), $f7
	LD	$f26,  0 * SIZE(X)
	fabs	$f27, $f15
	unop

	fcmovne	$f4, $f16, $f0
	LD	$f27,  1 * SIZE(X)
	addq	X, INCX, X
	lda	$1,   -1($1)		# i --

	fcmovne	$f5, $f17, $f1
	fcmovne	$f6, $f18, $f2
	fcmovne	$f7, $f19, $f3
	bgt	$1,$L12
	.align 4

$L13:
	addt	$f8,  $f9,  $f16
	fabs	$f20, $f8

	addt	$f10, $f11, $f17
	fabs	$f21, $f9

	addt	$f12, $f13, $f18
	fabs	$f22, $f10

	addt	$f14, $f15, $f19
	fabs	$f23, $f11

	CMPLT($f0,  $f16), $f4
	fabs	$f24, $f12

	CMPLT($f1,  $f17), $f5
	fabs	$f25, $f13

	CMPLT($f2,  $f18), $f6
	fabs	$f26, $f14
	CMPLT($f3,  $f19), $f7
	fabs	$f27, $f15

	fcmovne	$f4, $f16, $f0
	fcmovne	$f5, $f17, $f1
	fcmovne	$f6, $f18, $f2
	fcmovne	$f7, $f19, $f3
	.align 4

$L14:
	addt	$f8,  $f9,  $f16
	addt	$f10, $f11, $f17
	addt	$f12, $f13, $f18
	addt	$f14, $f15, $f19

	CMPLT($f0,  $f16), $f4
	CMPLT($f1,  $f17), $f5
	CMPLT($f2,  $f18), $f6
	CMPLT($f3,  $f19), $f7

	fcmovne	$f4, $f16, $f0
	fcmovne	$f5, $f17, $f1
	fcmovne	$f6, $f18, $f2
	fcmovne	$f7, $f19, $f3

	CMPLT($f0,  $f1), $f16
	CMPLT($f2,  $f3), $f17

	fcmovne	$f16, $f1, $f0
	fcmovne	$f17, $f3, $f2

	CMPLT($f0,  $f2), $f16
	fcmovne	$f16, $f2, $f0
	.align 4

$L15:
	and	N, 3, $1
	unop
	unop
	ble	$1,  $L20
	.align 4

$L16:
	LD	$f20,  0 * SIZE(X)
	LD	$f21,  1 * SIZE(X)
	unop
	addq	X, INCX, X

	fabs	$f20, $f29
	fabs	$f21, $f30
	addt	$f29, $f30, $f29

	CMPLT($f0,  $f29), $f16
	fcmovne	$f16, $f29, $f0

	lda	$1,   -1($1)		# i --
	bgt	$1, $L16
	.align 4

$L20:
	sra	N, 2, $1
	ble	$1,  $L40
	.align 4

	LD	$f10,  0 * SIZE(XX)
	LD	$f11,  1 * SIZE(XX)
	addq	XX, INCX, XX

	LD	$f12,  0 * SIZE(XX)
	LD	$f13,  1 * SIZE(XX)
	addq	XX, INCX, XX

	LD	$f14,  0 * SIZE(XX)
	LD	$f15,  1 * SIZE(XX)
	addq	XX, INCX, XX

	LD	$f16,  0 * SIZE(XX)
	LD	$f17,  1 * SIZE(XX)
	addq	XX, INCX, XX

	fabs	$f10, $f18
	fabs	$f11, $f19
	fabs	$f12, $f20
	fabs	$f13, $f21

	lda	$1,  -1($1)
	ble	$1, $L23
	.align 4

$L22:
	LD	$f10,  0 * SIZE(XX)
	fabs	$f14, $f22
	LD	$f11,  1 * SIZE(XX)
	addq	XX, INCX, XX

	LD	$f12,  0 * SIZE(XX)
	fabs	$f15, $f23
	LD	$f13,  1 * SIZE(XX)
	addq	XX, INCX, XX

	LD	$f14,  0 * SIZE(XX)
	fabs	$f16, $f24
	LD	$f15,  1 * SIZE(XX)
	addq	XX, INCX, XX

	LD	$f16,  0 * SIZE(XX)
	fabs	$f17, $f25
	LD	$f17,  1 * SIZE(XX)
	addq	XX, INCX, XX

	addt	$f18, $f19, $f4
	addt	$f20, $f21, $f5
	addt	$f22, $f23, $f6
	addt	$f24, $f25, $f7

	cmpteq	$f0, $f4, $f26
	cmpteq	$f0, $f5, $f27
	cmpteq	$f0, $f6, $f28
	cmpteq	$f0, $f7, $f29

	fabs	$f10, $f18
	lda	$0,    1($0)
	lda	$1,   -1($1)		# i --
	fbne	$f26, $End

	fabs	$f11, $f19
	lda	$0,    1($0)
	unop
	fbne	$f27, $End

	fabs	$f12, $f20
	lda	$0,    1($0)
	unop
	fbne	$f28, $End

	fabs	$f13, $f21
	lda	$0,    1($0)
	fbne	$f29, $End
	bgt	$1,  $L22
	.align 4

$L23:
	fabs	$f14, $f22
	fabs	$f15, $f23
	fabs	$f16, $f24
	fabs	$f17, $f25

	addt	$f18, $f19, $f4
	addt	$f20, $f21, $f5
	addt	$f22, $f23, $f6
	addt	$f24, $f25, $f7

	cmpteq	$f0, $f4, $f26
	cmpteq	$f0, $f5, $f27
	cmpteq	$f0, $f6, $f28
	cmpteq	$f0, $f7, $f29

	lda	$0,    1($0)
	fbne	$f26, $End
	lda	$0,    1($0)
	fbne	$f27, $End
	lda	$0,    1($0)
	fbne	$f28, $End
	lda	$0,    1($0)
	fbne	$f29, $End
	.align 4

$L40:
	LD	$f10,  0 * SIZE(XX)
	LD	$f11,  1 * SIZE(XX)

	addq	XX, INCX, XX

	fabs	$f10, $f18
	fabs	$f11, $f19

	addt	$f18, $f19, $f18
	cmpteq	$f0, $f18, $f2

	lda	$0,    1($0)
	fbne	$f2,  $End
	br	$31,  $L40
	.align 4

$End:
	ldt	$f2,   0($sp)
	ldt	$f3,   8($sp)
	ldt	$f4,  16($sp)
	ldt	$f5,  24($sp)

	ldt	$f6,  32($sp)
	ldt	$f7,  40($sp)
	ldt	$f8,  48($sp)
	ldt	$f9,  56($sp)
	lda	$sp,  STACKSIZE($sp)
	ret

	EPILOGUE

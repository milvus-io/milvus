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

#define PREFETCHSIZE	88

#define N	$16
#define X	$21
#define INCX	$17

#define XX	$18
#define I	$19

#define ALPHA_R	$f19
#define ALPHA_I	$f20

#define s0	$f0
#define s1	$f1
#define s2	$f10
#define s3	$f11

#define a0	$f12
#define a1	$f13
#define a2	$f14
#define a3	$f15
#define a4	$f16
#define a5	$f17
#define a6	$f18
#define a7	$f21

#define t0	$f22
#define t1	$f23
#define t2	$f24
#define t3	$f25

#define t4	$f26
#define t5	$f27
#define t6	$f28
#define t7	$f29

	PROLOGUE
	PROFCODE

	ldq	INCX, 0($sp)
	mov	X, XX
	ble	N,  $L999

	addq	INCX, INCX, INCX

	sra	N, 2, I
	ble	I, $L15

	LD	a0,  0 * SIZE(X)
	LD	a1,  1 * SIZE(X)
	SXADDQ	INCX, X, X
	LD	a2,  0 * SIZE(X)
	LD	a3,  1 * SIZE(X)
	SXADDQ	INCX, X, X
	LD	a4,  0 * SIZE(X)
	LD	a5,  1 * SIZE(X)
	SXADDQ	INCX, X, X
	LD	a6,  0 * SIZE(X)
	LD	a7,  1 * SIZE(X)
	SXADDQ	INCX, X, X

	MUL	a0, ALPHA_R, t0
	MUL	a1, ALPHA_I, t1
	MUL	a0, ALPHA_I, t2
	MUL	a1, ALPHA_R, t3

	SUB	t0, t1, t4
	ADD	t2, t3, t5

	lda	I,  -1(I)
	ble	I, $L13
	.align 4

$L12:
	ST	t4,  0 * SIZE(XX)
	MUL	a2, ALPHA_R, t0
	ST	t5,  1 * SIZE(XX)
	MUL	a3, ALPHA_I, t1

	MUL	a2, ALPHA_I, t2
	LD	a0,  0 * SIZE(X)
	MUL	a3, ALPHA_R, t3
	LD	a1,  1 * SIZE(X)

	SUB	t0, t1, t6
	SXADDQ	INCX, XX, XX
	ADD	t2, t3, t7
	SXADDQ	INCX, X, X

	MUL	a4, ALPHA_R, t0
	ST	t6,  0 * SIZE(XX)
	MUL	a5, ALPHA_I, t1
	ST	t7,  1 * SIZE(XX)

	MUL	a4, ALPHA_I, t2
	LD	a2,  0 * SIZE(X)
	MUL	a5, ALPHA_R, t3
	LD	a3,  1 * SIZE(X)

	SUB	t0, t1, t4
	SXADDQ	INCX, XX, XX
	ADD	t2, t3, t5
	SXADDQ	INCX, X, X

	MUL	a6, ALPHA_R, t0
	ST	t4,  0 * SIZE(XX)
	MUL	a7, ALPHA_I, t1
	ST	t5,  1 * SIZE(XX)

	MUL	a6, ALPHA_I, t2
	LD	a4,  0 * SIZE(X)
	MUL	a7, ALPHA_R, t3
	LD	a5,  1 * SIZE(X)

	SUB	t0, t1, t6
	SXADDQ	INCX, XX, XX
	ADD	t2, t3, t7
	SXADDQ	INCX, X, X

	MUL	a0, ALPHA_R, t0
	ST	t6,  0 * SIZE(XX)
	MUL	a1, ALPHA_I, t1
	ST	t7,  1 * SIZE(XX)

	MUL	a0, ALPHA_I, t2
	LD	a6,  0 * SIZE(X)
	MUL	a1, ALPHA_R, t3
	LD	a7,  1 * SIZE(X)

	SUB	t0, t1, t4
	lda	I,  -1(I)
	ADD	t2, t3, t5
	SXADDQ	INCX, XX, XX

	lds	$f31, PREFETCHSIZE * SIZE(X)
	unop
	SXADDQ	INCX, X, X
	bne	I, $L12
	.align 4

$L13:
	MUL	a2, ALPHA_R, t0
	MUL	a3, ALPHA_I, t1
 	ST	t4,  0 * SIZE(XX)
	MUL	a2, ALPHA_I, t2
	ST	t5,  1 * SIZE(XX)
	MUL	a3, ALPHA_R, t3

	SUB	t0, t1, t6
	SXADDQ	INCX, XX, XX
	ADD	t2, t3, t7
	unop

	ST	t6,  0 * SIZE(XX)
	MUL	a4, ALPHA_R, t0
	ST	t7,  1 * SIZE(XX)
	MUL	a5, ALPHA_I, t1
	MUL	a4, ALPHA_I, t2
	MUL	a5, ALPHA_R, t3

	SUB	t0, t1, t4
	SXADDQ	INCX, XX, XX
	ADD	t2, t3, t5
	unop

	MUL	a6, ALPHA_R, t0
	ST	t4,  0 * SIZE(XX)
	MUL	a7, ALPHA_I, t1
	ST	t5,  1 * SIZE(XX)

	MUL	a6, ALPHA_I, t2
	MUL	a7, ALPHA_R, t3

	SUB	t0, t1, t6
	SXADDQ	INCX, XX, XX
	ADD	t2, t3, t7

	ST	t6,  0 * SIZE(XX)
	ST	t7,  1 * SIZE(XX)
	SXADDQ	INCX, XX, XX
	.align 4

$L15:
	and	N, 3, I
	unop
	unop
	ble	I, $L999
	.align 4

$L17:
	LD	a0,  0 * SIZE(X)
	LD	a1,  1 * SIZE(X)
	SXADDQ	INCX, X, X

	MUL	a0, ALPHA_R, t0
	MUL	a1, ALPHA_I, t1
	MUL	a0, ALPHA_I, t2
	MUL	a1, ALPHA_R, t3

	SUB	t0, t1, t4
	ADD	t2, t3, t5

	ST	t4,  0 * SIZE(XX)
	ST	t5,  1 * SIZE(XX)
	SXADDQ	INCX, XX, XX

	lda	I,  -1(I)
	bne	I, $L17
	.align 4

$L999:
	ret
	EPILOGUE

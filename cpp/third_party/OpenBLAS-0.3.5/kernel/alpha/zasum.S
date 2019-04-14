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
#define X	$17
#define INCX	$18
#define I	$19

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
#define a7	$f19

#define t0	$f20
#define t1	$f21
#define t2	$f22
#define t3	$f23

	PROLOGUE
	PROFCODE

	fclr	s0
	unop
	fclr	t0
	addq	INCX, INCX, INCX

	fclr	s1
	unop
	fclr	t1
	ble	N,  $L999

	fclr	s2
	sra	N, 2, I
	fclr	s3
	ble	I, $L15

	LD	a0,  0 * SIZE(X)
	fclr	t2
	LD	a1,  1 * SIZE(X)
	SXADDQ	INCX, X, X

	LD	a2,  0 * SIZE(X)
	fclr	t3
	LD	a3,  1 * SIZE(X)
	SXADDQ	INCX, X, X

	LD	a4,  0 * SIZE(X)
	LD	a5,  1 * SIZE(X)
	SXADDQ	INCX, X, X
	lda	I,  -1(I)

	ble	I, $L13
	.align 4

$L12:
	ADD	s0, t0, s0
	ldl	$31, PREFETCHSIZE * SIZE(X)
	fabs	a0, t0
	lda	I,  -1(I)

	ADD	s1, t1, s1
	LD	a6,  0 * SIZE(X)
	fabs	a1, t1
	unop

	ADD	s2, t2, s2
	LD	a7,  1 * SIZE(X)
	fabs	a2, t2
	SXADDQ	INCX, X, X

	ADD	s3, t3, s3
	LD	a0,  0 * SIZE(X)
	fabs	a3, t3
	unop

	ADD	s0, t0, s0
	LD	a1,  1 * SIZE(X)
	fabs	a4, t0
	SXADDQ	INCX, X, X

	ADD	s1, t1, s1
	LD	a2,  0 * SIZE(X)
	fabs	a5, t1
	unop

	ADD	s2, t2, s2
	LD	a3,  1 * SIZE(X)
	fabs	a6, t2
	SXADDQ	INCX, X, X

	ADD	s3, t3, s3
	LD	a4,  0 * SIZE(X)
	fabs	a7, t3
	unop

	LD	a5,  1 * SIZE(X)
	unop
	SXADDQ	INCX, X, X
	bne	I, $L12
	.align 4

$L13:
	ADD	s0, t0, s0
	LD	a6,  0 * SIZE(X)
	fabs	a0, t0

	ADD	s1, t1, s1
	LD	a7,  1 * SIZE(X)
	fabs	a1, t1
	SXADDQ	INCX, X, X

	ADD	s2, t2, s2
	fabs	a2, t2
	ADD	s3, t3, s3
	fabs	a3, t3

	ADD	s0, t0, s0
	fabs	a4, t0
	ADD	s1, t1, s1
	fabs	a5, t1
	ADD	s2, t2, s2
	fabs	a6, t2
	ADD	s3, t3, s3
	fabs	a7, t3

	ADD	s2, t2, s2
	ADD	s3, t3, s3

	.align 4

$L15:
	ADD	s0, s2, s0
	and	N, 3, I
	ADD	s1, s3, s1
	ble	I, $L999
	.align 4

$L17:
	ADD	s0, t0, s0
	LD	a0,  0 * SIZE(X)
	fabs	a0, t0
	lda	I,  -1(I)

	ADD	s1, t1, s1
	LD	a1,  1 * SIZE(X)
	fabs	a1, t1
	SXADDQ	INCX, X, X

	bne	I, $L17
	.align 4

$L999:
	ADD	s0, t0, s0
	ADD	s1, t1, s1

	ADD	s0, s1, s0
	ret
	EPILOGUE

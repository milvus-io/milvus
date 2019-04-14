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

#define N	$4
#define	X	$5
#define INCX	$6

#define I	$2
#define TEMP	$3

#define a1	$f4
#define a2	$f5
#define a3	$f6
#define a4	$f7
#define a5	$f8
#define a6	$f9
#define a7	$f10
#define a8	$f11

#define s1	$f0
#define s2	$f1
#define s3	$f2
#define s4	$f3

	PROLOGUE

#ifdef F_INTERFACE
	LDINT	N,     0(N)
	LDINT	INCX,  0(INCX)
#endif

	blez	N, .L999
	MTC	$0,  s1

	blez	INCX, .L999
	dsll	INCX, INCX, BASE_SHIFT

	LD	s1,  0 * SIZE(X)
	daddiu	N, N, -1

	daddu	X, X, INCX
	NOP

	blez	N, .L999
	MOV	s2, s1

	MOV	s3, s1
	dsra	I, N, 3

	blez	I, .L15
	MOV	s4, s1

	LD	a1,  0 * SIZE(X)
	daddu	X, X, INCX
	LD	a2,  0 * SIZE(X)
	daddu	X, X, INCX
	LD	a3,  0 * SIZE(X)
	daddu	X, X, INCX
	LD	a4,  0 * SIZE(X)
	daddu	X, X, INCX
	LD	a5,  0 * SIZE(X)
	daddu	X, X, INCX
	LD	a6,  0 * SIZE(X)
	daddiu	I, I, -1

	blez	I, .L13
	daddu	X, X, INCX
	.align 3

.L12:
	CMPLT	$fcc0, a1, s1
	LD	a7,  0 * SIZE(X)
	CMPLT	$fcc1, a2, s2
	daddu	X, X, INCX

	CMPLT	$fcc2, a3, s3
	LD	a8,  0 * SIZE(X)
	CMPLT	$fcc3, a4, s4
	daddu	X, X, INCX

	CMOVT	s1, a1, $fcc0
	LD	a1,  0 * SIZE(X)
	CMOVT	s2, a2, $fcc1
	daddu	X, X, INCX

	CMOVT	s3, a3, $fcc2
	LD	a2,  0 * SIZE(X)
	CMOVT	s4, a4, $fcc3
	daddu	X, X, INCX

	CMPLT	$fcc0, a5, s1
	LD	a3,  0 * SIZE(X)
	CMPLT	$fcc1, a6, s2
	daddu	X, X, INCX
	CMPLT	$fcc2, a7, s3
	LD	a4,  0 * SIZE(X)
	CMPLT	$fcc3, a8, s4
	daddu	X, X, INCX

	CMOVT	s1, a5, $fcc0
	LD	a5,  0 * SIZE(X)
	CMOVT	s2, a6, $fcc1
	daddu	X, X, INCX

	CMOVT	s3, a7, $fcc2
	LD	a6,  0 * SIZE(X)
	CMOVT	s4, a8, $fcc3
	daddiu	I, I, -1

	bgtz	I, .L12
	daddu	X, X, INCX
	.align 3

.L13:
	CMPLT	$fcc0, a1, s1
	LD	a7,  0 * SIZE(X)
	CMPLT	$fcc1, a2, s2
	daddu	X, X, INCX

	CMPLT	$fcc2, a3, s3
	LD	a8,  0 * SIZE(X)
	CMPLT	$fcc3, a4, s4
	daddu	X, X, INCX

	CMOVT	s1, a1, $fcc0
	CMOVT	s2, a2, $fcc1
	CMOVT	s3, a3, $fcc2
	CMOVT	s4, a4, $fcc3

	CMPLT	$fcc0, a5, s1
	CMPLT	$fcc1, a6, s2
	CMPLT	$fcc2, a7, s3
	CMPLT	$fcc3, a8, s4

	CMOVT	s1, a5, $fcc0
	CMOVT	s2, a6, $fcc1
	CMOVT	s3, a7, $fcc2
	CMOVT	s4, a8, $fcc3
	.align 3

.L15:
	andi	I,  N, 7

	blez	I, .L998
	NOP
	.align	3

.L16:
	LD	a1,  0 * SIZE(X)
	daddiu	I, I, -1

	CMPLT	$fcc0, a1, s1

	CMOVT	s1, a1, $fcc0

	bgtz	I, .L16
	daddu	X, X, INCX
	.align 3

.L998:
	CMPLT	$fcc0, s2, s1
	CMPLT	$fcc1, s4, s3

	CMOVT	s1, s2, $fcc0
	CMOVT	s3, s4, $fcc1

	CMPLT	$fcc0, s3, s1
	CMOVT	s1, s3, $fcc0
	.align 3

.L999:
	j	$31
	NOP

	EPILOGUE

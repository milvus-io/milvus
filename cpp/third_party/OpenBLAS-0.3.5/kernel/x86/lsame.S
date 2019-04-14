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

	PROLOGUE
	PROFCODE

	movl	 4(%esp), %eax
	movl	 8(%esp), %edx
	movb	  (%eax), %al		# a = *A
	movb	  (%edx), %dl		# b = *B

	andl	$255, %eax
	andl	$255, %edx

	subl	$65,  %eax
	subl	$65,  %edx

#ifndef HAVE_CMOV
	movl	%eax, %ecx
	subl	$32,  %ecx
	jle	.L1
	movl	%ecx, %eax
.L1:

	movl	%edx, %ecx
	subl	$32,  %ecx
	jle	.L2
	movl	%ecx, %edx
.L2:
	subl	%eax, %edx
	movl	$0,   %eax
	movl	$1,   %edx
	jne	.L3
	movl	%edx, %eax
.L3:
#else
	movl	%eax, %ecx
	subl	$32,  %ecx
	cmovge	%ecx, %eax

	movl	%edx, %ecx
	subl	$32,  %ecx
	cmovge	%ecx, %edx

	subl	%eax, %edx
	movl	$0,   %eax
	movl	$1,   %edx
	cmove	%edx, %eax
#endif
	ret

	EPILOGUE

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

#include <stdio.h>
#include "common.h"

int CNAME(BLASLONG m, BLASLONG n, FLOAT *a, BLASLONG lda, FLOAT *b){

  BLASLONG i, j;

  FLOAT *aoffset;
  FLOAT *aoffset1, *aoffset2, *aoffset3, *aoffset4;
  FLOAT *aoffset5, *aoffset6, *aoffset7, *aoffset8;

  FLOAT *boffset,  *boffset1, *boffset2, *boffset3, *boffset4;

  FLOAT ctemp01, ctemp02, ctemp03, ctemp04;
  FLOAT ctemp05, ctemp06, ctemp07, ctemp08;
  FLOAT ctemp09, ctemp10, ctemp11, ctemp12;
  FLOAT ctemp13, ctemp14, ctemp15, ctemp16;
  FLOAT ctemp17, ctemp18, ctemp19, ctemp20;
  FLOAT ctemp21, ctemp22, ctemp23, ctemp24;
  FLOAT ctemp25, ctemp26, ctemp27, ctemp28;
  FLOAT ctemp29, ctemp30, ctemp31, ctemp32;
  FLOAT ctemp33, ctemp34, ctemp35, ctemp36;
  FLOAT ctemp37, ctemp38, ctemp39, ctemp40;
  FLOAT ctemp41, ctemp42, ctemp43, ctemp44;
  FLOAT ctemp45, ctemp46, ctemp47, ctemp48;
  FLOAT ctemp49, ctemp50, ctemp51, ctemp52;
  FLOAT ctemp53, ctemp54, ctemp55, ctemp56;
  FLOAT ctemp57, ctemp58, ctemp59, ctemp60;
  FLOAT ctemp61, ctemp62, ctemp63, ctemp64;

  aoffset   = a;
  boffset   = b;

#if 0
  fprintf(stderr, "M = %d N = %d\n", m, n);
#endif

  boffset2  = b + m  * (n & ~7);
  boffset3  = b + m  * (n & ~3);
  boffset4  = b + m  * (n & ~1);

  j = (m >> 3);
  if (j > 0){
    do{
      aoffset1  = aoffset;
      aoffset2  = aoffset1 + lda;
      aoffset3  = aoffset2 + lda;
      aoffset4  = aoffset3 + lda;
      aoffset5  = aoffset4 + lda;
      aoffset6  = aoffset5 + lda;
      aoffset7  = aoffset6 + lda;
      aoffset8  = aoffset7 + lda;
      aoffset += 8 * lda;

      boffset1  = boffset;
      boffset  += 64;

      i = (n >> 3);
      if (i > 0){
	do{
	  ctemp01 = *(aoffset1 + 0);
	  ctemp02 = *(aoffset1 + 1);
	  ctemp03 = *(aoffset1 + 2);
	  ctemp04 = *(aoffset1 + 3);
	  ctemp05 = *(aoffset1 + 4);
	  ctemp06 = *(aoffset1 + 5);
	  ctemp07 = *(aoffset1 + 6);
	  ctemp08 = *(aoffset1 + 7);
	  aoffset1 += 8;

	  ctemp09 = *(aoffset2 + 0);
	  ctemp10 = *(aoffset2 + 1);
	  ctemp11 = *(aoffset2 + 2);
	  ctemp12 = *(aoffset2 + 3);
	  ctemp13 = *(aoffset2 + 4);
	  ctemp14 = *(aoffset2 + 5);
	  ctemp15 = *(aoffset2 + 6);
	  ctemp16 = *(aoffset2 + 7);
	  aoffset2 += 8;

	  ctemp17 = *(aoffset3 + 0);
	  ctemp18 = *(aoffset3 + 1);
	  ctemp19 = *(aoffset3 + 2);
	  ctemp20 = *(aoffset3 + 3);
	  ctemp21 = *(aoffset3 + 4);
	  ctemp22 = *(aoffset3 + 5);
	  ctemp23 = *(aoffset3 + 6);
	  ctemp24 = *(aoffset3 + 7);
	  aoffset3 += 8;

	  ctemp25 = *(aoffset4 + 0);
	  ctemp26 = *(aoffset4 + 1);
	  ctemp27 = *(aoffset4 + 2);
	  ctemp28 = *(aoffset4 + 3);
	  ctemp29 = *(aoffset4 + 4);
	  ctemp30 = *(aoffset4 + 5);
	  ctemp31 = *(aoffset4 + 6);
	  ctemp32 = *(aoffset4 + 7);
	  aoffset4 += 8;

	  ctemp33 = *(aoffset5 + 0);
	  ctemp34 = *(aoffset5 + 1);
	  ctemp35 = *(aoffset5 + 2);
	  ctemp36 = *(aoffset5 + 3);
	  ctemp37 = *(aoffset5 + 4);
	  ctemp38 = *(aoffset5 + 5);
	  ctemp39 = *(aoffset5 + 6);
	  ctemp40 = *(aoffset5 + 7);
	  aoffset5 += 8;

	  ctemp41 = *(aoffset6 + 0);
	  ctemp42 = *(aoffset6 + 1);
	  ctemp43 = *(aoffset6 + 2);
	  ctemp44 = *(aoffset6 + 3);
	  ctemp45 = *(aoffset6 + 4);
	  ctemp46 = *(aoffset6 + 5);
	  ctemp47 = *(aoffset6 + 6);
	  ctemp48 = *(aoffset6 + 7);
	  aoffset6 += 8;

	  ctemp49 = *(aoffset7 + 0);
	  ctemp50 = *(aoffset7 + 1);
	  ctemp51 = *(aoffset7 + 2);
	  ctemp52 = *(aoffset7 + 3);
	  ctemp53 = *(aoffset7 + 4);
	  ctemp54 = *(aoffset7 + 5);
	  ctemp55 = *(aoffset7 + 6);
	  ctemp56 = *(aoffset7 + 7);
	  aoffset7 += 8;

	  ctemp57 = *(aoffset8 + 0);
	  ctemp58 = *(aoffset8 + 1);
	  ctemp59 = *(aoffset8 + 2);
	  ctemp60 = *(aoffset8 + 3);
	  ctemp61 = *(aoffset8 + 4);
	  ctemp62 = *(aoffset8 + 5);
	  ctemp63 = *(aoffset8 + 6);
	  ctemp64 = *(aoffset8 + 7);
	  aoffset8 += 8;

	  *(boffset1 +  0) = -ctemp01;
	  *(boffset1 +  1) = -ctemp02;
	  *(boffset1 +  2) = -ctemp03;
	  *(boffset1 +  3) = -ctemp04;
	  *(boffset1 +  4) = -ctemp05;
	  *(boffset1 +  5) = -ctemp06;
	  *(boffset1 +  6) = -ctemp07;
	  *(boffset1 +  7) = -ctemp08;

	  *(boffset1 +  8) = -ctemp09;
	  *(boffset1 +  9) = -ctemp10;
	  *(boffset1 + 10) = -ctemp11;
	  *(boffset1 + 11) = -ctemp12;
	  *(boffset1 + 12) = -ctemp13;
	  *(boffset1 + 13) = -ctemp14;
	  *(boffset1 + 14) = -ctemp15;
	  *(boffset1 + 15) = -ctemp16;

	  *(boffset1 + 16) = -ctemp17;
	  *(boffset1 + 17) = -ctemp18;
	  *(boffset1 + 18) = -ctemp19;
	  *(boffset1 + 19) = -ctemp20;
	  *(boffset1 + 20) = -ctemp21;
	  *(boffset1 + 21) = -ctemp22;
	  *(boffset1 + 22) = -ctemp23;
	  *(boffset1 + 23) = -ctemp24;

	  *(boffset1 + 24) = -ctemp25;
	  *(boffset1 + 25) = -ctemp26;
	  *(boffset1 + 26) = -ctemp27;
	  *(boffset1 + 27) = -ctemp28;
	  *(boffset1 + 28) = -ctemp29;
	  *(boffset1 + 29) = -ctemp30;
	  *(boffset1 + 30) = -ctemp31;
	  *(boffset1 + 31) = -ctemp32;

	  *(boffset1 + 32) = -ctemp33;
	  *(boffset1 + 33) = -ctemp34;
	  *(boffset1 + 34) = -ctemp35;
	  *(boffset1 + 35) = -ctemp36;
	  *(boffset1 + 36) = -ctemp37;
	  *(boffset1 + 37) = -ctemp38;
	  *(boffset1 + 38) = -ctemp39;
	  *(boffset1 + 39) = -ctemp40;

	  *(boffset1 + 40) = -ctemp41;
	  *(boffset1 + 41) = -ctemp42;
	  *(boffset1 + 42) = -ctemp43;
	  *(boffset1 + 43) = -ctemp44;
	  *(boffset1 + 44) = -ctemp45;
	  *(boffset1 + 45) = -ctemp46;
	  *(boffset1 + 46) = -ctemp47;
	  *(boffset1 + 47) = -ctemp48;

	  *(boffset1 + 48) = -ctemp49;
	  *(boffset1 + 49) = -ctemp50;
	  *(boffset1 + 50) = -ctemp51;
	  *(boffset1 + 51) = -ctemp52;
	  *(boffset1 + 52) = -ctemp53;
	  *(boffset1 + 53) = -ctemp54;
	  *(boffset1 + 54) = -ctemp55;
	  *(boffset1 + 55) = -ctemp56;

	  *(boffset1 + 56) = -ctemp57;
	  *(boffset1 + 57) = -ctemp58;
	  *(boffset1 + 58) = -ctemp59;
	  *(boffset1 + 59) = -ctemp60;
	  *(boffset1 + 60) = -ctemp61;
	  *(boffset1 + 61) = -ctemp62;
	  *(boffset1 + 62) = -ctemp63;
	  *(boffset1 + 63) = -ctemp64;

	  boffset1 += m * 8;
	  i --;
	}while(i > 0);
      }

      if (n & 4){
	ctemp01 = *(aoffset1 + 0);
	ctemp02 = *(aoffset1 + 1);
	ctemp03 = *(aoffset1 + 2);
	ctemp04 = *(aoffset1 + 3);
	aoffset1 += 4;

	ctemp05 = *(aoffset2 + 0);
	ctemp06 = *(aoffset2 + 1);
	ctemp07 = *(aoffset2 + 2);
	ctemp08 = *(aoffset2 + 3);
	aoffset2 += 4;

	ctemp09 = *(aoffset3 + 0);
	ctemp10 = *(aoffset3 + 1);
	ctemp11 = *(aoffset3 + 2);
	ctemp12 = *(aoffset3 + 3);
	aoffset3 += 4;

	ctemp13 = *(aoffset4 + 0);
	ctemp14 = *(aoffset4 + 1);
	ctemp15 = *(aoffset4 + 2);
	ctemp16 = *(aoffset4 + 3);
	aoffset4 += 4;

	ctemp17 = *(aoffset5 + 0);
	ctemp18 = *(aoffset5 + 1);
	ctemp19 = *(aoffset5 + 2);
	ctemp20 = *(aoffset5 + 3);
	aoffset5 += 4;

	ctemp21 = *(aoffset6 + 0);
	ctemp22 = *(aoffset6 + 1);
	ctemp23 = *(aoffset6 + 2);
	ctemp24 = *(aoffset6 + 3);
	aoffset6 += 4;

	ctemp25 = *(aoffset7 + 0);
	ctemp26 = *(aoffset7 + 1);
	ctemp27 = *(aoffset7 + 2);
	ctemp28 = *(aoffset7 + 3);
	aoffset7 += 4;

	ctemp29 = *(aoffset8 + 0);
	ctemp30 = *(aoffset8 + 1);
	ctemp31 = *(aoffset8 + 2);
	ctemp32 = *(aoffset8 + 3);
	aoffset8 += 4;

	*(boffset2 +  0) = -ctemp01;
	*(boffset2 +  1) = -ctemp02;
	*(boffset2 +  2) = -ctemp03;
	*(boffset2 +  3) = -ctemp04;
	*(boffset2 +  4) = -ctemp05;
	*(boffset2 +  5) = -ctemp06;
	*(boffset2 +  6) = -ctemp07;
	*(boffset2 +  7) = -ctemp08;
	*(boffset2 +  8) = -ctemp09;
	*(boffset2 +  9) = -ctemp10;
	*(boffset2 + 10) = -ctemp11;
	*(boffset2 + 11) = -ctemp12;
	*(boffset2 + 12) = -ctemp13;
	*(boffset2 + 13) = -ctemp14;
	*(boffset2 + 14) = -ctemp15;
	*(boffset2 + 15) = -ctemp16;

	*(boffset2 + 16) = -ctemp17;
	*(boffset2 + 17) = -ctemp18;
	*(boffset2 + 18) = -ctemp19;
	*(boffset2 + 19) = -ctemp20;
	*(boffset2 + 20) = -ctemp21;
	*(boffset2 + 21) = -ctemp22;
	*(boffset2 + 22) = -ctemp23;
	*(boffset2 + 23) = -ctemp24;
	*(boffset2 + 24) = -ctemp25;
	*(boffset2 + 25) = -ctemp26;
	*(boffset2 + 26) = -ctemp27;
	*(boffset2 + 27) = -ctemp28;
	*(boffset2 + 28) = -ctemp29;
	*(boffset2 + 29) = -ctemp30;
	*(boffset2 + 30) = -ctemp31;
	*(boffset2 + 31) = -ctemp32;

	boffset2 += 32;
      }

      if (n & 2){
	ctemp01 = *(aoffset1 + 0);
	ctemp02 = *(aoffset1 + 1);
	aoffset1 += 2;

	ctemp03 = *(aoffset2 + 0);
	ctemp04 = *(aoffset2 + 1);
	aoffset2 += 2;

	ctemp05 = *(aoffset3 + 0);
	ctemp06 = *(aoffset3 + 1);
	aoffset3 += 2;

	ctemp07 = *(aoffset4 + 0);
	ctemp08 = *(aoffset4 + 1);
	aoffset4 += 2;

	ctemp09 = *(aoffset5 + 0);
	ctemp10 = *(aoffset5 + 1);
	aoffset5 += 2;

	ctemp11 = *(aoffset6 + 0);
	ctemp12 = *(aoffset6 + 1);
	aoffset6 += 2;

	ctemp13 = *(aoffset7 + 0);
	ctemp14 = *(aoffset7 + 1);
	aoffset7 += 2;

	ctemp15 = *(aoffset8 + 0);
	ctemp16 = *(aoffset8 + 1);
	aoffset8 += 2;

	*(boffset3 +  0) = -ctemp01;
	*(boffset3 +  1) = -ctemp02;
	*(boffset3 +  2) = -ctemp03;
	*(boffset3 +  3) = -ctemp04;
	*(boffset3 +  4) = -ctemp05;
	*(boffset3 +  5) = -ctemp06;
	*(boffset3 +  6) = -ctemp07;
	*(boffset3 +  7) = -ctemp08;
	*(boffset3 +  8) = -ctemp09;
	*(boffset3 +  9) = -ctemp10;
	*(boffset3 + 10) = -ctemp11;
	*(boffset3 + 11) = -ctemp12;
	*(boffset3 + 12) = -ctemp13;
	*(boffset3 + 13) = -ctemp14;
	*(boffset3 + 14) = -ctemp15;
	*(boffset3 + 15) = -ctemp16;
	boffset3 += 16;
      }

      if (n & 1){
	ctemp01 = *(aoffset1 + 0);
	aoffset1 ++;
	ctemp02 = *(aoffset2 + 0);
	aoffset2 ++;
	ctemp03 = *(aoffset3 + 0);
	aoffset3 ++;
	ctemp04 = *(aoffset4 + 0);
	aoffset4 ++;
	ctemp05 = *(aoffset5 + 0);
	aoffset5 ++;
	ctemp06 = *(aoffset6 + 0);
	aoffset6 ++;
	ctemp07 = *(aoffset7 + 0);
	aoffset7 ++;
	ctemp08 = *(aoffset8 + 0);
	aoffset8 ++;

	*(boffset4 +  0) = -ctemp01;
	*(boffset4 +  1) = -ctemp02;
	*(boffset4 +  2) = -ctemp03;
	*(boffset4 +  3) = -ctemp04;
	*(boffset4 +  4) = -ctemp05;
	*(boffset4 +  5) = -ctemp06;
	*(boffset4 +  6) = -ctemp07;
	*(boffset4 +  7) = -ctemp08;
	boffset4 += 8;
      }

      j--;
    }while(j > 0);
  }

  if (m & 4){

    aoffset1  = aoffset;
    aoffset2  = aoffset1 + lda;
    aoffset3  = aoffset2 + lda;
    aoffset4  = aoffset3 + lda;
    aoffset += 4 * lda;

    boffset1  = boffset;
    boffset  += 32;

    i = (n >> 3);
    if (i > 0){

      do{
	ctemp01 = *(aoffset1 + 0);
	ctemp02 = *(aoffset1 + 1);
	ctemp03 = *(aoffset1 + 2);
	ctemp04 = *(aoffset1 + 3);
	ctemp05 = *(aoffset1 + 4);
	ctemp06 = *(aoffset1 + 5);
	ctemp07 = *(aoffset1 + 6);
	ctemp08 = *(aoffset1 + 7);
	aoffset1 += 8;

	ctemp09 = *(aoffset2 + 0);
	ctemp10 = *(aoffset2 + 1);
	ctemp11 = *(aoffset2 + 2);
	ctemp12 = *(aoffset2 + 3);
	ctemp13 = *(aoffset2 + 4);
	ctemp14 = *(aoffset2 + 5);
	ctemp15 = *(aoffset2 + 6);
	ctemp16 = *(aoffset2 + 7);
	aoffset2 += 8;

	ctemp17 = *(aoffset3 + 0);
	ctemp18 = *(aoffset3 + 1);
	ctemp19 = *(aoffset3 + 2);
	ctemp20 = *(aoffset3 + 3);
	ctemp21 = *(aoffset3 + 4);
	ctemp22 = *(aoffset3 + 5);
	ctemp23 = *(aoffset3 + 6);
	ctemp24 = *(aoffset3 + 7);
	aoffset3 += 8;

	ctemp25 = *(aoffset4 + 0);
	ctemp26 = *(aoffset4 + 1);
	ctemp27 = *(aoffset4 + 2);
	ctemp28 = *(aoffset4 + 3);
	ctemp29 = *(aoffset4 + 4);
	ctemp30 = *(aoffset4 + 5);
	ctemp31 = *(aoffset4 + 6);
	ctemp32 = *(aoffset4 + 7);
	aoffset4 += 8;

	*(boffset1 +  0) = -ctemp01;
	*(boffset1 +  1) = -ctemp02;
	*(boffset1 +  2) = -ctemp03;
	*(boffset1 +  3) = -ctemp04;
	*(boffset1 +  4) = -ctemp05;
	*(boffset1 +  5) = -ctemp06;
	*(boffset1 +  6) = -ctemp07;
	*(boffset1 +  7) = -ctemp08;

	*(boffset1 +  8) = -ctemp09;
	*(boffset1 +  9) = -ctemp10;
	*(boffset1 + 10) = -ctemp11;
	*(boffset1 + 11) = -ctemp12;
	*(boffset1 + 12) = -ctemp13;
	*(boffset1 + 13) = -ctemp14;
	*(boffset1 + 14) = -ctemp15;
	*(boffset1 + 15) = -ctemp16;

	*(boffset1 + 16) = -ctemp17;
	*(boffset1 + 17) = -ctemp18;
	*(boffset1 + 18) = -ctemp19;
	*(boffset1 + 19) = -ctemp20;
	*(boffset1 + 20) = -ctemp21;
	*(boffset1 + 21) = -ctemp22;
	*(boffset1 + 22) = -ctemp23;
	*(boffset1 + 23) = -ctemp24;

	*(boffset1 + 24) = -ctemp25;
	*(boffset1 + 25) = -ctemp26;
	*(boffset1 + 26) = -ctemp27;
	*(boffset1 + 27) = -ctemp28;
	*(boffset1 + 28) = -ctemp29;
	*(boffset1 + 29) = -ctemp30;
	*(boffset1 + 30) = -ctemp31;
	*(boffset1 + 31) = -ctemp32;

	boffset1 += 8 * m;
	i --;
      }while(i > 0);
    }

    if (n & 4) {
      ctemp01 = *(aoffset1 + 0);
      ctemp02 = *(aoffset1 + 1);
      ctemp03 = *(aoffset1 + 2);
      ctemp04 = *(aoffset1 + 3);
      aoffset1 += 4;

      ctemp05 = *(aoffset2 + 0);
      ctemp06 = *(aoffset2 + 1);
      ctemp07 = *(aoffset2 + 2);
      ctemp08 = *(aoffset2 + 3);
      aoffset2 += 4;

      ctemp09 = *(aoffset3 + 0);
      ctemp10 = *(aoffset3 + 1);
      ctemp11 = *(aoffset3 + 2);
      ctemp12 = *(aoffset3 + 3);
      aoffset3 += 4;

      ctemp13 = *(aoffset4 + 0);
      ctemp14 = *(aoffset4 + 1);
      ctemp15 = *(aoffset4 + 2);
      ctemp16 = *(aoffset4 + 3);
      aoffset4 += 4;

      *(boffset2 +  0) = -ctemp01;
      *(boffset2 +  1) = -ctemp02;
      *(boffset2 +  2) = -ctemp03;
      *(boffset2 +  3) = -ctemp04;
      *(boffset2 +  4) = -ctemp05;
      *(boffset2 +  5) = -ctemp06;
      *(boffset2 +  6) = -ctemp07;
      *(boffset2 +  7) = -ctemp08;

      *(boffset2 +  8) = -ctemp09;
      *(boffset2 +  9) = -ctemp10;
      *(boffset2 + 10) = -ctemp11;
      *(boffset2 + 11) = -ctemp12;
      *(boffset2 + 12) = -ctemp13;
      *(boffset2 + 13) = -ctemp14;
      *(boffset2 + 14) = -ctemp15;
      *(boffset2 + 15) = -ctemp16;
      boffset2 += 16;
    }

    if (n & 2){
      ctemp01 = *(aoffset1 + 0);
      ctemp02 = *(aoffset1 + 1);
      aoffset1 += 2;

      ctemp03 = *(aoffset2 + 0);
      ctemp04 = *(aoffset2 + 1);
      aoffset2 += 2;

      ctemp05 = *(aoffset3 + 0);
      ctemp06 = *(aoffset3 + 1);
      aoffset3 += 2;

      ctemp07 = *(aoffset4 + 0);
      ctemp08 = *(aoffset4 + 1);
      aoffset4 += 2;

      *(boffset3 +  0) = -ctemp01;
      *(boffset3 +  1) = -ctemp02;
      *(boffset3 +  2) = -ctemp03;
      *(boffset3 +  3) = -ctemp04;
      *(boffset3 +  4) = -ctemp05;
      *(boffset3 +  5) = -ctemp06;
      *(boffset3 +  6) = -ctemp07;
      *(boffset3 +  7) = -ctemp08;
      boffset3 += 8;
    }

    if (n & 1){
      ctemp01 = *(aoffset1 + 0);
      aoffset1 ++;
      ctemp02 = *(aoffset2 + 0);
      aoffset2 ++;
      ctemp03 = *(aoffset3 + 0);
      aoffset3 ++;
      ctemp04 = *(aoffset4 + 0);
      aoffset4 ++;

      *(boffset4 +  0) = -ctemp01;
      *(boffset4 +  1) = -ctemp02;
      *(boffset4 +  2) = -ctemp03;
      *(boffset4 +  3) = -ctemp04;
      boffset4 += 4;
    }
  }

  if (m & 2){
    aoffset1  = aoffset;
    aoffset2  = aoffset1 + lda;
    aoffset += 2 * lda;

    boffset1  = boffset;
    boffset  += 16;

    i = (n >> 3);
    if (i > 0){
      do{
	ctemp01 = *(aoffset1 + 0);
	ctemp02 = *(aoffset1 + 1);
	ctemp03 = *(aoffset1 + 2);
	ctemp04 = *(aoffset1 + 3);
	ctemp05 = *(aoffset1 + 4);
	ctemp06 = *(aoffset1 + 5);
	ctemp07 = *(aoffset1 + 6);
	ctemp08 = *(aoffset1 + 7);
	aoffset1 += 8;

	ctemp09 = *(aoffset2 + 0);
	ctemp10 = *(aoffset2 + 1);
	ctemp11 = *(aoffset2 + 2);
	ctemp12 = *(aoffset2 + 3);
	ctemp13 = *(aoffset2 + 4);
	ctemp14 = *(aoffset2 + 5);
	ctemp15 = *(aoffset2 + 6);
	ctemp16 = *(aoffset2 + 7);
	aoffset2 += 8;

	*(boffset1 +  0) = -ctemp01;
	*(boffset1 +  1) = -ctemp02;
	*(boffset1 +  2) = -ctemp03;
	*(boffset1 +  3) = -ctemp04;
	*(boffset1 +  4) = -ctemp05;
	*(boffset1 +  5) = -ctemp06;
	*(boffset1 +  6) = -ctemp07;
	*(boffset1 +  7) = -ctemp08;

	*(boffset1 +  8) = -ctemp09;
	*(boffset1 +  9) = -ctemp10;
	*(boffset1 + 10) = -ctemp11;
	*(boffset1 + 11) = -ctemp12;
	*(boffset1 + 12) = -ctemp13;
	*(boffset1 + 13) = -ctemp14;
	*(boffset1 + 14) = -ctemp15;
	*(boffset1 + 15) = -ctemp16;

	boffset1 += 8 * m;
	i --;
      }while(i > 0);
    }

    if (n & 4){
      ctemp01 = *(aoffset1 + 0);
      ctemp02 = *(aoffset1 + 1);
      ctemp03 = *(aoffset1 + 2);
      ctemp04 = *(aoffset1 + 3);
      aoffset1 += 4;

      ctemp05 = *(aoffset2 + 0);
      ctemp06 = *(aoffset2 + 1);
      ctemp07 = *(aoffset2 + 2);
      ctemp08 = *(aoffset2 + 3);
      aoffset2 += 4;

      *(boffset2 +  0) = -ctemp01;
      *(boffset2 +  1) = -ctemp02;
      *(boffset2 +  2) = -ctemp03;
      *(boffset2 +  3) = -ctemp04;
      *(boffset2 +  4) = -ctemp05;
      *(boffset2 +  5) = -ctemp06;
      *(boffset2 +  6) = -ctemp07;
      *(boffset2 +  7) = -ctemp08;
      boffset2 += 8;
    }

    if (n & 2){
      ctemp01 = *(aoffset1 + 0);
      ctemp02 = *(aoffset1 + 1);
      aoffset1 += 2;

      ctemp03 = *(aoffset2 + 0);
      ctemp04 = *(aoffset2 + 1);
      aoffset2 += 2;

      *(boffset3 +  0) = -ctemp01;
      *(boffset3 +  1) = -ctemp02;
      *(boffset3 +  2) = -ctemp03;
      *(boffset3 +  3) = -ctemp04;
      boffset3 += 4;
    }

    if (n & 1){
      ctemp01 = *(aoffset1 + 0);
      aoffset1 ++;
      ctemp02 = *(aoffset2 + 0);
      aoffset2 ++;

      *(boffset4 +  0) = -ctemp01;
      *(boffset4 +  1) = -ctemp02;
      boffset4 += 2;
    }
  }

  if (m & 1){
    aoffset1  = aoffset;
    // aoffset += lda;

    boffset1  = boffset;
    // boffset  += 8;

    i = (n >> 3);
    if (i > 0){
      do{
	ctemp01 = *(aoffset1 + 0);
	ctemp02 = *(aoffset1 + 1);
	ctemp03 = *(aoffset1 + 2);
	ctemp04 = *(aoffset1 + 3);
	ctemp05 = *(aoffset1 + 4);
	ctemp06 = *(aoffset1 + 5);
	ctemp07 = *(aoffset1 + 6);
	ctemp08 = *(aoffset1 + 7);
	aoffset1 += 8;

	*(boffset1 +  0) = -ctemp01;
	*(boffset1 +  1) = -ctemp02;
	*(boffset1 +  2) = -ctemp03;
	*(boffset1 +  3) = -ctemp04;
	*(boffset1 +  4) = -ctemp05;
	*(boffset1 +  5) = -ctemp06;
	*(boffset1 +  6) = -ctemp07;
	*(boffset1 +  7) = -ctemp08;

	boffset1 += 8 * m;
	 i --;
       }while(i > 0);
     }

     if (n & 4){
       ctemp01 = *(aoffset1 + 0);
       ctemp02 = *(aoffset1 + 1);
       ctemp03 = *(aoffset1 + 2);
       ctemp04 = *(aoffset1 + 3);
       aoffset1 += 4;

       *(boffset2 +  0) = -ctemp01;
       *(boffset2 +  1) = -ctemp02;
       *(boffset2 +  2) = -ctemp03;
       *(boffset2 +  3) = -ctemp04;
       // boffset2 += 4;
     }

     if (n & 2){
       ctemp01 = *(aoffset1 + 0);
       ctemp02 = *(aoffset1 + 1);
       aoffset1 += 2;

       *(boffset3 +  0) = -ctemp01;
       *(boffset3 +  1) = -ctemp02;
       // boffset3 += 2;
     }

     if (n & 1){
       ctemp01 = *(aoffset1 + 0);
       aoffset1 ++;
      *(boffset4 +  0) = -ctemp01;
      boffset4 ++;
    }
  }

  return 0;
}

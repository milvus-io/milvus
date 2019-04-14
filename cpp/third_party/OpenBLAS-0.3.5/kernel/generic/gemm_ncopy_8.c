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

  FLOAT *boffset;
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


  aoffset = a;
  boffset = b;

  j = (n >> 3);
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

      i = (m >> 3);
      if (i > 0){
	do{
	  ctemp01 = *(aoffset1 +  0);
	  ctemp02 = *(aoffset1 +  1);
	  ctemp03 = *(aoffset1 +  2);
	  ctemp04 = *(aoffset1 +  3);
	  ctemp05 = *(aoffset1 +  4);
	  ctemp06 = *(aoffset1 +  5);
	  ctemp07 = *(aoffset1 +  6);
	  ctemp08 = *(aoffset1 +  7);

	  ctemp09 = *(aoffset2 +  0);
	  ctemp10 = *(aoffset2 +  1);
	  ctemp11 = *(aoffset2 +  2);
	  ctemp12 = *(aoffset2 +  3);
	  ctemp13 = *(aoffset2 +  4);
	  ctemp14 = *(aoffset2 +  5);
	  ctemp15 = *(aoffset2 +  6);
	  ctemp16 = *(aoffset2 +  7);

	  ctemp17 = *(aoffset3 +  0);
	  ctemp18 = *(aoffset3 +  1);
	  ctemp19 = *(aoffset3 +  2);
	  ctemp20 = *(aoffset3 +  3);
	  ctemp21 = *(aoffset3 +  4);
	  ctemp22 = *(aoffset3 +  5);
	  ctemp23 = *(aoffset3 +  6);
	  ctemp24 = *(aoffset3 +  7);

	  ctemp25 = *(aoffset4 +  0);
	  ctemp26 = *(aoffset4 +  1);
	  ctemp27 = *(aoffset4 +  2);
	  ctemp28 = *(aoffset4 +  3);
	  ctemp29 = *(aoffset4 +  4);
	  ctemp30 = *(aoffset4 +  5);
	  ctemp31 = *(aoffset4 +  6);
	  ctemp32 = *(aoffset4 +  7);

	  ctemp33 = *(aoffset5 +  0);
	  ctemp34 = *(aoffset5 +  1);
	  ctemp35 = *(aoffset5 +  2);
	  ctemp36 = *(aoffset5 +  3);
	  ctemp37 = *(aoffset5 +  4);
	  ctemp38 = *(aoffset5 +  5);
	  ctemp39 = *(aoffset5 +  6);
	  ctemp40 = *(aoffset5 +  7);

	  ctemp41 = *(aoffset6 +  0);
	  ctemp42 = *(aoffset6 +  1);
	  ctemp43 = *(aoffset6 +  2);
	  ctemp44 = *(aoffset6 +  3);
	  ctemp45 = *(aoffset6 +  4);
	  ctemp46 = *(aoffset6 +  5);
	  ctemp47 = *(aoffset6 +  6);
	  ctemp48 = *(aoffset6 +  7);

	  ctemp49 = *(aoffset7 +  0);
	  ctemp50 = *(aoffset7 +  1);
	  ctemp51 = *(aoffset7 +  2);
	  ctemp52 = *(aoffset7 +  3);
	  ctemp53 = *(aoffset7 +  4);
	  ctemp54 = *(aoffset7 +  5);
	  ctemp55 = *(aoffset7 +  6);
	  ctemp56 = *(aoffset7 +  7);

	  ctemp57 = *(aoffset8 +  0);
	  ctemp58 = *(aoffset8 +  1);
	  ctemp59 = *(aoffset8 +  2);
	  ctemp60 = *(aoffset8 +  3);
	  ctemp61 = *(aoffset8 +  4);
	  ctemp62 = *(aoffset8 +  5);
	  ctemp63 = *(aoffset8 +  6);
	  ctemp64 = *(aoffset8 +  7);

	  *(boffset +  0) = ctemp01;
	  *(boffset +  1) = ctemp09;
	  *(boffset +  2) = ctemp17;
	  *(boffset +  3) = ctemp25;
	  *(boffset +  4) = ctemp33;
	  *(boffset +  5) = ctemp41;
	  *(boffset +  6) = ctemp49;
	  *(boffset +  7) = ctemp57;

	  *(boffset +  8) = ctemp02;
	  *(boffset +  9) = ctemp10;
	  *(boffset + 10) = ctemp18;
	  *(boffset + 11) = ctemp26;
	  *(boffset + 12) = ctemp34;
	  *(boffset + 13) = ctemp42;
	  *(boffset + 14) = ctemp50;
	  *(boffset + 15) = ctemp58;

	  *(boffset + 16) = ctemp03;
	  *(boffset + 17) = ctemp11;
	  *(boffset + 18) = ctemp19;
	  *(boffset + 19) = ctemp27;
	  *(boffset + 20) = ctemp35;
	  *(boffset + 21) = ctemp43;
	  *(boffset + 22) = ctemp51;
	  *(boffset + 23) = ctemp59;

	  *(boffset + 24) = ctemp04;
	  *(boffset + 25) = ctemp12;
	  *(boffset + 26) = ctemp20;
	  *(boffset + 27) = ctemp28;
	  *(boffset + 28) = ctemp36;
	  *(boffset + 29) = ctemp44;
	  *(boffset + 30) = ctemp52;
	  *(boffset + 31) = ctemp60;

	  *(boffset + 32) = ctemp05;
	  *(boffset + 33) = ctemp13;
	  *(boffset + 34) = ctemp21;
	  *(boffset + 35) = ctemp29;
	  *(boffset + 36) = ctemp37;
	  *(boffset + 37) = ctemp45;
	  *(boffset + 38) = ctemp53;
	  *(boffset + 39) = ctemp61;

	  *(boffset + 40) = ctemp06;
	  *(boffset + 41) = ctemp14;
	  *(boffset + 42) = ctemp22;
	  *(boffset + 43) = ctemp30;
	  *(boffset + 44) = ctemp38;
	  *(boffset + 45) = ctemp46;
	  *(boffset + 46) = ctemp54;
	  *(boffset + 47) = ctemp62;

	  *(boffset + 48) = ctemp07;
	  *(boffset + 49) = ctemp15;
	  *(boffset + 50) = ctemp23;
	  *(boffset + 51) = ctemp31;
	  *(boffset + 52) = ctemp39;
	  *(boffset + 53) = ctemp47;
	  *(boffset + 54) = ctemp55;
	  *(boffset + 55) = ctemp63;

	  *(boffset + 56) = ctemp08;
	  *(boffset + 57) = ctemp16;
	  *(boffset + 58) = ctemp24;
	  *(boffset + 59) = ctemp32;
	  *(boffset + 60) = ctemp40;
	  *(boffset + 61) = ctemp48;
	  *(boffset + 62) = ctemp56;
	  *(boffset + 63) = ctemp64;

	  aoffset1 +=  8;
	  aoffset2 +=  8;
	  aoffset3 +=  8;
	  aoffset4 +=  8;
	  aoffset5 +=  8;
	  aoffset6 +=  8;
	  aoffset7 +=  8;
	  aoffset8 +=  8;
	  boffset  += 64;
	  i --;
	}while(i > 0);
      }

      i = (m & 7);
      if (i > 0){
	do{
	  ctemp01 = *(aoffset1 +  0);
	  ctemp09 = *(aoffset2 +  0);
	  ctemp17 = *(aoffset3 +  0);
	  ctemp25 = *(aoffset4 +  0);
	  ctemp33 = *(aoffset5 +  0);
	  ctemp41 = *(aoffset6 +  0);
	  ctemp49 = *(aoffset7 +  0);
	  ctemp57 = *(aoffset8 +  0);

	  *(boffset +  0) = ctemp01;
	  *(boffset +  1) = ctemp09;
	  *(boffset +  2) = ctemp17;
	  *(boffset +  3) = ctemp25;
	  *(boffset +  4) = ctemp33;
	  *(boffset +  5) = ctemp41;
	  *(boffset +  6) = ctemp49;
	  *(boffset +  7) = ctemp57;

	  aoffset1 ++;
	  aoffset2 ++;
	  aoffset3 ++;
	  aoffset4 ++;
	  aoffset5 ++;
	  aoffset6 ++;
	  aoffset7 ++;
	  aoffset8 ++;

	  boffset += 8;
	  i --;
	}while(i > 0);
      }
      j--;
    }while(j > 0);
  } /* end of if(j > 0) */

  if (n & 4){
    aoffset1  = aoffset;
    aoffset2  = aoffset1 + lda;
    aoffset3  = aoffset2 + lda;
    aoffset4  = aoffset3 + lda;
    aoffset += 4 * lda;

    i = (m >> 2);
    if (i > 0){
      do{
	ctemp01 = *(aoffset1 +  0);
	ctemp02 = *(aoffset1 +  1);
	ctemp03 = *(aoffset1 +  2);
	ctemp04 = *(aoffset1 +  3);

	ctemp05 = *(aoffset2 +  0);
	ctemp06 = *(aoffset2 +  1);
	ctemp07 = *(aoffset2 +  2);
	ctemp08 = *(aoffset2 +  3);

	ctemp09 = *(aoffset3 +  0);
	ctemp10 = *(aoffset3 +  1);
	ctemp11 = *(aoffset3 +  2);
	ctemp12 = *(aoffset3 +  3);

	ctemp13 = *(aoffset4 +  0);
	ctemp14 = *(aoffset4 +  1);
	ctemp15 = *(aoffset4 +  2);
	ctemp16 = *(aoffset4 +  3);

	*(boffset +  0) = ctemp01;
	*(boffset +  1) = ctemp05;
	*(boffset +  2) = ctemp09;
	*(boffset +  3) = ctemp13;

	*(boffset +  4) = ctemp02;
	*(boffset +  5) = ctemp06;
	*(boffset +  6) = ctemp10;
	*(boffset +  7) = ctemp14;

	*(boffset +  8) = ctemp03;
	*(boffset +  9) = ctemp07;
	*(boffset + 10) = ctemp11;
	*(boffset + 11) = ctemp15;

	*(boffset + 12) = ctemp04;
	*(boffset + 13) = ctemp08;
	*(boffset + 14) = ctemp12;
	*(boffset + 15) = ctemp16;

	aoffset1 +=  4;
	aoffset2 +=  4;
	aoffset3 +=  4;
	aoffset4 +=  4;
	boffset  +=  16;
	i --;
      }while(i > 0);
    }

    i = (m & 3);
    if (i > 0){
      do{
	ctemp01 = *(aoffset1 +  0);
	ctemp02 = *(aoffset2 +  0);
	ctemp03 = *(aoffset3 +  0);
	ctemp04 = *(aoffset4 +  0);

	*(boffset +  0) = ctemp01;
	*(boffset +  1) = ctemp02;
	*(boffset +  2) = ctemp03;
	*(boffset +  3) = ctemp04;

	aoffset1 ++;
	aoffset2 ++;
	aoffset3 ++;
	aoffset4 ++;

	boffset += 4;
	i --;
      }while(i > 0);
    }
  } /* end of if(j > 0) */

  if (n & 2){
    aoffset1  = aoffset;
    aoffset2  = aoffset1 + lda;
    aoffset += 2 * lda;

    i = (m >> 1);
    if (i > 0){
      do{
	ctemp01 = *(aoffset1 +  0);
	ctemp02 = *(aoffset1 +  1);
	ctemp03 = *(aoffset2 +  0);
	ctemp04 = *(aoffset2 +  1);

	*(boffset +  0) = ctemp01;
	*(boffset +  1) = ctemp03;
	*(boffset +  2) = ctemp02;
	*(boffset +  3) = ctemp04;

	aoffset1 +=  2;
	aoffset2 +=  2;
	boffset  +=  4;
	i --;
      }while(i > 0);
    }

    if (m & 1){
      ctemp01 = *(aoffset1 +  0);
      ctemp02 = *(aoffset2 +  0);

      *(boffset +  0) = ctemp01;
      *(boffset +  1) = ctemp02;

      aoffset1 ++;
      aoffset2 ++;
      boffset += 2;
    }
  } /* end of if(j > 0) */

  if (n & 1){
    aoffset1  = aoffset;

    i = m;
    if (i > 0){
      do{
	ctemp01 = *(aoffset1 +  0);

	*(boffset +  0) = ctemp01;

	aoffset1 ++;
	boffset  ++;
	i --;
      }while(i > 0);
    }

  } /* end of if(j > 0) */

  return 0;
}

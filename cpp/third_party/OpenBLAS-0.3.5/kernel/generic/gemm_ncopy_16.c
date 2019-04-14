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
  FLOAT *aoffset9, *aoffset10, *aoffset11, *aoffset12;
  FLOAT *aoffset13, *aoffset14, *aoffset15, *aoffset16;

  FLOAT *boffset;
  FLOAT ctemp01, ctemp02, ctemp03, ctemp04;
  FLOAT ctemp05, ctemp06, ctemp07, ctemp08;
  FLOAT ctemp09, ctemp10, ctemp11, ctemp12;
  FLOAT ctemp13, ctemp14, ctemp15, ctemp16;
  FLOAT ctemp17, ctemp18, ctemp19, ctemp20;
  FLOAT ctemp21, ctemp22, ctemp23, ctemp24;
  FLOAT ctemp25, ctemp26, ctemp27, ctemp28;
  FLOAT ctemp29, ctemp30, ctemp31, ctemp32;

  aoffset = a;
  boffset = b;

  j = (n >> 4);
  if (j > 0){
    do{
      aoffset1  = aoffset;
      aoffset2  = aoffset1  + lda;
      aoffset3  = aoffset2  + lda;
      aoffset4  = aoffset3  + lda;
      aoffset5  = aoffset4  + lda;
      aoffset6  = aoffset5  + lda;
      aoffset7  = aoffset6  + lda;
      aoffset8  = aoffset7  + lda;
      aoffset9  = aoffset8  + lda;
      aoffset10 = aoffset9  + lda;
      aoffset11 = aoffset10 + lda;
      aoffset12 = aoffset11 + lda;
      aoffset13 = aoffset12 + lda;
      aoffset14 = aoffset13 + lda;
      aoffset15 = aoffset14 + lda;
      aoffset16 = aoffset15 + lda;
      aoffset += 16 * lda;

      i = (m >> 1);
      if (i > 0){
	do{
	  ctemp01 = *(aoffset1 +  0);
	  ctemp02 = *(aoffset1 +  1);
	  ctemp03 = *(aoffset2 +  0);
	  ctemp04 = *(aoffset2 +  1);

	  ctemp05 = *(aoffset3 +  0);
	  ctemp06 = *(aoffset3 +  1);
	  ctemp07 = *(aoffset4 +  0);
	  ctemp08 = *(aoffset4 +  1);

	  ctemp09 = *(aoffset5 +  0);
	  ctemp10 = *(aoffset5 +  1);
	  ctemp11 = *(aoffset6 +  0);
	  ctemp12 = *(aoffset6 +  1);

	  ctemp13 = *(aoffset7 +  0);
	  ctemp14 = *(aoffset7 +  1);
	  ctemp15 = *(aoffset8 +  0);
	  ctemp16 = *(aoffset8 +  1);

	  ctemp17 = *(aoffset9 +  0);
	  ctemp18 = *(aoffset9 +  1);
	  ctemp19 = *(aoffset10 +  0);
	  ctemp20 = *(aoffset10 +  1);

	  ctemp21 = *(aoffset11 +  0);
	  ctemp22 = *(aoffset11 +  1);
	  ctemp23 = *(aoffset12 +  0);
	  ctemp24 = *(aoffset12 +  1);

	  ctemp25 = *(aoffset13 +  0);
	  ctemp26 = *(aoffset13 +  1);
	  ctemp27 = *(aoffset14 +  0);
	  ctemp28 = *(aoffset14 +  1);

	  ctemp29 = *(aoffset15 +  0);
	  ctemp30 = *(aoffset15 +  1);
	  ctemp31 = *(aoffset16 +  0);
	  ctemp32 = *(aoffset16 +  1);

	  *(boffset +  0) = ctemp01;
	  *(boffset +  1) = ctemp03;
	  *(boffset +  2) = ctemp05;
	  *(boffset +  3) = ctemp07;
	  *(boffset +  4) = ctemp09;
	  *(boffset +  5) = ctemp11;
	  *(boffset +  6) = ctemp13;
	  *(boffset +  7) = ctemp15;

	  *(boffset +  8) = ctemp17;
	  *(boffset +  9) = ctemp19;
	  *(boffset + 10) = ctemp21;
	  *(boffset + 11) = ctemp23;
	  *(boffset + 12) = ctemp25;
	  *(boffset + 13) = ctemp27;
	  *(boffset + 14) = ctemp29;
	  *(boffset + 15) = ctemp31;

	  *(boffset + 16) = ctemp02;
	  *(boffset + 17) = ctemp04;
	  *(boffset + 18) = ctemp06;
	  *(boffset + 19) = ctemp08;
	  *(boffset + 20) = ctemp10;
	  *(boffset + 21) = ctemp12;
	  *(boffset + 22) = ctemp14;
	  *(boffset + 23) = ctemp16;

	  *(boffset + 24) = ctemp18;
	  *(boffset + 25) = ctemp20;
	  *(boffset + 26) = ctemp22;
	  *(boffset + 27) = ctemp24;
	  *(boffset + 28) = ctemp26;
	  *(boffset + 29) = ctemp28;
	  *(boffset + 30) = ctemp30;
	  *(boffset + 31) = ctemp32;

	  aoffset1 +=  2;
	  aoffset2 +=  2;
	  aoffset3 +=  2;
	  aoffset4 +=  2;
	  aoffset5 +=  2;
	  aoffset6 +=  2;
	  aoffset7 +=  2;
	  aoffset8 +=  2;

	  aoffset9  +=  2;
	  aoffset10 +=  2;
	  aoffset11 +=  2;
	  aoffset12 +=  2;
	  aoffset13 +=  2;
	  aoffset14 +=  2;
	  aoffset15 +=  2;
	  aoffset16 +=  2;
	  boffset   += 32;

	  i --;
	}while(i > 0);
      }

      if (m & 1){
	ctemp01 = *(aoffset1 +  0);
	ctemp03 = *(aoffset2 +  0);
	ctemp05 = *(aoffset3 +  0);
	ctemp07 = *(aoffset4 +  0);
	ctemp09 = *(aoffset5 +  0);
	ctemp11 = *(aoffset6 +  0);
	ctemp13 = *(aoffset7 +  0);
	ctemp15 = *(aoffset8 +  0);

	ctemp17 = *(aoffset9 +  0);
	ctemp19 = *(aoffset10 +  0);
	ctemp21 = *(aoffset11 +  0);
	ctemp23 = *(aoffset12 +  0);
	ctemp25 = *(aoffset13 +  0);
	ctemp27 = *(aoffset14 +  0);
	ctemp29 = *(aoffset15 +  0);
	ctemp31 = *(aoffset16 +  0);

	*(boffset +  0) = ctemp01;
	*(boffset +  1) = ctemp03;
	*(boffset +  2) = ctemp05;
	*(boffset +  3) = ctemp07;
	*(boffset +  4) = ctemp09;
	*(boffset +  5) = ctemp11;
	*(boffset +  6) = ctemp13;
	*(boffset +  7) = ctemp15;

	*(boffset +  8) = ctemp17;
	*(boffset +  9) = ctemp19;
	*(boffset + 10) = ctemp21;
	*(boffset + 11) = ctemp23;
	*(boffset + 12) = ctemp25;
	*(boffset + 13) = ctemp27;
	*(boffset + 14) = ctemp29;
	*(boffset + 15) = ctemp31;

	boffset   += 16;
      }
      j--;
    }while(j > 0);
  } /* end of if(j > 0) */

  if (n & 8){
    aoffset1  = aoffset;
    aoffset2  = aoffset1  + lda;
    aoffset3  = aoffset2  + lda;
    aoffset4  = aoffset3  + lda;
    aoffset5  = aoffset4  + lda;
    aoffset6  = aoffset5  + lda;
    aoffset7  = aoffset6  + lda;
    aoffset8  = aoffset7  + lda;
    aoffset += 8 * lda;

    i = (m >> 1);
    if (i > 0){
      do{
	ctemp01 = *(aoffset1 +  0);
	ctemp02 = *(aoffset1 +  1);
	ctemp03 = *(aoffset2 +  0);
	ctemp04 = *(aoffset2 +  1);

	ctemp05 = *(aoffset3 +  0);
	ctemp06 = *(aoffset3 +  1);
	ctemp07 = *(aoffset4 +  0);
	ctemp08 = *(aoffset4 +  1);

	ctemp09 = *(aoffset5 +  0);
	ctemp10 = *(aoffset5 +  1);
	ctemp11 = *(aoffset6 +  0);
	ctemp12 = *(aoffset6 +  1);

	ctemp13 = *(aoffset7 +  0);
	ctemp14 = *(aoffset7 +  1);
	ctemp15 = *(aoffset8 +  0);
	ctemp16 = *(aoffset8 +  1);

	*(boffset +  0) = ctemp01;
	*(boffset +  1) = ctemp03;
	*(boffset +  2) = ctemp05;
	*(boffset +  3) = ctemp07;
	*(boffset +  4) = ctemp09;
	*(boffset +  5) = ctemp11;
	*(boffset +  6) = ctemp13;
	*(boffset +  7) = ctemp15;

	*(boffset +  8) = ctemp02;
	*(boffset +  9) = ctemp04;
	*(boffset + 10) = ctemp06;
	*(boffset + 11) = ctemp08;
	*(boffset + 12) = ctemp10;
	*(boffset + 13) = ctemp12;
	*(boffset + 14) = ctemp14;
	*(boffset + 15) = ctemp16;

	aoffset1 +=  2;
	aoffset2 +=  2;
	aoffset3 +=  2;
	aoffset4 +=  2;
	aoffset5 +=  2;
	aoffset6 +=  2;
	aoffset7 +=  2;
	aoffset8 +=  2;

	boffset   += 16;

	i --;
      }while(i > 0);
    }

    if (m & 1){
      ctemp01 = *(aoffset1 +  0);
      ctemp03 = *(aoffset2 +  0);
      ctemp05 = *(aoffset3 +  0);
      ctemp07 = *(aoffset4 +  0);
      ctemp09 = *(aoffset5 +  0);
      ctemp11 = *(aoffset6 +  0);
      ctemp13 = *(aoffset7 +  0);
      ctemp15 = *(aoffset8 +  0);

      *(boffset +  0) = ctemp01;
      *(boffset +  1) = ctemp03;
      *(boffset +  2) = ctemp05;
      *(boffset +  3) = ctemp07;
      *(boffset +  4) = ctemp09;
      *(boffset +  5) = ctemp11;
      *(boffset +  6) = ctemp13;
      *(boffset +  7) = ctemp15;

      boffset   += 8;
    }
  }

  if (n & 4){
    aoffset1  = aoffset;
    aoffset2  = aoffset1  + lda;
    aoffset3  = aoffset2  + lda;
    aoffset4  = aoffset3  + lda;
    aoffset += 4 * lda;

    i = (m >> 1);
    if (i > 0){
      do{
	ctemp01 = *(aoffset1 +  0);
	ctemp02 = *(aoffset1 +  1);
	ctemp03 = *(aoffset2 +  0);
	ctemp04 = *(aoffset2 +  1);

	ctemp05 = *(aoffset3 +  0);
	ctemp06 = *(aoffset3 +  1);
	ctemp07 = *(aoffset4 +  0);
	ctemp08 = *(aoffset4 +  1);

	*(boffset +  0) = ctemp01;
	*(boffset +  1) = ctemp03;
	*(boffset +  2) = ctemp05;
	*(boffset +  3) = ctemp07;
	*(boffset +  4) = ctemp02;
	*(boffset +  5) = ctemp04;
	*(boffset +  6) = ctemp06;
	*(boffset +  7) = ctemp08;

	aoffset1 +=  2;
	aoffset2 +=  2;
	aoffset3 +=  2;
	aoffset4 +=  2;
	boffset   += 8;

	i --;
      }while(i > 0);
    }

    if (m & 1){
      ctemp01 = *(aoffset1 +  0);
      ctemp03 = *(aoffset2 +  0);
      ctemp05 = *(aoffset3 +  0);
      ctemp07 = *(aoffset4 +  0);

      *(boffset +  0) = ctemp01;
      *(boffset +  1) = ctemp03;
      *(boffset +  2) = ctemp05;
      *(boffset +  3) = ctemp07;
      boffset   += 4;
    }
  }

  if (n & 2){
    aoffset1  = aoffset;
    aoffset2  = aoffset1  + lda;
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
	boffset   += 4;

	i --;
      }while(i > 0);
    }

    if (m & 1){
      ctemp01 = *(aoffset1 +  0);
      ctemp03 = *(aoffset2 +  0);

      *(boffset +  0) = ctemp01;
      *(boffset +  1) = ctemp03;
      boffset   += 2;
    }
  }

  if (n & 1){
    aoffset1  = aoffset;

    i = (m >> 1);
    if (i > 0){
      do{
	ctemp01 = *(aoffset1 +  0);
	ctemp02 = *(aoffset1 +  1);

	*(boffset +  0) = ctemp01;
	*(boffset +  1) = ctemp02;

	aoffset1 +=  2;
	boffset   += 2;

	i --;
      }while(i > 0);
    }

    if (m & 1){
      ctemp01 = *(aoffset1 +  0);

      *(boffset +  0) = ctemp01;
      // boffset   += 1;
    }
  }

  return 0;
}

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
  lda *= 2;

#if 0
  fprintf(stderr, "m = %d n = %d\n", m,n );
#endif

  j = (n >> 2);
  if (j > 0){
    do{
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

	  *(boffset +  0) = ctemp01;
	  *(boffset +  1) = ctemp02;
	  *(boffset +  2) = ctemp09;
	  *(boffset +  3) = ctemp10;
	  *(boffset +  4) = ctemp17;
	  *(boffset +  5) = ctemp18;
	  *(boffset +  6) = ctemp25;
	  *(boffset +  7) = ctemp26;

	  *(boffset +  8) = ctemp03;
	  *(boffset +  9) = ctemp04;
	  *(boffset + 10) = ctemp11;
	  *(boffset + 11) = ctemp12;
	  *(boffset + 12) = ctemp19;
	  *(boffset + 13) = ctemp20;
	  *(boffset + 14) = ctemp27;
	  *(boffset + 15) = ctemp28;

	  *(boffset + 16) = ctemp05;
	  *(boffset + 17) = ctemp06;
	  *(boffset + 18) = ctemp13;
	  *(boffset + 19) = ctemp14;
	  *(boffset + 20) = ctemp21;
	  *(boffset + 21) = ctemp22;
	  *(boffset + 22) = ctemp29;
	  *(boffset + 23) = ctemp30;

	  *(boffset + 24) = ctemp07;
	  *(boffset + 25) = ctemp08;
	  *(boffset + 26) = ctemp15;
	  *(boffset + 27) = ctemp16;
	  *(boffset + 28) = ctemp23;
	  *(boffset + 29) = ctemp24;
	  *(boffset + 30) = ctemp31;
	  *(boffset + 31) = ctemp32;

	  aoffset1 +=  8;
	  aoffset2 +=  8;
	  aoffset3 +=  8;
	  aoffset4 +=  8;
	  boffset  += 32;
	  i --;
	}while(i > 0);
      }

      if (m & 2) {
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
	*(boffset +  1) = ctemp02;
	*(boffset +  2) = ctemp05;
	*(boffset +  3) = ctemp06;
	*(boffset +  4) = ctemp09;
	*(boffset +  5) = ctemp10;
	*(boffset +  6) = ctemp13;
	*(boffset +  7) = ctemp14;

	*(boffset +  8) = ctemp03;
	*(boffset +  9) = ctemp04;
	*(boffset + 10) = ctemp07;
	*(boffset + 11) = ctemp08;
	*(boffset + 12) = ctemp11;
	*(boffset + 13) = ctemp12;
	*(boffset + 14) = ctemp15;
	*(boffset + 15) = ctemp16;

	aoffset1 +=  4;
	aoffset2 +=  4;
	aoffset3 +=  4;
	aoffset4 +=  4;
	boffset  += 16;
      }

      if (m & 1) {
	ctemp01 = *(aoffset1 +  0);
	ctemp02 = *(aoffset1 +  1);

	ctemp03 = *(aoffset2 +  0);
	ctemp04 = *(aoffset2 +  1);

	ctemp05 = *(aoffset3 +  0);
	ctemp06 = *(aoffset3 +  1);

	ctemp07 = *(aoffset4 +  0);
	ctemp08 = *(aoffset4 +  1);

	*(boffset +  0) = ctemp01;
	*(boffset +  1) = ctemp02;
	*(boffset +  2) = ctemp03;
	*(boffset +  3) = ctemp04;
	*(boffset +  4) = ctemp05;
	*(boffset +  5) = ctemp06;
	*(boffset +  6) = ctemp07;
	*(boffset +  7) = ctemp08;

	/* aoffset1 +=  2;
	aoffset2 +=  2;
	aoffset3 +=  2;
	aoffset4 +=  2; */
	boffset  +=  8;
      }
      j--;
    }while(j > 0);
  } /* end of if(j > 0) */

  if (n & 2){
    aoffset1  = aoffset;
    aoffset2  = aoffset1 + lda;
    aoffset += 2 * lda;

    i = (m >> 2);
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

	*(boffset +  0) = ctemp01;
	*(boffset +  1) = ctemp02;
	*(boffset +  2) = ctemp09;
	*(boffset +  3) = ctemp10;
	*(boffset +  4) = ctemp03;
	*(boffset +  5) = ctemp04;
	*(boffset +  6) = ctemp11;
	*(boffset +  7) = ctemp12;

	*(boffset +  8) = ctemp05;
	*(boffset +  9) = ctemp06;
	*(boffset + 10) = ctemp13;
	*(boffset + 11) = ctemp14;
	*(boffset + 12) = ctemp07;
	*(boffset + 13) = ctemp08;
	*(boffset + 14) = ctemp15;
	*(boffset + 15) = ctemp16;

	aoffset1 +=  8;
	aoffset2 +=  8;
	boffset  += 16;
	i --;
      }while(i > 0);
    }

    if (m & 2) {
      ctemp01 = *(aoffset1 +  0);
      ctemp02 = *(aoffset1 +  1);
      ctemp03 = *(aoffset1 +  2);
      ctemp04 = *(aoffset1 +  3);

      ctemp05 = *(aoffset2 +  0);
      ctemp06 = *(aoffset2 +  1);
      ctemp07 = *(aoffset2 +  2);
      ctemp08 = *(aoffset2 +  3);

      *(boffset +  0) = ctemp01;
      *(boffset +  1) = ctemp02;
      *(boffset +  2) = ctemp05;
      *(boffset +  3) = ctemp06;
      *(boffset +  4) = ctemp03;
      *(boffset +  5) = ctemp04;
      *(boffset +  6) = ctemp07;
      *(boffset +  7) = ctemp08;

      aoffset1 +=  4;
      aoffset2 +=  4;
      boffset  +=  8;
    }

    if (m & 1) {
      ctemp01 = *(aoffset1 +  0);
      ctemp02 = *(aoffset1 +  1);

      ctemp03 = *(aoffset2 +  0);
      ctemp04 = *(aoffset2 +  1);

      *(boffset +  0) = ctemp01;
      *(boffset +  1) = ctemp02;
      *(boffset +  2) = ctemp03;
      *(boffset +  3) = ctemp04;

      /* aoffset1 +=  2;
      aoffset2 +=  2; */
      boffset  +=  4;
    }
  }

  if (n & 1){
    aoffset1  = aoffset;

    i = (m >> 2);
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

	*(boffset +  0) = ctemp01;
	*(boffset +  1) = ctemp02;
	*(boffset +  2) = ctemp03;
	*(boffset +  3) = ctemp04;
	*(boffset +  4) = ctemp05;
	*(boffset +  5) = ctemp06;
	*(boffset +  6) = ctemp07;
	*(boffset +  7) = ctemp08;

	aoffset1 +=  8;
	boffset  +=  8;
	i --;
      }while(i > 0);
    }

    if (m & 2) {
      ctemp01 = *(aoffset1 +  0);
      ctemp02 = *(aoffset1 +  1);
      ctemp03 = *(aoffset1 +  2);
      ctemp04 = *(aoffset1 +  3);

      *(boffset +  0) = ctemp01;
      *(boffset +  1) = ctemp02;
      *(boffset +  2) = ctemp03;
      *(boffset +  3) = ctemp04;

      aoffset1 +=  4;
      boffset  +=  4;
    }

    if (m & 1) {
      ctemp01 = *(aoffset1 +  0);
      ctemp02 = *(aoffset1 +  1);

      *(boffset +  0) = ctemp01;
      *(boffset +  1) = ctemp02;
    }
  }

  return 0;
}

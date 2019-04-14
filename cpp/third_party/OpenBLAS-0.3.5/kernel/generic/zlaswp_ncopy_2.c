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

#define a2	(a1 + 2)
#define a4	(a3 + 2)

int CNAME(BLASLONG n, BLASLONG k1, BLASLONG k2, FLOAT *a, BLASLONG lda, blasint *ipiv, FLOAT *buffer){

  BLASLONG i, j, ip1, ip2;
  blasint *piv;
  FLOAT *a1, *a3;
  FLOAT *b1, *b2, *b3, *b4;
  FLOAT A1, A2, A3, A4;
  FLOAT A5, A6, A7, A8;
  FLOAT B1, B2, B3, B4;
  FLOAT B5, B6, B7, B8;

  a -= 2;
  lda *= 2;
  k1 --;

 ipiv += k1;

  if (n  <= 0) return 0;

  j = (n >> 1);
  if (j > 0) {
    do {
      piv = ipiv;

      a1 = a + (k1 + 1) * 2;
      a3 = a1 + lda;

      ip1 = *(piv + 0) * 2;
      ip2 = *(piv + 1) * 2;
      piv += 2;

      b1 = a + ip1;
      b2 = a + ip2;

      b3 = b1 + lda;
      b4 = b2 + lda;

      i = ((k2 - k1) >> 1);

      if (i > 0) {
	do {
	  A1 = *(a1 + 0);
	  A2 = *(a1 + 1);
	  A3 = *(a2 + 0);
	  A4 = *(a2 + 1);
	  A5 = *(a3 + 0);
	  A6 = *(a3 + 1);
	  A7 = *(a4 + 0);
	  A8 = *(a4 + 1);

	  B1 = *(b1 + 0);
	  B2 = *(b1 + 1);
	  B3 = *(b2 + 0);
	  B4 = *(b2 + 1);
	  B5 = *(b3 + 0);
	  B6 = *(b3 + 1);
	  B7 = *(b4 + 0);
	  B8 = *(b4 + 1);

	  ip1 = *(piv + 0) * 2;
	  ip2 = *(piv + 1) * 2;
	  piv += 2;

	  if (b1 == a1) {
	    if (b2 == a2) {
	      *(buffer + 0) = A1;
	      *(buffer + 1) = A2;
	      *(buffer + 2) = A5;
	      *(buffer + 3) = A6;
	      *(buffer + 4) = A3;
	      *(buffer + 5) = A4;
	      *(buffer + 6) = A7;
	      *(buffer + 7) = A8;
	    } else {
	      *(buffer + 0) = A1;
	      *(buffer + 1) = A2;
	      *(buffer + 2) = A5;
	      *(buffer + 3) = A6;
	      *(buffer + 4) = B3;
	      *(buffer + 5) = B4;
	      *(buffer + 6) = B7;
	      *(buffer + 7) = B8;

	      *(b2 + 0) = A3;
	      *(b2 + 1) = A4;
	      *(b4 + 0) = A7;
	      *(b4 + 1) = A8;
	    }
	} else
	  if (b1 == a2) {
	    if (b2 == a2) {
	      *(buffer + 0) = A3;
	      *(buffer + 1) = A4;
	      *(buffer + 2) = A7;
	      *(buffer + 3) = A8;
	      *(buffer + 4) = A1;
	      *(buffer + 5) = A2;
	      *(buffer + 6) = A5;
	      *(buffer + 7) = A6;
	    } else {
	      *(buffer + 0) = A3;
	      *(buffer + 1) = A4;
	      *(buffer + 2) = A7;
	      *(buffer + 3) = A8;
	      *(buffer + 4) = B3;
	      *(buffer + 5) = B4;
	      *(buffer + 6) = B7;
	      *(buffer + 7) = B8;

	      *(b2 + 0) = A1;
	      *(b2 + 1) = A2;
	      *(b4 + 0) = A5;
	      *(b4 + 1) = A6;
	    }
	  } else {
	    if (b2 == a2) {
	      *(buffer + 0) = B1;
	      *(buffer + 1) = B2;
	      *(buffer + 2) = B5;
	      *(buffer + 3) = B6;
	      *(buffer + 4) = A3;
	      *(buffer + 5) = A4;
	      *(buffer + 6) = A7;
	      *(buffer + 7) = A8;

	      *(b1 + 0) = A1;
	      *(b1 + 1) = A2;
	      *(b3 + 0) = A5;
	      *(b3 + 1) = A6;
	    } else
	      if (b2 == b1) {
		*(buffer + 0) = B1;
		*(buffer + 1) = B2;
		*(buffer + 2) = B5;
		*(buffer + 3) = B6;
		*(buffer + 4) = A1;
		*(buffer + 5) = A2;
		*(buffer + 6) = A5;
		*(buffer + 7) = A6;

		*(b1 + 0) = A3;
		*(b1 + 1) = A4;
		*(b3 + 0) = A7;
		*(b3 + 1) = A8;
	      } else {
		*(buffer + 0) = B1;
		*(buffer + 1) = B2;
		*(buffer + 2) = B5;
		*(buffer + 3) = B6;
		*(buffer + 4) = B3;
		*(buffer + 5) = B4;
		*(buffer + 6) = B7;
		*(buffer + 7) = B8;
		*(b1 + 0) = A1;
		*(b1 + 1) = A2;
		*(b2 + 0) = A3;
		*(b2 + 1) = A4;
		*(b3 + 0) = A5;
		*(b3 + 1) = A6;
		*(b4 + 0) = A7;
		*(b4 + 1) = A8;
	      }
	    }

	    buffer += 8;

	    b1 = a + ip1;
	    b2 = a + ip2;

	    b3 = b1 + lda;
	    b4 = b2 + lda;

	    a1 += 4;
	    a3 += 4;

	    i --;
	} while (i > 0);
      }

      i = ((k2 - k1) & 1);

      if (i > 0) {
	A1 = *(a1 + 0);
	A2 = *(a1 + 1);
	B1 = *(b1 + 0);
	B2 = *(b1 + 1);
	A3 = *(a3 + 0);
	A4 = *(a3 + 1);
	B3 = *(b3 + 0);
	B4 = *(b3 + 1);

	if (a1 == b1) {
	  *(buffer + 0) = A1;
	  *(buffer + 1) = A2;
	  *(buffer + 2) = A3;
	  *(buffer + 3) = A4;

	} else {
	  *(buffer + 0) = B1;
	  *(buffer + 1) = B2;
	  *(buffer + 2) = B3;
	  *(buffer + 3) = B4;
	  *(b1 + 0) = A1;
	  *(b1 + 1) = A2;
	  *(b3 + 0) = A3;
	  *(b3 + 1) = A4;
	}
	buffer += 4;
      }

      a += 2 * lda;
      j --;
    } while (j > 0);
  }

  if (n & 1) {
    piv = ipiv;

    a1 = a + (k1 + 1) * 2;

    ip1 = *(piv + 0) * 2;
    ip2 = *(piv + 1) * 2;
    piv += 2;

    b1 = a + ip1;
    b2 = a + ip2;

    i = ((k2 - k1) >> 1);

    if (i > 0) {
      do {
	A1 = *(a1 + 0);
	A2 = *(a1 + 1);
	A3 = *(a2 + 0);
	A4 = *(a2 + 1);
	B1 = *(b1 + 0);
	B2 = *(b1 + 1);
	B3 = *(b2 + 0);
	B4 = *(b2 + 1);

	ip1 = *(piv + 0) * 2;
	ip2 = *(piv + 1) * 2;
	piv += 2;

	if (b1 == a1) {
	  if (b2 == a2) {
	    *(buffer + 0) = A1;
	    *(buffer + 1) = A2;
	    *(buffer + 2) = A3;
	    *(buffer + 3) = A4;
	  } else {
	    *(buffer + 0) = A1;
	    *(buffer + 1) = A2;
	    *(buffer + 2) = B3;
	    *(buffer + 3) = B4;

	    *(b2 + 0) = A3;
	    *(b2 + 1) = A4;
	  }
	} else
	  if (b1 == a2) {
	    if (b2 == a2) {
	      *(buffer + 0) = A3;
	      *(buffer + 1) = A4;
	      *(buffer + 2) = A1;
	      *(buffer + 3) = A2;
	    } else {
	      *(buffer + 0) = A3;
	      *(buffer + 1) = A4;
	      *(buffer + 2) = B3;
	      *(buffer + 3) = B4;
	      *(b2 + 0) = A1;
	      *(b2 + 1) = A2;
	    }
	  } else {
	    if (b2 == a2) {
	      *(buffer + 0) = B1;
	      *(buffer + 1) = B2;
	      *(buffer + 2) = A3;
	      *(buffer + 3) = A4;
	      *(b1 + 0) = A1;
	      *(b1 + 1) = A2;
	    } else
	      if (b2 == b1) {
		*(buffer + 0) = B1;
		*(buffer + 1) = B2;
		*(buffer + 2) = A1;
		*(buffer + 3) = A2;
		*(b1 + 0) = A3;
		*(b1 + 1) = A4;
	      } else {
		*(buffer + 0) = B1;
		*(buffer + 1) = B2;
		*(buffer + 2) = B3;
		*(buffer + 3) = B4;
		*(b1 + 0) = A1;
		*(b1 + 1) = A2;
		*(b2 + 0) = A3;
		*(b2 + 1) = A4;
	      }
	  }

	buffer += 4;

	b1 = a + ip1;
	b2 = a + ip2;

	a1 += 4;

	i --;
      } while (i > 0);
    }

    i = ((k2 - k1) & 1);

    if (i > 0) {
      A1 = *(a1 + 0);
      A2 = *(a1 + 1);
      B1 = *(b1 + 0);
      B2 = *(b1 + 1);

      if (a1 == b1) {
	*(buffer + 0) = A1;
	*(buffer + 1) = A2;
      } else {
	*(buffer + 0) = B1;
	*(buffer + 1) = B2;
	*(b1 + 0) = A1;
	*(b1 + 1) = A2;
      }
      // buffer += 2;
    }
  }

  return 0;
}


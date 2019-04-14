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

#define PREFETCHSIZE 12

#define a2	(a1 + 1)
#define a4	(a3 + 1)

int CNAME(BLASLONG n, BLASLONG k1, BLASLONG k2, FLOAT *a, BLASLONG lda, blasint *ipiv, FLOAT *buffer){

  BLASLONG i, j, ip1, ip2;
  blasint *piv;
  FLOAT *a1, *a3;
  FLOAT *b1, *b2, *b3, *b4;
  FLOAT A1, A2, B1, B2, A3, A4, B3, B4;

  a--;
  k1 --;

 ipiv += k1;

  if (n  <= 0) return 0;

  j = (n >> 1);
  if (j > 0) {
    do {
      piv = ipiv;

      a1 = a + k1 + 1;
      a3 = a1 + 1 * lda;

      ip1 = *(piv + 0);
      ip2 = *(piv + 1);
      piv += 2;

      b1 = a + ip1;
      b2 = a + ip2;

      b3 = b1 + 1 * lda;
      b4 = b2 + 1 * lda;

      i = ((k2 - k1) >> 1);

      if (i > 0) {
	do {

#ifdef __GNUC__
	  __builtin_prefetch(a1 + PREFETCHSIZE, 0, 0);
	  __builtin_prefetch(a3 + PREFETCHSIZE, 0, 0);
#endif

	  A1 = *a1;
	  A2 = *a2;
	  A3 = *a3;
	  A4 = *a4;

	  B1 = *b1;
	  B2 = *b2;
	  B3 = *b3;
	  B4 = *b4;

	  ip1 = *(piv + 0);
	  ip2 = *(piv + 1);
	  piv += 2;

	  if (b1 == a1) {
	    if (b2 == a2) {
	      *(buffer + 0) = A1;
	      *(buffer + 1) = A3;
	      *(buffer + 2) = A2;
	      *(buffer + 3) = A4;
	    } else {
	      *(buffer + 0) = A1;
	      *(buffer + 1) = A3;
	      *(buffer + 2) = B2;
	      *(buffer + 3) = B4;

	      *b2 = A2;
	      *b4 = A4;
	    }
	} else {
	  if (b1 == a2) {
	    if (b2 == a2) {
	      *(buffer + 0) = A2;
	      *(buffer + 1) = A4;
	      *(buffer + 2) = A1;
	      *(buffer + 3) = A3;
	    } else {
	      *(buffer + 0) = A2;
	      *(buffer + 1) = A4;
	      *(buffer + 2) = B2;
	      *(buffer + 3) = B4;
	      *b2 = A1;
	      *b4 = A3;
	    }
	  } else {
	    if (b2 == a2) {
	      *(buffer + 0) = B1;
	      *(buffer + 1) = B3;
	      *(buffer + 2) = A2;
	      *(buffer + 3) = A4;
	      *b1 = A1;
	      *b3 = A3;
	    } else {
	      if (b2 == b1) {
		*(buffer + 0) = B1;
		*(buffer + 1) = B3;
		*(buffer + 2) = A1;
		*(buffer + 3) = A3;
		*b1 = A2;
		*b3 = A4;
	      } else {
		*(buffer + 0) = B1;
		*(buffer + 1) = B3;
		*(buffer + 2) = B2;
		*(buffer + 3) = B4;
		*b1 = A1;
		*b2 = A2;
		*b3 = A3;
		*b4 = A4;
	      }
	    }
	  }
	  }

	  buffer += 4;

	  b1 = a + ip1;
	  b2 = a + ip2;

	  b3 = b1 + 1 * lda;
	  b4 = b2 + 1 * lda;

	  a1 += 2;
	  a3 += 2;

	  i --;
	} while (i > 0);
      }

      i = ((k2 - k1) & 1);

      if (i > 0) {
	A1 = *a1;
	B1 = *b1;
	A3 = *a3;
	B3 = *b3;

	if (a1 == b1) {
	  *(buffer + 0) = A1;
	  *(buffer + 1) = A3;
	} else {
	  *(buffer + 0) = B1;
	  *(buffer + 1) = B3;
	  *b1 = A1;
	  *b3 = A3;
	}
	buffer += 2;
      }

      a += 2 * lda;
      j --;
    } while (j > 0);
  }

  if (n & 1) {
    piv = ipiv;

    a1 = a + k1 + 1;

    ip1 = *(piv + 0);
    ip2 = *(piv + 1);
    piv += 2;

    b1 = a + ip1;
    b2 = a + ip2;

    i = ((k2 - k1) >> 1);

    if (i > 0) {
      do {
	A1 = *a1;
	A2 = *a2;
	B1 = *b1;
	B2 = *b2;

	ip1 = *(piv + 0);
	ip2 = *(piv + 1);
	piv += 2;

	if (b1 == a1) {
	    if (b2 == a2) {
	      *(buffer + 0) = A1;
	      *(buffer + 1) = A2;
	    } else {
	      *(buffer + 0) = A1;
	      *(buffer + 1) = B2;

	      *b2 = A2;
	    }
	} else
	  if (b1 == a2) {
	      if (b2 == a2) {
		*(buffer + 0) = A2;
		*(buffer + 1) = A1;
	      } else {
		*(buffer + 0) = A2;
		*(buffer + 1) = B2;
		*b2 = A1;
	      }
	  } else {
	      if (b2 == a2) {
		*(buffer + 0) = B1;
		*(buffer + 1) = A2;
		*b1 = A1;
	      } else
		if (b2 == b1) {
		  *(buffer + 0) = B1;
		  *(buffer + 1) = A1;
		  *b1 = A2;
		} else {
		  *(buffer + 0) = B1;
		  *(buffer + 1) = B2;
		  *b1 = A1;
		  *b2 = A2;
		}
	  }

	buffer += 2;

	b1 = a + ip1;
	b2 = a + ip2;

	a1 += 2;

	i --;
      } while (i > 0);
    }

    i = ((k2 - k1) & 1);

    if (i > 0) {
      A1 = *a1;
      B1 = *b1;

      if (a1 == b1) {
	*(buffer + 0) = A1;
      } else {
	*(buffer + 0) = B1;
	*b1 = A1;
      }
    }
  }

  return 0;
}


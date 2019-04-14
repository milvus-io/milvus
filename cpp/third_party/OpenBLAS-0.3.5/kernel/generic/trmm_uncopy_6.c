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

int CNAME(BLASLONG m, BLASLONG n, FLOAT *a, BLASLONG lda, BLASLONG posX, BLASLONG posY, FLOAT *b){

	BLASLONG i, js;
	BLASLONG X, mm;

	FLOAT data01, data02, data03, data04, data05, data06;
	FLOAT data07, data08, data09, data10, data11, data12;
	FLOAT data13, data14, data15, data16, data17, data18;
	FLOAT data19, data20, data21, data22, data23, data24;
	FLOAT data25, data26, data27, data28, data29, data30;
	FLOAT data31, data32, data33, data34, data35, data36;

	FLOAT *ao1, *ao2, *ao3, *ao4, *ao5, *ao6;

	//js = (n >> 2);
	js = n/6;
	if (js > 0){
		do {
			X = posX;

			if (posX <= posY) {
				ao1 = a + posX + (posY + 0) * lda;
				ao2 = a + posX + (posY + 1) * lda;
				ao3 = a + posX + (posY + 2) * lda;
				ao4 = a + posX + (posY + 3) * lda;
				ao5 = a + posX + (posY + 4) * lda;
				ao6 = a + posX + (posY + 5) * lda;
			} else {
				ao1 = a + posY + (posX + 0) * lda;
				ao2 = a + posY + (posX + 1) * lda;
				ao3 = a + posY + (posX + 2) * lda;
				ao4 = a + posY + (posX + 3) * lda;
				ao5 = a + posY + (posX + 4) * lda;
				ao6 = a + posY + (posX + 5) * lda;
			}

			i = m/6;
			if (i > 0) {
				do {
					if (X < posY) {
						data01 = *(ao1 + 0);
						data02 = *(ao1 + 1);
						data03 = *(ao1 + 2);
						data04 = *(ao1 + 3);
						data05 = *(ao1 + 4);
						data06 = *(ao1 + 5);

						data07 = *(ao2 + 0);
						data08 = *(ao2 + 1);
						data09 = *(ao2 + 2);
						data10 = *(ao2 + 3);
						data11 = *(ao2 + 4);
						data12 = *(ao2 + 5);

						data13 = *(ao3 + 0);
						data14 = *(ao3 + 1);
						data15 = *(ao3 + 2);
						data16 = *(ao3 + 3);
						data17 = *(ao3 + 4);
						data18 = *(ao3 + 5);

						data19 = *(ao4 + 0);
						data20 = *(ao4 + 1);
						data21 = *(ao4 + 2);
						data22 = *(ao4 + 3);
						data23 = *(ao4 + 4);
						data24 = *(ao4 + 5);

						data25 = *(ao5 + 0);
						data26 = *(ao5 + 1);
						data27 = *(ao5 + 2);
						data28 = *(ao5 + 3);
						data29 = *(ao5 + 4);
						data30 = *(ao5 + 5);

						data31 = *(ao6 + 0);
						data32 = *(ao6 + 1);
						data33 = *(ao6 + 2);
						data34 = *(ao6 + 3);
						data35 = *(ao6 + 4);
						data36 = *(ao6 + 5);

						b[ 0] = data01;
						b[ 1] = data07;
						b[ 2] = data13;
						b[ 3] = data19;
						b[ 4] = data25;
						b[ 5] = data31;

						b[ 6] = data02;
						b[ 7] = data08;
						b[ 8] = data14;
						b[ 9] = data20;
						b[10] = data26;
						b[11] = data32;

						b[12] = data03;
						b[13] = data09;
						b[14] = data15;
						b[15] = data21;
						b[16] = data27;
						b[17] = data33;

						b[18] = data04;
						b[19] = data10;
						b[20] = data16;
						b[21] = data22;
						b[22] = data28;
						b[23] = data34;

						b[24] = data05;
						b[25] = data11;
						b[26] = data17;
						b[27] = data23;
						b[28] = data29;
						b[29] = data35;

						b[30] = data06;
						b[31] = data12;
						b[32] = data18;
						b[33] = data24;
						b[34] = data30;
						b[35] = data36;

						ao1 += 6;
						ao2 += 6;
						ao3 += 6;
						ao4 += 6;
						ao5 += 6;
						ao6 += 6;
						b += 36;
					} else
						if (X > posY) {
							b[ 0] = ZERO;
							b[ 1] = ZERO;
							b[ 2] = ZERO;
							b[ 3] = ZERO;
							b[ 4] = ZERO;
							b[ 5] = ZERO;
							b[ 6] = ZERO;
							b[ 7] = ZERO;
							b[ 8] = ZERO;
							b[ 9] = ZERO;
							b[10] = ZERO;
							b[11] = ZERO;
							b[12] = ZERO;
							b[13] = ZERO;
							b[14] = ZERO;
							b[15] = ZERO;
							b[16] = ZERO;
							b[17] = ZERO;
							b[18] = ZERO;
							b[19] = ZERO;
							b[20] = ZERO;
							b[21] = ZERO;
							b[22] = ZERO;
							b[23] = ZERO;
							b[24] = ZERO;
							b[25] = ZERO;
							b[26] = ZERO;
							b[27] = ZERO;
							b[28] = ZERO;
							b[29] = ZERO;
							b[30] = ZERO;
							b[31] = ZERO;
							b[32] = ZERO;
							b[33] = ZERO;
							b[34] = ZERO;
							b[35] = ZERO;

							ao1 += 6 * lda;
							ao2 += 6 * lda;
							ao3 += 6 * lda;
							ao4 += 6 * lda;
							ao5 += 6 * lda;
							ao6 += 6 * lda;

							b   += 36;
						} else {
							data01 = *(ao1 + 0);
							data07 = *(ao2 + 0);
							data13 = *(ao3 + 0);
							data19 = *(ao4 + 0);
							data25 = *(ao5 + 0);
							data31 = *(ao6 + 0);

							data08 = *(ao2 + 1);
							data14 = *(ao3 + 1);
							data20 = *(ao4 + 1);
							data26 = *(ao5 + 1);
							data32 = *(ao6 + 1);

							data15 = *(ao3 + 2);
							data21 = *(ao4 + 2);
							data27 = *(ao5 + 2);
							data33 = *(ao6 + 2);

							data22 = *(ao4 + 3);
							data28 = *(ao5 + 3);
							data34 = *(ao6 + 3);

							data29 = *(ao5 + 4);
							data35 = *(ao6 + 4);

							data36 = *(ao6 + 5);

#ifdef UNIT
							b[ 0] = ONE;
							b[ 1] = data07;
							b[ 2] = data13;
							b[ 3] = data19;
							b[ 4] = data25;
							b[ 5] = data31;

							b[ 6] = ZERO;
							b[ 7] = ONE;
							b[ 8] = data14;
							b[ 9] = data20;
							b[10] = data26;
							b[11] = data32;

							b[12] = ZERO;
							b[13] = ZERO;
							b[14] = ONE;
							b[15] = data21;
							b[16] = data27;
							b[17] = data33;

							b[18] = ZERO;
							b[19] = ZERO;
							b[20] = ZERO;
							b[21] = ONE;
							b[22] = data28;
							b[23] = data34;

							b[24] = ZERO;
							b[25] = ZERO;
							b[26] = ZERO;
							b[27] = ZERO;
							b[28] = ONE;
							b[29] = data35;

							b[30] = ZERO;
							b[31] = ZERO;
							b[32] = ZERO;
							b[33] = ZERO;
							b[34] = ZERO;
							b[35] = ONE;
#else
							b[ 0] = data01;
							b[ 1] = data07;
							b[ 2] = data13;
							b[ 3] = data19;
							b[ 4] = data25;
							b[ 5] = data31;

							b[ 6] = ZERO;
							b[ 7] = data08;
							b[ 8] = data14;
							b[ 9] = data20;
							b[10] = data26;
							b[11] = data32;

							b[12] = ZERO;
							b[13] = ZERO;
							b[14] = data15;
							b[15] = data21;
							b[16] = data27;
							b[17] = data33;

							b[18] = ZERO;
							b[19] = ZERO;
							b[20] = ZERO;
							b[21] = data22;
							b[22] = data28;
							b[23] = data34;

							b[24] = ZERO;
							b[25] = ZERO;
							b[26] = ZERO;
							b[27] = ZERO;
							b[28] = data29;
							b[29] = data35;

							b[30] = ZERO;
							b[31] = ZERO;
							b[32] = ZERO;
							b[33] = ZERO;
							b[34] = ZERO;
							b[35] = data36;
#endif

							ao1 += 6;
							ao2 += 6;
							ao3 += 6;
							ao4 += 6;
							ao5 += 6;
							ao6 += 7;

							b += 36;
						}
					X += 6;
					i --;
				} while (i > 0);
			}
			mm = m - m/6;
			if (mm & 4) {
				if (X < posY) {
					data01 = *(ao1 + 0);
					data02 = *(ao1 + 1);
					data03 = *(ao1 + 2);
					data04 = *(ao1 + 3);

					data05 = *(ao2 + 0);
					data06 = *(ao2 + 1);
					data07 = *(ao2 + 2);
					data08 = *(ao2 + 3);

					data09 = *(ao3 + 0);
					data10 = *(ao3 + 1);
					data11 = *(ao3 + 2);
					data12 = *(ao3 + 3);

					data13 = *(ao4 + 0);
					data14 = *(ao4 + 1);
					data15 = *(ao4 + 2);
					data16 = *(ao4 + 3);

					b[ 0] = data01;
					b[ 1] = data05;
					b[ 2] = data09;
					b[ 3] = data13;
					b[ 4] = data02;
					b[ 5] = data06;
					b[ 6] = data10;
					b[ 7] = data14;

					b[ 8] = data03;
					b[ 9] = data07;
					b[10] = data11;
					b[11] = data15;
					b[12] = data04;
					b[13] = data08;
					b[14] = data12;
					b[15] = data16;

					ao1 += 4;
					ao2 += 4;
					ao3 += 4;
					ao4 += 4;
					b += 16;
				} else
					if (X > posY) {
						b[ 0] = ZERO;
						b[ 1] = ZERO;
						b[ 2] = ZERO;
						b[ 3] = ZERO;
						b[ 4] = ZERO;
						b[ 5] = ZERO;
						b[ 6] = ZERO;
						b[ 7] = ZERO;
						b[ 8] = ZERO;
						b[ 9] = ZERO;
						b[10] = ZERO;
						b[11] = ZERO;
						b[12] = ZERO;
						b[13] = ZERO;
						b[14] = ZERO;
						b[15] = ZERO;
						b[16] = ZERO;
						b[17] = ZERO;
						b[18] = ZERO;
						b[19] = ZERO;
						b[20] = ZERO;
						b[21] = ZERO;
						b[22] = ZERO;
						b[23] = ZERO;

						ao1 += 4 * lda;
						ao2 += 4 * lda;
						ao3 += 4 * lda;
						ao4 += 4 * lda;

						b   += 16;
					} else {
#ifdef UNIT
						data05 = *(ao2 + 0);

						data09 = *(ao3 + 0);
						data10 = *(ao3 + 1);

						data13 = *(ao4 + 0);
						data14 = *(ao4 + 1);
						data15 = *(ao4 + 2);

						b[ 0] = ONE;
						b[ 1] = data05;
						b[ 2] = data09;
						b[ 3] = data13;

						b[ 4] = ZERO;
						b[ 5] = ONE;
						b[ 6] = data10;
						b[ 7] = data14;

						b[ 8] = ZERO;
						b[ 9] = ZERO;
						b[10] = ONE;
						b[11] = data15;

						b[12] = ZERO;
						b[13] = ZERO;
						b[14] = ZERO;
						b[15] = ONE;
#else
						data01 = *(ao1 + 0);

						data05 = *(ao2 + 0);
						data06 = *(ao2 + 1);

						data09 = *(ao3 + 0);
						data10 = *(ao3 + 1);
						data11 = *(ao3 + 2);

						data13 = *(ao4 + 0);
						data14 = *(ao4 + 1);
						data15 = *(ao4 + 2);
						data16 = *(ao4 + 3);

						b[ 0] = data01;
						b[ 1] = data05;
						b[ 2] = data09;
						b[ 3] = data13;

						b[ 4] = ZERO;
						b[ 5] = data06;
						b[ 6] = data10;
						b[ 7] = data14;

						b[ 8] = ZERO;
						b[ 9] = ZERO;
						b[10] = data11;
						b[11] = data15;

						b[12] = ZERO;
						b[13] = ZERO;
						b[14] = ZERO;
						b[15] = data16;
#endif
						ao1 += 4;
						ao2 += 4;
						ao3 += 4;
						ao4 += 4;

						b += 16;
					}
				X += 4;
			}

			if (mm & 3) {
				if (X < posY) {
					if (mm & 2) {
						data01 = *(ao1 + 0);
						data02 = *(ao1 + 1);
						data03 = *(ao2 + 0);
						data04 = *(ao2 + 1);
						data05 = *(ao3 + 0);
						data06 = *(ao3 + 1);
						data07 = *(ao4 + 0);
						data08 = *(ao4 + 1);

						b[ 0] = data01;
						b[ 1] = data03;
						b[ 2] = data05;
						b[ 3] = data07;
						b[ 4] = data02;
						b[ 5] = data04;
						b[ 6] = data06;
						b[ 7] = data08;

						ao1 += 2;
						ao2 += 2;
						ao3 += 2;
						ao4 += 2;
						b += 8;
					}

					if (mm & 1) {
						data01 = *(ao1 + 0);
						data03 = *(ao2 + 0);
						data05 = *(ao3 + 0);
						data07 = *(ao4 + 0);

						b[ 0] = data01;
						b[ 1] = data03;
						b[ 2] = data05;
						b[ 3] = data07;

						ao1 += 1;
						ao2 += 1;
						ao3 += 1;
						ao4 += 1;
						b += 4;
					}

				} else
					if (X > posY) {
						if (m & 2) {
							ao1 += 2 * lda;
							ao2 += 2 * lda;
							b   += 8;
						}

						if (m & 1) {
							ao1 += lda;
							b += 4;
						}

					} else {
#ifdef UNIT
						data05 = *(ao2 + 0);
						data09 = *(ao3 + 0);
						data13 = *(ao4 + 0);

						if (i >= 2) {
							data10 = *(ao3 + 1);
							data14 = *(ao4 + 1);
						}

						if (i >= 3) {
							data15 = *(ao4 + 2);
						}

						b[ 0] = ONE;
						b[ 1] = data05;
						b[ 2] = data09;
						b[ 3] = data13;
						b += 4;

						if(i >= 2) {
							b[ 0] = ZERO;
							b[ 1] = ONE;
							b[ 2] = data10;
							b[ 3] = data14;
							b += 4;
						}

						if (i >= 3) {
							b[ 0] = ZERO;
							b[ 1] = ZERO;
							b[ 2] = ONE;
							b[ 3] = data15;
							b += 4;
						}
#else
						data01 = *(ao1 + 0);
						data05 = *(ao2 + 0);
						data09 = *(ao3 + 0);
						data13 = *(ao4 + 0);

						if (i >= 2) {
							data06 = *(ao2 + 1);
							data10 = *(ao3 + 1);
							data14 = *(ao4 + 1);
						}

						if (i >= 3) {
							data11 = *(ao3 + 2);
							data15 = *(ao4 + 2);
						}

						b[ 0] = data01;
						b[ 1] = data05;
						b[ 2] = data09;
						b[ 3] = data13;
						b += 4;

						if(i >= 2) {
							b[ 0] = ZERO;
							b[ 1] = data06;
							b[ 2] = data10;
							b[ 3] = data14;
							b += 4;
						}

						if (i >= 3) {
							b[ 0] = ZERO;
							b[ 1] = ZERO;
							b[ 2] = data11;
							b[ 3] = data15;
							b += 4;
						}
#endif
					}
			}

			posY += 4;
			js --;
		} while (js > 0);
	} /* End of main loop */

	if (n & 2){
		X = posX;

		if (posX <= posY) {
			ao1 = a + posX + (posY + 0) * lda;
			ao2 = a + posX + (posY + 1) * lda;
		} else {
			ao1 = a + posY + (posX + 0) * lda;
			ao2 = a + posY + (posX + 1) * lda;
		}

		i = (m >> 1);
		if (i > 0) {
			do {
				if (X < posY) {
					data01 = *(ao1 + 0);
					data02 = *(ao1 + 1);
					data05 = *(ao2 + 0);
					data06 = *(ao2 + 1);

					b[ 0] = data01;
					b[ 1] = data05;
					b[ 2] = data02;
					b[ 3] = data06;

					ao1 += 2;
					ao2 += 2;
					b += 4;

				} else
					if (X > posY) {
						ao1 += 2 * lda;
						ao2 += 2 * lda;
						b += 4;

					} else {
#ifdef UNIT
						data05 = *(ao2 + 0);

						b[ 0] = ONE;
						b[ 1] = data05;
						b[ 2] = ZERO;
						b[ 3] = ONE;
#else
						data01 = *(ao1 + 0);
						data05 = *(ao2 + 0);
						data06 = *(ao2 + 1);

						b[ 0] = data01;
						b[ 1] = data05;
						b[ 2] = ZERO;
						b[ 3] = data06;
#endif

						ao1 += 2 * lda;
						ao2 += 2 * lda;

						b += 4;
					}

				X += 2;
				i --;
			} while (i > 0);
		}

		i = (m & 1);
		if (i) {

			if (X < posY) {
				data01 = *(ao1 + 0);
				data05 = *(ao2 + 0);

				b[ 0] = data01;
				b[ 1] = data05;
				ao1 += 1;
				ao2 += 1;
				b += 2;
			} else
				if (X > posY) {
					ao1 += lda;
					ao2 += lda;
					b += 2;
				} else {
#ifdef UNIT
					data05 = *(ao2 + 0);
					b[ 0] = ONE;
					b[ 1] = data05;
#else
					data01 = *(ao1 + 0);
					data05 = *(ao2 + 0);

					b[ 0] = data01;
					b[ 1] = data05;
#endif
					ao1 += lda;
					ao2 += lda;
					b += 2;
				}
		}

		posY += 2;
	}

	if (n & 1){
		X = posX;

		if (posX <= posY) {
			ao1 = a + posX + (posY + 0) * lda;
		} else {
			ao1 = a + posY + (posX + 0) * lda;
		}

		i = m;
		if (m > 0) {
			do {
				if (X < posY) {
					data01 = *(ao1 + 0);
					b[ 0] = data01;
					ao1 += 1;
					b += 1;
				} else
					if (X > posY)  {
						ao1 += lda;
						b += 1;
					} else {
#ifdef UNIT
						b[ 0] = ONE;
#else
						data01 = *(ao1 + 0);
						b[ 0] = data01;
#endif
						ao1 += lda;
						b += 1;
					}

				X += 1;
				i --;
			} while (i > 0);
		}
	}

	return 0;
}

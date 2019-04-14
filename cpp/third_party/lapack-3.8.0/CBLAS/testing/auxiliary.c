/*
 *     Written by T. H. Do, 1/23/98, SGI/CRAY Research.
 */
#include <string.h>
#include "cblas.h"
#include "cblas_test.h"

void get_transpose_type(char *type, CBLAS_TRANSPOSE *trans) {
  if( (strncmp( type,"n",1 )==0)||(strncmp( type,"N",1 )==0) )
        *trans = CblasNoTrans;
  else if( (strncmp( type,"t",1 )==0)||(strncmp( type,"T",1 )==0) )
        *trans = CblasTrans;
  else if( (strncmp( type,"c",1 )==0)||(strncmp( type,"C",1 )==0) )
        *trans = CblasConjTrans;
  else *trans = UNDEFINED;
}

void get_uplo_type(char *type, CBLAS_UPLO *uplo) {
  if( (strncmp( type,"u",1 )==0)||(strncmp( type,"U",1 )==0) )
        *uplo = CblasUpper;
  else if( (strncmp( type,"l",1 )==0)||(strncmp( type,"L",1 )==0) )
        *uplo = CblasLower;
  else *uplo = UNDEFINED;
}
void get_diag_type(char *type, CBLAS_DIAG *diag) {
  if( (strncmp( type,"u",1 )==0)||(strncmp( type,"U",1 )==0) )
        *diag = CblasUnit;
  else if( (strncmp( type,"n",1 )==0)||(strncmp( type,"N",1 )==0) )
        *diag = CblasNonUnit;
  else *diag = UNDEFINED;
}
void get_side_type(char *type, CBLAS_SIDE *side) {
  if( (strncmp( type,"l",1 )==0)||(strncmp( type,"L",1 )==0) )
        *side = CblasLeft;
  else if( (strncmp( type,"r",1 )==0)||(strncmp( type,"R",1 )==0) )
        *side = CblasRight;
  else *side = UNDEFINED;
}

%module nsg
%{
#define SWIG_FILE_WITH_INIT
#include <numpy/arrayobject.h>

/* Include the header in the wrapper code */
#include "nsg.h"


%}


/* Parse the header file */
%include "index.h"


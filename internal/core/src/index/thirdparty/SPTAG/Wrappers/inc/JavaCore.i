%module JAVASPTAG

%{
#include "inc/CoreInterface.h"
%}

%include <std_shared_ptr.i>
%shared_ptr(AnnIndex)
%shared_ptr(QueryResult)
%include "JavaCommon.i"

%{
#define SWIG_FILE_WITH_INIT
%}

%include "CoreInterface.h"
%include "../../AnnService/inc/Core/SearchResult.h"

%module CSHARPSPTAGClient

%{
#include "inc/ClientInterface.h"
%}

%include <std_shared_ptr.i>
%shared_ptr(AnnClient)
%shared_ptr(RemoteSearchResult)
%include "CsharpCommon.i"

%{
#define SWIG_FILE_WITH_INIT
%}

%include "ClientInterface.h"

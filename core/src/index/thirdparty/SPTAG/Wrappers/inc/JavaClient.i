%module JAVASPTAGClient

%{
#include "inc/ClientInterface.h"
%}

%include <std_shared_ptr.i>
%shared_ptr(AnnClient)
%shared_ptr(RemoteSearchResult)
%include "JavaCommon.i"

%{
#define SWIG_FILE_WITH_INIT
%}

%include "ClientInterface.h"

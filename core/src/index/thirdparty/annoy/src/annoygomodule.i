%module annoyindex

%{
#include "annoygomodule.h"
%}


// const float *
%typemap(gotype) (const float *)  "[]float32"

%typemap(in) (const float *)
%{
    float *v;
    vector<float> w;
    v = (float *)$input.array;
    for (int i = 0; i < $input.len; i++) {
       w.push_back(v[i]);
    }
    $1 = &w[0];
%}

// vector<int32_t> *
%typemap(gotype) (vector<int32_t> *)  "*[]int"

%typemap(in) (vector<int32_t> *)
%{
  $1 = new vector<int32_t>();
%}

%typemap(freearg) (vector<int32_t> *)
%{
  delete $1;
%}

%typemap(argout) (vector<int32_t> *)
%{
  {
    $input->len = $1->size();
    $input->cap = $1->size();
    $input->array = malloc($input->len * sizeof(intgo));
    for (int i = 0; i < $1->size(); i++) {
        ((intgo *)$input->array)[i] = (intgo)(*$1)[i];
    }
  }
%}


// vector<float> *
%typemap(gotype) (vector<float> *)  "*[]float32"

%typemap(in) (vector<float> *)
%{
  $1 = new vector<float>();
%}

%typemap(freearg) (vector<float> *)
%{
  delete $1;
%}

%typemap(argout) (vector<float> *)
%{
  {
    $input->len = $1->size();
    $input->cap = $1->size();
    $input->array = malloc($input->len * sizeof(float));
    for (int i = 0; i < $1->size(); i++) {
        ((float *)$input->array)[i] = (float)(*$1)[i];
    }
  }
%}


%typemap(gotype) (const char *) "string"

%typemap(in) (const char *)
%{
  $1 = (char *)calloc((((_gostring_)$input).n + 1), sizeof(char));
  strncpy($1, (((_gostring_)$input).p), ((_gostring_)$input).n);
%}

%typemap(freearg) (const char *)
%{
  free($1);
%}


/* Let's just grab the original header file here */
%include "annoygomodule.h"

%feature("notabstract") GoAnnoyIndexAngular;
%feature("notabstract") GoAnnoyIndexEuclidean;
%feature("notabstract") GoAnnoyIndexManhattan;



